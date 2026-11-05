//! `internal/sources` 的行为回归测试。

use std::{
    collections::{BTreeMap, HashMap},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::git::{ActivityStopGuard, pack_size};
use super::*;
use crate::internal::{
    api::{LogCategory, LogLevel, SessionError},
    auth::{AuthContext, Permission, SourceAuthProvider, new_auth_context_with_session_id},
    config::{BaseSource, GitInfo, GitRepositorySource},
    session::{ActivityRecorder, ActivityRecorderError, NoopActivityRecorder, NoopStopper},
    util::Stopper,
};

/// 只记录同步停止请求的 fixture，用于验证 Git guard 的丢弃路径。
struct RequestTrackingStopper(Arc<AtomicUsize>);

#[async_trait]
impl Stopper for RequestTrackingStopper {
    /// 保存 `Drop` 已经传播取消的可观察次数。
    fn request_stop(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// fixture 没有后台任务；完整 Stop 发出同步请求后即可完成。
    async fn stop(&self) {
        self.request_stop();
    }
}

/// Git process future 被 abort 时，RAII guard 仍必须同步停止活动记录器。
#[tokio::test]
async fn git_activity_guard_future_drop_requests_stop() {
    let request_count = Arc::new(AtomicUsize::new(0));
    let stopper: Arc<dyn Stopper> = Arc::new(RequestTrackingStopper(Arc::clone(&request_count)));
    let guard_installed = Arc::new(Notify::new());
    let task_guard_installed = Arc::clone(&guard_installed);
    let task = tokio::spawn(async move {
        let _guard = ActivityStopGuard::new(Some(stopper));
        task_guard_installed.notify_one();
        std::future::pending::<()>().await;
    });

    guard_installed.notified().await;
    task.abort();
    assert!(
        task.await
            .expect_err("abort 后任务必须以 JoinError 结束")
            .is_cancelled()
    );
    assert_eq!(request_count.load(Ordering::SeqCst), 1);
}

/// 生产 Git handler 默认接入真实 proxy factory，调用方无需二次注入。
#[test]
fn git_handler_has_production_proxy_factory_by_default() {
    let handler = new_git_handler(
        "/tmp/sources-proxy-default",
        None,
        "https://api.example.test",
        None,
        None,
        ProcessMode::from("fresh"),
        false,
    );
    assert!(
        handler
            .git_proxy_factory
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    );
}

/// 子进程配置必须贯穿访问验证和实际 fetch，且不能写入父进程全局环境。
#[tokio::test]
async fn source_network_environment_reaches_validation_and_clone() {
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let before = std::env::var_os("GIT_CONFIG_COUNT");
    let clone_in_scope = |marker: &'static str| async move {
        let (fixture, origin, _) = create_bare_git_repo();
        let worktree = fixture.path().join("worktree");
        git_run(&worktree, &["commit", "--allow-empty", "-m", marker]);
        git_run(&worktree, &["push", path_text(&origin), "main"]);
        let expected_head = git_output(&worktree, &["rev-parse", "HEAD"]);
        let base = TempDir::new().unwrap();
        let mut source = git_test_source(&origin, Some("main"));
        source.git_info.url = Some("https://source-environment.invalid/repository.git".to_owned());
        source.mount_path = base.path().join("repository").display().to_string();
        // 两个任务使用同一 URL、不同本地源；串用配置会得到错误的 HEAD。
        let environment = [
            ("GIT_CONFIG_COUNT".to_owned(), "1".to_owned()),
            (
                "GIT_CONFIG_KEY_0".to_owned(),
                format!("url.file://{}.insteadOf", origin.display()),
            ),
            (
                "GIT_CONFIG_VALUE_0".to_owned(),
                source.git_info.url.clone().unwrap(),
            ),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect();
        let manager = new_source_handler_manager(
            base.path(),
            None,
            "",
            None,
            None,
            ProcessMode::from("fresh"),
            true,
        )
        .unwrap();
        GIT_ENVIRONMENT
            .scope(
                environment,
                manager.process_sources(&CancellationToken::new(), &[&source]),
            )
            .await
            .unwrap();
        assert_eq!(
            git_output(Path::new(&source.mount_path), &["rev-parse", "HEAD"]),
            expected_head
        );
        assert!(GIT_ENVIRONMENT.try_with(|_| ()).is_err());
    };
    let (first, second) = tokio::join!(
        tokio::spawn(clone_in_scope("one")),
        tokio::spawn(clone_in_scope("two"))
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(std::env::var_os("GIT_CONFIG_COUNT"), before);
}

/// 验证 fresh 模式从本地 bare repository 拉取指定 ref，并保留公开 origin URL。
#[tokio::test]
async fn test_git_handler_process_fresh_clones_local_repository() {
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let (root, bare_repo, commit_sha) = create_bare_git_repo();
    let base_dir = TempDir::new().expect("base temp dir should be created");
    let source = git_test_source(&bare_repo, Some("main"));
    let handler = new_git_handler(
        base_dir.path(),
        None,
        "",
        None,
        Some(Arc::new(NoopActivityRecorder)),
        ProcessMode::from("fresh"),
        false,
    );

    handler
        .process(&CancellationToken::new(), &source)
        .await
        .expect("fresh clone should succeed");

    let repo_path = source.get_directory(base_dir.path());
    assert_eq!(
        std::fs::read_to_string(repo_path.join("README.md")).expect("README should be cloned"),
        "hello\n"
    );
    assert_eq!(git_output(&repo_path, &["rev-parse", "HEAD"]), commit_sha);
    assert_eq!(
        git_output(&repo_path, &["remote", "get-url", "origin"]),
        format!("file://{}", bare_repo.display())
    );
    drop(root);
}

/// 新 outcome branch 不伪造远端引用；真实推送后必须建立可用的 upstream。
#[tokio::test]
async fn test_git_handler_process_creates_outcome_branch() {
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let (_root, bare_repo, _) = create_bare_git_repo();
    let base_dir = TempDir::new().expect("base temp dir should be created");
    let source = git_test_source(&bare_repo, Some("main"));
    let mut repo_branches = HashMap::new();
    repo_branches.insert(bare_repo.display().to_string(), "task-branch".to_owned());
    let handler = new_git_handler(
        base_dir.path(),
        None,
        "",
        Some(repo_branches),
        Some(Arc::new(NoopActivityRecorder)),
        ProcessMode::from("fresh"),
        false,
    );

    handler
        .process(&CancellationToken::new(), &source)
        .await
        .expect("clone and outcome branch setup should succeed");

    let repo_path = source.get_directory(base_dir.path());
    assert_eq!(
        git_output(&repo_path, &["branch", "--show-current"]),
        "task-branch"
    );
    assert!(
        git_output(
            &repo_path,
            &["for-each-ref", "refs/remotes/origin/task-branch"]
        )
        .is_empty()
    );
    git_run(&repo_path, &["config", "user.email", "test@example.com"]);
    git_run(&repo_path, &["config", "user.name", "Test User"]);
    git_run(&repo_path, &["config", "commit.gpgsign", "false"]);
    git_run(&repo_path, &["commit", "--allow-empty", "-m", "task work"]);
    git_run(&repo_path, &["push", "-u", "origin", "task-branch"]);
    assert_eq!(
        git_output(&repo_path, &["rev-parse", "origin/task-branch"]),
        git_output(&repo_path, &["rev-parse", "HEAD"])
    );
    assert_eq!(
        git_output(&repo_path, &["rev-parse", "--abbrev-ref", "@{upstream}"]),
        "origin/task-branch"
    );
}

/// 验证只有非空 custom URL 会修改已经存在仓库的 origin。
#[tokio::test]
async fn test_git_handler_update_remote_url() {
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let base_dir = TempDir::new().expect("base temp dir should be created");
    let repo_path = base_dir.path().join("demo");
    git_run(Path::new(""), &["init", path_text(&repo_path)]);
    git_run(
        &repo_path,
        &["remote", "add", "origin", "https://example.test/old.git"],
    );

    let mut source = GitRepositorySource {
        base: BaseSource {
            source_type: "git_repository".to_owned(),
        },
        git_info: GitInfo {
            git_type: "github".to_owned(),
            host: "github".to_owned(),
            repo: "demo".to_owned(),
            url: Some("https://example.test/new.git".to_owned()),
            ..GitInfo::default()
        },
        ..GitRepositorySource::default()
    };
    let handler = new_git_handler(
        base_dir.path(),
        None,
        "",
        None,
        None,
        ProcessMode::from("resume"),
        false,
    );
    handler
        .update_remote_url(&CancellationToken::new(), &source)
        .await
        .expect("custom URL update should succeed");
    assert_eq!(
        git_output(&repo_path, &["remote", "get-url", "origin"]),
        "https://example.test/new.git"
    );

    source.git_info.url = None;
    handler
        .update_remote_url(&CancellationToken::new(), &source)
        .await
        .expect("missing custom URL should be a no-op");
}

/// 验证 auth provider 调用条件与 URL userinfo 脱敏格式。
#[test]
fn test_git_handler_authentication_helpers() {
    let auth_context = Arc::new(
        new_auth_context_with_session_id(&[], "session-id")
            .expect("empty auth configuration should parse"),
    );
    let provider = FakeSourceAuthProvider::new("https://token@example.test/repo.git", true);
    let handler = new_git_handler(
        TempDir::new().expect("temp dir should be created").path(),
        Some(auth_context),
        "",
        None,
        None,
        ProcessMode::from("fresh"),
        false,
    );
    let (authenticated, ok) = handler.get_authenticated_url(
        "https://example.test/repo.git",
        Some(&provider),
        &Permission::from("read"),
    );
    assert!(ok);
    assert_eq!(authenticated, "https://token@example.test/repo.git");
    let observed = provider
        .observed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(observed.source_url, "https://example.test/repo.git");
    assert_eq!(observed.permission, Permission::from("read"));
    drop(observed);

    let no_context_handler = new_git_handler(
        TempDir::new().expect("temp dir should be created").path(),
        None,
        "",
        None,
        None,
        ProcessMode::from("fresh"),
        false,
    );
    assert_eq!(
        no_context_handler.get_authenticated_url(
            "https://example.test/repo.git",
            Some(&provider),
            &Permission::from("read"),
        ),
        ("https://example.test/repo.git".to_owned(), false)
    );
    assert_eq!(
        handler.sanitize_url("https://token:secret@example.test/repo.git?x=1#frag"),
        "https://<token>@example.test/repo.git?x=1#frag"
    );
    assert!(
        handler
            .sanitize_url("not a url https://secret@example.test/repo.git")
            .contains("https://<token>@example.test/repo.git")
    );
}

/// 验证 remote branch 探测只接受命令成功且 stdout 非空。
#[tokio::test]
async fn test_git_handler_branch_helpers() {
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let (_root, bare_repo, _) = create_bare_git_repo();
    let repo_path = TempDir::new().expect("temp dir should be created");
    let handler = new_git_handler(
        TempDir::new().expect("temp dir should be created").path(),
        None,
        "",
        None,
        None,
        ProcessMode::from("fresh"),
        false,
    );
    let url = format!("file://{}", bare_repo.display());
    assert!(
        handler
            .branch_exists_on_remote(&CancellationToken::new(), repo_path.path(), &url, "main")
            .await
    );
    assert!(
        !handler
            .branch_exists_on_remote(&CancellationToken::new(), repo_path.path(), &url, "missing")
            .await
    );
}

/// 验证 pack size 只统计 `.pack` 文件。
#[test]
fn test_pack_size() {
    let repo_path = TempDir::new().expect("temp dir should be created");
    let pack_dir = repo_path.path().join(".git/objects/pack");
    std::fs::create_dir_all(&pack_dir).expect("pack directory should be created");
    std::fs::write(pack_dir.join("one.pack"), b"1234").expect("first pack should be written");
    std::fs::write(pack_dir.join("two.pack"), b"123").expect("second pack should be written");
    std::fs::write(pack_dir.join("ignore.idx"), b"123456789").expect("index should be written");
    assert_eq!(
        pack_size(repo_path.path()).expect("pack size should work"),
        7
    );
}

/// 验证 manager 的空 base directory、动态 handler 分派和 缺失 source 索引错误。
#[tokio::test]
async fn test_source_handler_manager_dispatches_handlers() {
    let empty_error = new_source_handler_manager(
        PathBuf::new(),
        None,
        "",
        None,
        None,
        ProcessMode::from("fresh"),
        false,
    )
    .err()
    .expect("empty base directory should fail");
    assert_eq!(empty_error.to_string(), "base directory cannot be empty");

    let base = TempDir::new().expect("temp dir should be created");
    let mut manager = new_source_handler_manager(
        base.path(),
        None,
        "",
        None,
        None,
        ProcessMode::from("fresh"),
        false,
    )
    .expect("manager should be created");
    manager.handlers.clear();
    let handler = Arc::new(FakeSourceHandler::new("custom_source"));
    manager.register_handler(Some(handler.clone() as Arc<dyn SourceHandler>));

    let source = FakeConfigSource {
        source_type: "custom_source".to_owned(),
    };
    assert!(!source.is_repository());
    assert_eq!(source.get_directory(base.path()), base.path());
    source.validate().expect("fake source should validate");
    manager
        .process_sources(&CancellationToken::new(), &[&source])
        .await
        .expect("custom handler should process source");
    assert_eq!(
        *handler
            .processed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        vec!["custom_source".to_owned()]
    );

    let missing_sources: [Option<&FakeConfigSource>; 1] = [None];
    let missing_error = manager
        .process_sources(&CancellationToken::new(), &missing_sources)
        .await
        .expect_err("missing source should fail");
    assert!(missing_error.to_string().contains("source[0] is missing"));
}

/// 验证 manager、默认 recorder 和动态 handler 都能安全借用当前 async 栈帧。
///
/// 该测试刻意让两个 trait object 持有局部 `AtomicBool` 引用；若构造器或 handler
/// 容器重新隐含 `'static`，本测试会在编译阶段失败，而不是依赖运行时 drop 约定。
#[tokio::test]
async fn source_handler_manager_preserves_scoped_trait_object_lifetimes() {
    let failure_recorded = AtomicBool::new(false);
    let source_processed = AtomicBool::new(false);
    let recorder: Arc<dyn ActivityRecorder + '_> = Arc::new(BorrowedActivityRecorder {
        failure_recorded: &failure_recorded,
    });
    let base = TempDir::new().expect("temp dir should be created");
    let mut manager = new_source_handler_manager(
        base.path(),
        None,
        "",
        None,
        Some(recorder),
        ProcessMode::from("fresh"),
        false,
    )
    .expect("scoped recorder manager should be created");

    manager.handlers.clear();
    let source = FakeConfigSource {
        source_type: "scoped_source".to_owned(),
    };
    manager
        .process_sources(&CancellationToken::new(), &[&source])
        .await
        .expect("missing handler remains a non-fatal result");
    assert!(failure_recorded.load(Ordering::SeqCst));

    let handler: Arc<dyn SourceHandler + '_> = Arc::new(BorrowedSourceHandler {
        source_processed: &source_processed,
    });
    manager.register_handler(Some(handler));
    manager
        .process_sources(&CancellationToken::new(), &[&source])
        .await
        .expect("scoped handler should process source");
    assert!(source_processed.load(Ordering::SeqCst));
}

/// 创建使用生产 URL 选择逻辑的本地 file source。
fn git_test_source(repo: &Path, git_ref: Option<&str>) -> GitRepositorySource {
    GitRepositorySource {
        base: BaseSource {
            source_type: "git_repository".to_owned(),
        },
        git_info: GitInfo {
            git_type: "test-file".to_owned(),
            host: "test-file".to_owned(),
            repo: repo.display().to_string(),
            git_ref: git_ref.map(str::to_owned),
            ..GitInfo::default()
        },
        ..GitRepositorySource::default()
    }
}

/// 显式目录和默认目录使用相同的浅克隆、分支、提交及 upstream 语义。
#[tokio::test]
async fn repository_sources_clone_default_branch_named_branch_and_commit() {
    let (fixture, origin, initial) = create_bare_git_repo();
    let worktree = fixture.path().join("worktree");
    for revision in ["second", "third"] {
        std::fs::write(worktree.join("README.md"), revision).unwrap();
        git_run(&worktree, &["commit", "-am", revision]);
    }
    let main_sha = git_output(&worktree, &["rev-parse", "HEAD"]);
    git_run(&worktree, &["checkout", "-b", "feature/nested"]);
    std::fs::write(worktree.join("README.md"), "feature").unwrap();
    git_run(&worktree, &["commit", "-am", "feature"]);
    let feature_sha = git_output(&worktree, &["rev-parse", "HEAD"]);
    git_run(&worktree, &["tag", "feature/nested", &initial]);
    git_run(&worktree, &["push", "--mirror", path_text(&origin)]);
    let base = TempDir::new().unwrap();
    for explicit_path in [false, true] {
        for mode in ["fresh", "allow-prefetched"] {
            let handler = new_git_handler(
                base.path().join(format!("{mode}-{explicit_path}")),
                None,
                "",
                None,
                None,
                ProcessMode::from(mode),
                false,
            );
            for (name, git_ref, expected_sha, branch) in [
                ("default", None, &main_sha, Some("main")),
                (
                    "branch",
                    Some("refs/heads/feature/nested"),
                    &feature_sha,
                    Some("feature/nested"),
                ),
                ("commit", Some(initial.as_str()), &initial, None),
            ] {
                let target = base
                    .path()
                    .join(format!("{mode}-{explicit_path}"))
                    .join(name);
                if name == "branch" {
                    std::fs::create_dir_all(&target).unwrap();
                }
                let mut source = git_test_source(&origin, git_ref);
                source.git_info.url = Some(format!("file://{}", origin.display()));
                source.git_info.repo = name.to_owned();
                if explicit_path {
                    source.mount_path = target.display().to_string();
                }
                handler
                    .process(&CancellationToken::new(), &source)
                    .await
                    .unwrap();
                assert_eq!(git_output(&target, &["rev-parse", "HEAD"]), *expected_sha);
                assert_eq!(git_output(&target, &["rev-list", "--count", "HEAD"]), "1");
                assert_eq!(
                    git_output(&target, &["rev-parse", "--is-shallow-repository"]),
                    "true"
                );
                assert!(git_output(&target, &["tag", "--list"]).is_empty());
                assert_eq!(
                    git_output(&target, &["remote", "get-url", "origin"]),
                    format!("file://{}", origin.display())
                );
                assert_eq!(
                    git_output(&target, &["config", "--get-all", "remote.origin.fetch"]),
                    "+refs/heads/*:refs/remotes/origin/*"
                );
                if let Some(branch) = branch {
                    assert_eq!(
                        git_output(&target, &["symbolic-ref", "--short", "HEAD"]),
                        branch
                    );
                    assert_eq!(
                        git_output(&target, &["rev-parse", "--abbrev-ref", "@{upstream}"]),
                        format!("origin/{branch}")
                    );
                } else {
                    assert!(
                        !std::fs::read_to_string(target.join(".git/HEAD"))
                            .unwrap()
                            .starts_with("ref:")
                    );
                }
            }
        }
    }
}

/// 空远端允许创建首个提交；未请求具体 ref 时不要求 HEAD 已有 commit。
#[tokio::test]
async fn repository_sources_clone_empty_remote() {
    let fixture = TempDir::new().unwrap();
    let origin = fixture.path().join("empty.git");
    git_run(
        Path::new(""),
        &["init", "--bare", "-b", "main", path_text(&origin)],
    );
    for mode in ["fresh", "allow-prefetched"] {
        let base = fixture.path().join(mode);
        let target = base.join("repository");
        let mut source = git_test_source(&origin, None);
        source.mount_path = target.display().to_string();
        let handler = new_git_handler(&base, None, "", None, None, ProcessMode::from(mode), true);
        handler
            .process(&CancellationToken::new(), &source)
            .await
            .unwrap();
        assert!(target.join(".git").is_dir());
        assert!(git_output(&target, &["for-each-ref", "--format=%(refname)"]).is_empty());
        git_run(&target, &["config", "user.email", "test@example.com"]);
        git_run(&target, &["config", "user.name", "Test User"]);
        git_run(&target, &["commit", "--allow-empty", "-m", "first"]);
        assert_eq!(git_output(&target, &["rev-list", "--count", "HEAD"]), "1");
    }
}

/// Fetch 保留协议回退和重试，同时不清理已有仓库内容。
#[tokio::test]
async fn repository_sources_fetch_retries_with_http1_fallback() {
    let (_fixture, origin, initial) = create_bare_git_repo();
    let base = TempDir::new().unwrap();
    let bin = base.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let wrapper = bin.join("git");
    std::fs::write(
        &wrapper,
        r#"#!/bin/sh
case " $* " in
  *" fetch "*)
    count=0
    if [ -f "$GIT_RETRY_STATE" ]; then count=$(cat "$GIT_RETRY_STATE"); fi
    count=$((count + 1))
    echo "$count" > "$GIT_RETRY_STATE"
    if [ "$count" -lt 4 ]; then
      echo 'simulated transient fetch failure' >&2
      exit 128
    fi
    case " $* " in
      *" http.version=HTTP/1.1 "*) ;;
      *) echo 'HTTP/1.1 fallback missing' >&2; exit 1 ;;
    esac
    ;;
esac
exec "$REAL_GIT" "$@"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var_os("PATH").unwrap();
    let real_git = std::env::split_paths(&inherited_path)
        .map(|path| path.join("git"))
        .find(|path| path.is_file())
        .unwrap();
    let test_path =
        std::env::join_paths(std::iter::once(bin).chain(std::env::split_paths(&inherited_path)))
            .unwrap();
    let target = base.path().join("repository");
    std::fs::create_dir(&target).unwrap();
    git_run(&target, &["init"]);
    std::fs::write(target.join("keep"), "local data").unwrap();
    let state = base.path().join("attempts");
    let git_url = format!("file://{}", origin.display());
    let args = vec![
        "fetch".to_owned(),
        "--depth=1".to_owned(),
        "--no-tags".to_owned(),
        "--".to_owned(),
        git_url.clone(),
        "refs/heads/main".to_owned(),
    ];
    let handler = new_git_handler(
        base.path(),
        None,
        "",
        None,
        None,
        ProcessMode::from("fresh"),
        false,
    );
    GIT_ENVIRONMENT
        .scope(
            vec![
                ("PATH".into(), test_path),
                ("REAL_GIT".into(), real_git.into_os_string()),
                ("GIT_RETRY_STATE".into(), state.clone().into_os_string()),
            ],
            handler.run_git_fetch_with_retry_delay(
                &CancellationToken::new(),
                &target,
                &git_url,
                &git_url,
                None,
                &args,
                std::time::Duration::ZERO,
            ),
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read_to_string(state).unwrap().trim(), "4");
    assert_eq!(git_output(&target, &["rev-parse", "FETCH_HEAD"]), initial);
    assert_eq!(
        std::fs::read_to_string(target.join("keep")).unwrap(),
        "local data"
    );
}

/// 不允许 fresh 模式删除目标中的用户数据或穿过符号链接。
#[tokio::test]
async fn repository_sources_reject_nonempty_symlink_and_parent_paths() {
    let base = TempDir::new().unwrap();
    let occupied = base.path().join("occupied");
    std::fs::create_dir(&occupied).unwrap();
    std::fs::write(occupied.join("keep"), "user data").unwrap();
    let symlink = base.path().join("link");
    std::os::unix::fs::symlink(&occupied, &symlink).unwrap();
    let broken = base.path().join("broken");
    std::os::unix::fs::symlink(base.path().join("missing"), &broken).unwrap();
    for mode in ["fresh", "allow-prefetched", "resume"] {
        let handler = new_git_handler(
            base.path(),
            None,
            "",
            None,
            None,
            ProcessMode::from(mode),
            false,
        );
        for target in [
            occupied.clone(),
            symlink.clone(),
            symlink.join("child"),
            broken.clone(),
            base.path().join("../escape"),
            PathBuf::from("relative"),
        ] {
            let mut source = git_test_source(Path::new("/nonexistent.git"), None);
            source.mount_path = target.display().to_string();
            let error = handler
                .process(&CancellationToken::new(), &source)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("repository directory"),
                "{error}"
            );
        }
    }
    assert_eq!(
        std::fs::read_to_string(occupied.join("keep")).unwrap(),
        "user data"
    );
    assert!(!occupied.join("child").exists());
}

/// 新建、拒绝覆盖、prefetched 保留本地修改和 resume 更新均与目录来源无关。
#[tokio::test]
async fn repository_sources_lifecycle_is_independent_of_directory_selection() {
    let (fixture, origin, _) = create_bare_git_repo();
    let base = TempDir::new().unwrap();
    for explicit_path in [false, true] {
        let mut source = git_test_source(&origin, None);
        source.git_info.url = Some(format!("file://{}", origin.display()));
        source.git_info.repo = format!("repo-{explicit_path}");
        let target = base.path().join(&source.git_info.repo);
        if explicit_path {
            source.mount_path = target.display().to_string();
        }
        let fresh = new_git_handler(
            base.path(),
            None,
            "",
            None,
            None,
            ProcessMode::from("fresh"),
            false,
        );
        fresh
            .process(&CancellationToken::new(), &source)
            .await
            .unwrap();
        std::fs::write(target.join("README.md"), "local changes").unwrap();
        assert!(
            fresh
                .process(&CancellationToken::new(), &source)
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read_to_string(target.join("README.md")).unwrap(),
            "local changes"
        );
        let prefetched = new_git_handler(
            base.path(),
            None,
            "",
            None,
            None,
            ProcessMode::from("allow-prefetched"),
            false,
        );
        prefetched
            .process(&CancellationToken::new(), &source)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(target.join("README.md")).unwrap(),
            "local changes"
        );

        let worktree = fixture.path().join("worktree");
        let content = format!("upstream-{explicit_path}");
        std::fs::write(worktree.join("README.md"), &content).unwrap();
        git_run(&worktree, &["commit", "-am", &content]);
        git_run(&worktree, &["push", path_text(&origin), "main"]);
        let resume = new_git_handler(
            base.path(),
            None,
            "",
            None,
            None,
            ProcessMode::from("resume"),
            false,
        );
        resume
            .process(&CancellationToken::new(), &source)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(target.join("README.md")).unwrap(),
            content
        );
        assert_eq!(
            git_output(&target, &["rev-parse", "HEAD"]),
            git_output(&worktree, &["rev-parse", "HEAD"])
        );
    }
}

/// 分支输入不能退化为同名 tag；失败保留现场，再次首次初始化拒绝覆盖。
#[tokio::test]
async fn repository_sources_reject_tag_as_branch_without_cleanup() {
    let (fixture, origin, _) = create_bare_git_repo();
    let worktree = fixture.path().join("worktree");
    git_run(&worktree, &["tag", "tag-only"]);
    git_run(&worktree, &["push", "--tags", path_text(&origin)]);
    let base = TempDir::new().unwrap();
    let handler = new_git_handler(
        base.path(),
        None,
        "",
        None,
        None,
        ProcessMode::from("fresh"),
        false,
    );
    for explicit_path in [false, true] {
        for existing in [false, true] {
            let target = base
                .path()
                .join(format!("target-{explicit_path}-{existing}"));
            if existing {
                std::fs::create_dir(&target).unwrap();
            }
            let mut source = git_test_source(&origin, Some("refs/heads/tag-only"));
            source.git_info.url = Some(format!("file://{}", origin.display()));
            source.git_info.repo = target.file_name().unwrap().to_str().unwrap().to_owned();
            if explicit_path {
                source.mount_path = target.display().to_string();
            }
            assert!(
                handler
                    .process(&CancellationToken::new(), &source)
                    .await
                    .is_err()
            );
            assert!(target.join(".git").is_dir());
            assert_eq!(
                std::fs::read_to_string(target.join("README.md")).unwrap(),
                "hello\n"
            );
            source.git_info.git_ref = Some("refs/heads/main".to_owned());
            let error = handler
                .process(&CancellationToken::new(), &source)
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("repository directory must be empty")
            );
            assert_eq!(
                std::fs::read_to_string(target.join("README.md")).unwrap(),
                "hello\n"
            );
        }
    }
}

/// 取消或丢弃初始化只停止执行，保留已写入内容，不进行目录清理或原地重试。
#[tokio::test]
async fn repository_sources_cancel_and_abort_leave_partial_repository() {
    let (_fixture, origin, _) = create_bare_git_repo();
    let base = TempDir::new().unwrap();
    for explicit_path in [false, true] {
        for abort in [false, true] {
            for existing in [false, true] {
                let target = base
                    .path()
                    .join(format!("target-{explicit_path}-{abort}-{existing}"));
                if existing {
                    std::fs::create_dir(&target).unwrap();
                }
                let branch = format!("delayed-{explicit_path}-{abort}-{existing}");
                let mut source = git_test_source(&origin, Some(&branch));
                source.git_info.url = Some(format!("file://{}", origin.display()));
                source.git_info.repo = target.file_name().unwrap().to_str().unwrap().to_owned();
                if explicit_path {
                    source.mount_path = target.display().to_string();
                }
                let cancellation = CancellationToken::new();
                let task_cancellation = cancellation.clone();
                let base_path = base.path().to_owned();
                let task_source = source.clone();
                let task = tokio::spawn(async move {
                    let handler = new_git_handler(
                        base_path,
                        None,
                        "",
                        None,
                        None,
                        ProcessMode::from("fresh"),
                        false,
                    );
                    handler.process(&task_cancellation, &task_source).await
                });
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while !target.join(".git/config").exists() {
                        assert!(!task.is_finished(), "preparation unexpectedly finished");
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                if abort {
                    task.abort();
                } else {
                    cancellation.cancel();
                }
                let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
                    .await
                    .unwrap();
                if abort {
                    assert!(result.unwrap_err().is_cancelled());
                } else {
                    assert!(result.unwrap().is_err());
                }
                assert!(target.join(".git/config").is_file());
            }
        }
    }
}

/// 创建 main branch、一个 commit 及其 bare clone，并返回保持根目录存活的句柄。
fn create_bare_git_repo() -> (TempDir, PathBuf, String) {
    let root = TempDir::new().expect("git fixture temp dir should be created");
    let worktree = root.path().join("worktree");
    let bare_repo = root.path().join("origin.git");
    git_run(Path::new(""), &["init", "-b", "main", path_text(&worktree)]);
    git_run(&worktree, &["config", "user.email", "test@example.com"]);
    git_run(&worktree, &["config", "user.name", "Test User"]);
    std::fs::write(worktree.join("README.md"), b"hello\n")
        .expect("README fixture should be written");
    git_run(&worktree, &["add", "README.md"]);
    git_run(&worktree, &["commit", "-m", "initial"]);
    let commit_sha = git_output(&worktree, &["rev-parse", "HEAD"]);
    git_run(
        Path::new(""),
        &[
            "clone",
            "--bare",
            path_text(&worktree),
            path_text(&bare_repo),
        ],
    );
    (root, bare_repo, commit_sha)
}

/// 执行 fixture 所需的同步 git 命令，并在失败时展示合并诊断。
fn git_run(dir: &Path, args: &[&str]) {
    let mut command = Command::new("git");
    command.args(args);
    if !dir.as_os_str().is_empty() {
        command.current_dir(dir);
    }
    let output = command.output().expect("git should be installed");
    assert!(
        output.status.success(),
        "git {} failed: {}{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// 执行 git 并返回去除首尾空白的 stdout。
fn git_output(dir: &Path, args: &[&str]) -> String {
    let mut command = Command::new("git");
    command.args(args);
    if !dir.as_os_str().is_empty() {
        command.current_dir(dir);
    }
    let output = command.output().expect("git should be installed");
    assert!(
        output.status.success(),
        "git {} failed: {}{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// 把测试路径借为命令参数；fixture 路径由 tempfile 保证为 UTF-8 可表示。
fn path_text(path: &Path) -> &str {
    path.to_str().expect("fixture path should be valid UTF-8")
}

/// fake provider 记录最后一次认证调用，便于验证 permission 和 source URL。
struct FakeSourceAuthProvider {
    authenticated_url: String,
    ok: bool,
    observed: Mutex<ObservedAuthentication>,
}

impl FakeSourceAuthProvider {
    /// 创建带固定认证结果的 fake provider。
    fn new(authenticated_url: &str, ok: bool) -> Self {
        Self {
            authenticated_url: authenticated_url.to_owned(),
            ok,
            observed: Mutex::new(ObservedAuthentication::default()),
        }
    }
}

impl SourceAuthProvider for FakeSourceAuthProvider {
    /// 记录输入并返回构造时配置的结果。
    fn authenticate_url(
        &self,
        _auth_context: &AuthContext,
        source_url: &str,
        permission: &Permission,
    ) -> (String, bool) {
        *self
            .observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = ObservedAuthentication {
            source_url: source_url.to_owned(),
            permission: permission.clone(),
        };
        (self.authenticated_url.clone(), self.ok)
    }

    /// 用公开 URL 替换认证 URL。
    fn sanitize_output(&self, output: &str, git_url: &str, authenticated_url: &str) -> String {
        output.replace(authenticated_url, git_url)
    }
}

/// fake provider 观察到的最后一次认证输入。
#[derive(Default)]
struct ObservedAuthentication {
    source_url: String,
    permission: Permission,
}

/// 可由 manager 动态分派的非 Git source。
struct FakeConfigSource {
    source_type: String,
}

impl FakeConfigSource {
    /// 动态 fixture 不是 Git repository。
    fn is_repository(&self) -> bool {
        false
    }

    /// 非仓库 fixture 原样使用 base 目录。
    fn get_directory(&self, base_dir: &Path) -> PathBuf {
        base_dir.to_path_buf()
    }

    /// 构造完成的 fixture 始终有效。
    fn validate(&self) -> SourcesResult<()> {
        Ok(())
    }
}

impl SourceValue for FakeConfigSource {
    /// 返回测试配置的动态标签。
    fn get_type(&self) -> &str {
        &self.source_type
    }
}

/// 记录已处理 source 标签的 fake handler。
struct FakeSourceHandler {
    source_type: String,
    processed: Mutex<Vec<String>>,
}

impl FakeSourceHandler {
    /// 创建只接受一个标签的 fake handler。
    fn new(source_type: &str) -> Self {
        Self {
            source_type: source_type.to_owned(),
            processed: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl SourceHandler for FakeSourceHandler {
    /// 只匹配构造时指定的 source 标签。
    fn can_handle(&self, source_type: &str) -> bool {
        source_type == self.source_type
    }

    /// 记录 source 标签并成功返回。
    async fn process(
        &self,
        _cancellation: &CancellationToken,
        source: &dyn SourceValue,
    ) -> SourcesResult<()> {
        self.processed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(source.get_type().to_owned());
        Ok(())
    }
}

/// 借用局部标志的 activity recorder，用于证明 manager 不要求 `'static`。
struct BorrowedActivityRecorder<'a> {
    failure_recorded: &'a AtomicBool,
}

#[async_trait]
impl ActivityRecorder for BorrowedActivityRecorder<'_> {
    /// 本生命周期测试不需要保存普通 activity。
    fn record_activity(
        &self,
        _level: LogLevel,
        _category: LogCategory,
        _message: String,
        _extra: BTreeMap<String, Value>,
    ) {
    }

    /// 返回无后台任务的 stopper，避免测试引入计时副作用。
    fn record_long_running_activity(
        &self,
        _category: LogCategory,
        _message: String,
    ) -> Arc<dyn Stopper> {
        Arc::new(NoopStopper)
    }

    /// 记录 manager 确实通过借用 wrapper 调用了 failure 路径。
    async fn record_failure_result(
        &self,
        _session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        self.failure_recorded.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// 借用局部标志的动态 source handler，用于证明注册容器继承 manager 生命周期。
struct BorrowedSourceHandler<'a> {
    source_processed: &'a AtomicBool,
}

#[async_trait]
impl SourceHandler for BorrowedSourceHandler<'_> {
    /// 只处理生命周期测试使用的固定 source 标签。
    fn can_handle(&self, source_type: &str) -> bool {
        source_type == "scoped_source"
    }

    /// 标记处理已发生；不创建文件、网络连接或子进程。
    async fn process(
        &self,
        _cancellation: &CancellationToken,
        _source: &dyn SourceValue,
    ) -> SourcesResult<()> {
        self.source_processed.store(true, Ordering::SeqCst);
        Ok(())
    }
}
