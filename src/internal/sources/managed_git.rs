//! Managed Agents 的资源 clone：浅历史、精确 checkout、原子安装和工作树保留。

use std::{
    os::fd::{AsRawFd, OwnedFd},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use tokio_util::sync::CancellationToken;

use super::{GitHandler, SourcesError, SourcesResult};
use crate::internal::{
    api::{LogCategory, LogLevel},
    config::{GitCheckout, GitRepositorySource, git_resources},
    process::CaptureMode,
};

impl GitHandler<'_> {
    /// 凭证仅由已启动的 CONNECT relay 注入，资源 URL 和 origin 始终不含凭证。
    pub async fn process_managed_repository(
        &self,
        cancellation: &CancellationToken,
        source: &GitRepositorySource,
        environment: &[(String, String)],
    ) -> SourcesResult<()> {
        git_resources::validate_resource(source).map_err(source_error)?;
        let record =
            async |status: &str, level, duration_ms: Option<u64>, failure_reason: Option<&str>| {
                if let Some(recorder) = &self.activity_recorder {
                    recorder
                        .record_activity_and_wait(
                            level,
                            LogCategory::Init,
                            format!("Git repository preparation {status}"),
                            std::collections::BTreeMap::from([
                                (
                                    "resource_type".to_owned(),
                                    serde_json::json!("git_repository"),
                                ),
                                ("status".to_owned(), serde_json::json!(status)),
                                ("duration_ms".to_owned(), serde_json::json!(duration_ms)),
                                (
                                    "failure_reason".to_owned(),
                                    serde_json::json!(failure_reason),
                                ),
                                ("url".to_owned(), serde_json::json!(source.git_info.url)),
                                (
                                    "mount_path".to_owned(),
                                    serde_json::json!(source.mount_path),
                                ),
                            ]),
                        )
                        .await;
                }
            };
        record("started", LogLevel::Info, None, None).await;
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(120),
            self.prepare_managed_repository(
                cancellation,
                source,
                environment,
                Path::new("/workspace"),
            ),
        )
        .await
        .unwrap_or_else(|_| Err(SourcesError::message("repository preparation timed out")));
        let duration_ms = Some(started.elapsed().as_millis() as u64);
        match &result {
            Ok(()) => record("ready", LogLevel::Info, duration_ms, None).await,
            Err(error) => {
                record(
                    "failed",
                    LogLevel::Error,
                    duration_ms,
                    Some(git_preparation_failure(&error.to_string())),
                )
                .await
            }
        }
        result
    }

    /// 在目标父目录创建临时 clone，成功后以不可覆盖的 rename 安装。
    async fn prepare_managed_repository(
        &self,
        cancellation: &CancellationToken,
        source: &GitRepositorySource,
        environment: &[(String, String)],
        workspace: &Path,
    ) -> SourcesResult<()> {
        let target = git_resources::validate_mount_path(
            source.mount_path.as_deref().unwrap_or_default(),
            workspace,
        )
        .map_err(source_error)?;
        let parent = target
            .parent()
            .ok_or_else(|| SourcesError::message("repository parent is missing"))?;
        let parent_fd = open_real_directories(parent)?;
        let anchored_parent = descriptor_path(&parent_fd);
        let target_name = target
            .file_name()
            .ok_or_else(|| SourcesError::message("repository name is missing"))?;
        let anchored_target = anchored_parent.join(target_name);
        let git_url = source
            .git_info
            .url
            .as_deref()
            .ok_or_else(|| SourcesError::message("repository URL is missing"))?;
        let mut environment = environment.to_vec();
        environment.extend([
            ("GIT_TERMINAL_PROMPT".to_owned(), "0".to_owned()),
            ("GIT_CONFIG_NOSYSTEM".to_owned(), "1".to_owned()),
            ("GIT_CONFIG_GLOBAL".to_owned(), "/dev/null".to_owned()),
            // Git 的出口必须经过同一个 relay；NO_PROXY 不能让 GitHub 绕行。
            ("NO_PROXY".to_owned(), String::new()),
            ("no_proxy".to_owned(), String::new()),
        ]);
        if let Ok(metadata) = std::fs::symlink_metadata(&anchored_target) {
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(SourcesError::message(
                    "repository target is not a real directory",
                ));
            }
            let repository_fd = nix::fcntl::openat(
                &parent_fd,
                target_name,
                directory_flags(),
                nix::sys::stat::Mode::empty(),
            )
            .map_err(source_error)?;
            let repository_path = descriptor_path(&repository_fd);
            let metadata =
                std::fs::symlink_metadata(repository_path.join(".git")).map_err(source_error)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(SourcesError::message(
                    "existing repository has no real .git directory",
                ));
            }
            // 恢复仅校验仓库身份；不 fetch/reset/checkout，保留未提交内容和本地分支。
            self.managed_git_output(
                cancellation,
                Some(&repository_path),
                &environment,
                &[
                    "config".to_owned(),
                    "--local".to_owned(),
                    "--get-regexp".to_owned(),
                    "^remote\\.origin\\.url$".to_owned(),
                    exact_git_config_pattern(git_url),
                ],
            )
            .await?;
            return Ok(());
        }
        if let Some(GitCheckout::Branch { name }) = &source.checkout {
            self.managed_git(
                cancellation,
                None,
                git_url,
                &environment,
                &[
                    "check-ref-format".to_owned(),
                    "--branch".to_owned(),
                    name.clone(),
                ],
            )
            .await?;
        }
        let temporary = tempfile::Builder::new()
            .prefix(".oma-git-")
            .tempdir_in(&anchored_parent)
            .map_err(source_error)?;
        let clone = temporary.path().join("repository");
        if let Some(GitCheckout::Commit { sha }) = &source.checkout {
            let sha = self
                .resolve_commit_id(cancellation, git_url, &environment, sha)
                .await?;
            let mut init_args = vec!["init".to_owned()];
            if sha.len() == 64 {
                init_args.push("--object-format=sha256".to_owned());
            }
            init_args.push("repository".to_owned());
            self.managed_git(
                cancellation,
                Some(temporary.path()),
                git_url,
                &environment,
                &init_args,
            )
            .await?;
            for args in [
                vec![
                    "remote".to_owned(),
                    "add".to_owned(),
                    "origin".to_owned(),
                    git_url.to_owned(),
                ],
                vec![
                    "config".to_owned(),
                    "remote.origin.tagOpt".to_owned(),
                    "--no-tags".to_owned(),
                ],
                vec![
                    "config".to_owned(),
                    "--unset-all".to_owned(),
                    "remote.origin.fetch".to_owned(),
                ],
                vec![
                    "fetch".to_owned(),
                    "--depth=1".to_owned(),
                    "--no-tags".to_owned(),
                    "--no-recurse-submodules".to_owned(),
                    "origin".to_owned(),
                    sha.clone(),
                ],
            ] {
                self.managed_git(cancellation, Some(&clone), git_url, &environment, &args)
                    .await?;
            }
            let fetched = self
                .managed_git_output(
                    cancellation,
                    Some(&clone),
                    &environment,
                    &["rev-parse".to_owned(), "FETCH_HEAD^{commit}".to_owned()],
                )
                .await?;
            if fetched.trim() != sha {
                return Err(SourcesError::message(
                    "fetched object does not match the requested commit",
                ));
            }
            self.managed_git(
                cancellation,
                Some(&clone),
                git_url,
                &environment,
                &[
                    "checkout".to_owned(),
                    "--detach".to_owned(),
                    "FETCH_HEAD".to_owned(),
                ],
            )
            .await?;
        } else {
            let mut args = vec![
                "clone".to_owned(),
                "--depth=1".to_owned(),
                "--single-branch".to_owned(),
                "--no-tags".to_owned(),
                "--no-recurse-submodules".to_owned(),
            ];
            if let Some(GitCheckout::Branch { name }) = &source.checkout {
                args.extend(["--branch".to_owned(), name.clone()]);
            }
            args.extend(["--".to_owned(), git_url.to_owned(), "repository".to_owned()]);
            self.managed_git(
                cancellation,
                Some(temporary.path()),
                git_url,
                &environment,
                &args,
            )
            .await?;
            if let Some(GitCheckout::Branch { name }) = &source.checkout {
                // clone --branch 也接受 tag；只有精确的本地 branch HEAD 才满足资源合同。
                let head = self
                    .managed_git_output(
                        cancellation,
                        Some(&clone),
                        &environment,
                        &[
                            "symbolic-ref".to_owned(),
                            "--quiet".to_owned(),
                            "HEAD".to_owned(),
                        ],
                    )
                    .await;
                if !head.is_ok_and(|head| head.trim() == format!("refs/heads/{name}")) {
                    return Err(SourcesError::message(
                        "requested branch was not checked out; a tag cannot satisfy branch checkout",
                    ));
                }
            }
        }
        nix::fcntl::renameat2(
            nix::fcntl::AT_FDCWD,
            &clone,
            &parent_fd,
            target_name,
            nix::fcntl::RenameFlags::RENAME_NOREPLACE,
        )
        .map_err(source_error)?;
        tracing::info!(mount_path = %target.display(), "Prepared Git resource");
        Ok(())
    }

    /// 复用原有可取消的 Git 命令执行器，禁用 helper 和 checkout hooks。
    async fn managed_git(
        &self,
        cancellation: &CancellationToken,
        directory: Option<&Path>,
        url: &str,
        environment: &[(String, String)],
        args: &[String],
    ) -> SourcesResult<()> {
        self.run_git_command(
            cancellation,
            directory,
            url,
            url,
            None,
            environment,
            &managed_git_args(args),
        )
        .await
    }

    /// 短 SHA 只匹配服务端广告的对象 ID，不能被解释成同名 branch。
    async fn resolve_commit_id(
        &self,
        cancellation: &CancellationToken,
        url: &str,
        environment: &[(String, String)],
        sha: &str,
    ) -> SourcesResult<String> {
        let sha = sha.to_ascii_lowercase();
        if matches!(sha.len(), 40 | 64) {
            return Ok(sha);
        }
        let refs = self
            .managed_git_output(
                cancellation,
                None,
                environment,
                &["ls-remote".to_owned(), "--refs".to_owned(), url.to_owned()],
            )
            .await?;
        let mut matches = refs
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|object| {
                object.starts_with(&sha)
                    && matches!(object.len(), 40 | 64)
                    && object.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            .collect::<Vec<_>>();
        matches.sort_unstable();
        matches.dedup();
        if matches.len() != 1 {
            return Err(SourcesError::message(
                "commit prefix is not uniquely advertised by the repository; provide the full commit SHA",
            ));
        }
        Ok(matches[0].to_owned())
    }

    /// 读取 Git 的对象元数据，不把任意远端输出写入日志。
    async fn managed_git_output(
        &self,
        cancellation: &CancellationToken,
        directory: Option<&Path>,
        environment: &[(String, String)],
        args: &[String],
    ) -> SourcesResult<String> {
        let output = super::git::run_external_command(
            cancellation,
            "git",
            &managed_git_args(args),
            directory,
            environment,
            CaptureMode::Separate,
        )
        .await
        .map_err(source_error)?;
        let success = output.status.success();
        let (stdout, stderr) = output.output.expect_separate();
        if !success {
            return Err(SourcesError::message(git_preparation_failure(
                &String::from_utf8_lossy(&stderr),
            )));
        }
        String::from_utf8(stdout).map_err(source_error)
    }
}

/// 仅上报固定原因码，远端输出和可能包含凭证的错误文本不进入公开事件。
fn git_preparation_failure(error: &str) -> &'static str {
    let error = error.to_ascii_lowercase();
    let reasons: &[(&str, &[&str])] = &[
        ("cancelled", &["cancelled", "canceled"]),
        ("timed_out", &["timed_out", "timed out", "timeout"]),
        (
            "access_denied",
            &[
                "access_denied",
                "authentication failed",
                "could not read username",
                "repository not found",
                "error: 401",
                "error: 403",
            ],
        ),
        (
            "network_error",
            &[
                "network_error",
                "could not resolve host",
                "could not resolve proxy",
                "failed to connect",
                "unable to access",
                "connection reset",
                "ssl certificate",
            ],
        ),
        (
            "checkout_failed",
            &[
                "checkout_failed",
                "remote branch",
                "not our ref",
                "unadvertised object",
                "requested commit",
                "requested branch",
                "commit prefix",
                "could not resolve repository commit",
            ],
        ),
        (
            "path_conflict",
            &[
                "path_conflict",
                "not a real directory",
                "no real .git",
                "symlink",
                "file exists",
                "os error 13",
                "os error 28",
            ],
        ),
    ];
    reasons
        .iter()
        .find(|(_, patterns)| patterns.iter().any(|pattern| error.contains(pattern)))
        .map_or("unknown", |(reason, _)| *reason)
}

/// 统一关闭可写凭证 helper 和 checkout hooks。
fn managed_git_args(args: &[String]) -> Vec<String> {
    let mut safe_args = vec![
        "-c".to_owned(),
        "credential.helper=".to_owned(),
        "-c".to_owned(),
        "core.hooksPath=/dev/null".to_owned(),
    ];
    safe_args.extend_from_slice(args);
    safe_args
}

/// 使用 openat/O_NOFOLLOW 逐层固定真实目录，阻止并发 symlink 替换越界。
fn open_real_directories(directory: &Path) -> SourcesResult<OwnedFd> {
    let mut current = nix::fcntl::open("/", directory_flags(), nix::sys::stat::Mode::empty())
        .map_err(source_error)?;
    for component in directory.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        match nix::fcntl::openat(
            &current,
            name,
            directory_flags(),
            nix::sys::stat::Mode::empty(),
        ) {
            Ok(next) => current = next,
            Err(nix::errno::Errno::ENOENT) => {
                match nix::sys::stat::mkdirat(
                    &current,
                    name,
                    nix::sys::stat::Mode::from_bits_truncate(0o755),
                ) {
                    Ok(()) | Err(nix::errno::Errno::EEXIST) => {}
                    Err(error) => return Err(source_error(error)),
                }
                current = nix::fcntl::openat(
                    &current,
                    name,
                    directory_flags(),
                    nix::sys::stat::Mode::empty(),
                )
                .map_err(source_error)?;
            }
            Err(_) => {
                return Err(SourcesError::message(
                    "repository path contains a symlink or non-directory",
                ));
            }
        }
    }
    Ok(current)
}

/// descriptor 在 child chdir 后自动关闭，Git 只能看到固定后的工作目录。
fn directory_flags() -> nix::fcntl::OFlag {
    nix::fcntl::OFlag::O_RDONLY
        | nix::fcntl::OFlag::O_DIRECTORY
        | nix::fcntl::OFlag::O_NOFOLLOW
        | nix::fcntl::OFlag::O_CLOEXEC
}

/// /proc/self/fd 仅用于 parent 文件操作及 exec 前的 chdir，不进入 Git argv。
fn descriptor_path(descriptor: &OwnedFd) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", descriptor.as_raw_fd()))
}

/// Git config 的 POSIX 扩展正则按字面匹配完整 URL，避免恢复时误认其它仓库。
fn exact_git_config_pattern(value: &str) -> String {
    let mut pattern = String::from("^");
    for character in value.chars() {
        if ".^$[](){}*+?|\\".contains(character) {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('$');
    pattern
}

/// 保留非敏感的执行上下文，同时使用 sources 的既有错误类型。
fn source_error(error: impl std::fmt::Display) -> SourcesError {
    SourcesError::message(format!("prepare Git resource: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::{
        config::{BaseSource, GitInfo},
        sources::{ProcessMode, new_git_handler},
    };
    use std::{path::PathBuf, process::Command};
    use tempfile::TempDir;

    /// 原因码覆盖常见 Git 失败，绝不把原始错误或凭证作为原因上报。
    #[test]
    fn preparation_failure_reasons_are_safe_codes() {
        for (error, expected) in [
            (
                "fatal: could not read Username for 'https://github.com': terminal prompts disabled",
                "access_denied",
            ),
            (
                "fatal: Authentication failed for https://secret@github.com/owner/repo",
                "access_denied",
            ),
            (
                "fatal: Remote branch absent not found in upstream origin",
                "checkout_failed",
            ),
            (
                "fatal: unable to access URL: Could not resolve host",
                "network_error",
            ),
            ("repository preparation timed out", "timed_out"),
            ("command cancelled", "cancelled"),
            ("repository path contains a symlink", "path_conflict"),
            ("arbitrary remote output containing secret", "unknown"),
        ] {
            assert_eq!(git_preparation_failure(error), expected);
        }
    }

    /// 启动不读取宿主 Git 配置的真实 fixture 命令。
    fn git(directory: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args([
                "-c",
                "commit.gpgsign=false",
                "-c",
                "core.hooksPath=/dev/null",
            ])
            .args(args)
            .current_dir(directory)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_COUNT", "0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    /// 创建两个 main commits、一个 feature 分支和一个 tag，供浅 clone 验收。
    fn fixture() -> (TempDir, PathBuf, String) {
        let root = tempfile::tempdir().unwrap();
        let origin = root.path().join("origin");
        std::fs::create_dir(&origin).unwrap();
        git(&origin, &["init", "-b", "main"]);
        git(&origin, &["config", "user.name", "Fixture"]);
        git(&origin, &["config", "user.email", "fixture@example.test"]);
        std::fs::write(origin.join("file.txt"), "first").unwrap();
        git(&origin, &["add", "file.txt"]);
        git(&origin, &["commit", "-m", "first"]);
        let first = git(&origin, &["rev-parse", "HEAD"]);
        git(&origin, &["tag", "v1"]);
        std::fs::write(origin.join("file.txt"), "second").unwrap();
        git(&origin, &["commit", "-am", "second"]);
        git(&origin, &["checkout", "-b", "feature"]);
        std::fs::write(origin.join("file.txt"), "feature").unwrap();
        git(&origin, &["commit", "-am", "feature"]);
        git(&origin, &["checkout", "main"]);
        // 与 feature branch 同名的 tag 指向另一 commit，验证 branch 不被 tag 替代。
        git(&origin, &["tag", "feature", &first]);
        (root, origin, first)
    }

    /// 以临时目录代替生产 /workspace；生产入口仍只接受 HTTPS 与 /workspace。
    fn source(origin: &Path, target: &Path, checkout: Option<GitCheckout>) -> GitRepositorySource {
        GitRepositorySource {
            base: BaseSource {
                source_type: "git_repository".to_owned(),
            },
            git_info: GitInfo {
                repo: "owner/repo".to_owned(),
                url: Some(url::Url::from_directory_path(origin).unwrap().to_string()),
                ..GitInfo::default()
            },
            mount_path: Some(target.display().to_string()),
            checkout,
        }
    }

    /// 缺失分支、取消和越界 symlink 均失败，且不会遗留半成品 clone。
    #[tokio::test]
    async fn failures_cleanup_without_fallback_or_symlink_escape() {
        let _guard = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
        let (fixture, origin, _) = fixture();
        let workspace = fixture.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let handler = new_git_handler(
            &workspace,
            None,
            "",
            None,
            None,
            ProcessMode::from("fresh"),
            false,
        );
        let target = workspace.join("missing");
        let resource = source(
            &origin,
            &target,
            Some(GitCheckout::Branch {
                name: "does-not-exist".to_owned(),
            }),
        );
        assert!(
            handler
                .prepare_managed_repository(&CancellationToken::new(), &resource, &[], &workspace)
                .await
                .is_err()
        );
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(
            handler
                .prepare_managed_repository(
                    &cancelled,
                    &source(&origin, &target, None),
                    &[],
                    &workspace
                )
                .await
                .is_err()
        );
        assert!(!target.exists());
        let outside = fixture.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, workspace.join("escape")).unwrap();
        let resource = source(&origin, &workspace.join("escape/repo"), None);
        assert!(
            handler
                .prepare_managed_repository(&CancellationToken::new(), &resource, &[], &workspace)
                .await
                .is_err()
        );
        assert!(!outside.join("repo").exists());
    }

    /// 只有同名 tag 而没有 branch 时必须失败，且不安装 detached 工作树。
    #[tokio::test]
    async fn branch_checkout_rejects_a_tag_only_name() {
        let _guard = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
        let (fixture, origin, _) = fixture();
        let workspace = fixture.path().join("workspace");
        let target = workspace.join("repo");
        let handler = new_git_handler(
            &workspace,
            None,
            "",
            None,
            None,
            ProcessMode::from("fresh"),
            false,
        );
        let resource = source(
            &origin,
            &target,
            Some(GitCheckout::Branch {
                name: "v1".to_owned(),
            }),
        );
        let result = handler
            .prepare_managed_repository(&CancellationToken::new(), &resource, &[], &workspace)
            .await;
        assert!(result.is_err());
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    }

    /// 默认分支、显式分支和 commit 都只取一层历史，同名 branch 优先于 tag。
    #[tokio::test]
    async fn default_branch_branch_and_commit_are_shallow() {
        let _guard = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
        let (fixture, origin, first) = fixture();
        let workspace = fixture.path().join("workspace");
        let handler = new_git_handler(
            &workspace,
            None,
            "",
            None,
            None,
            ProcessMode::from("fresh"),
            false,
        );
        let cases = [
            ("default", None, "second"),
            (
                "branch",
                Some(GitCheckout::Branch {
                    name: "feature".to_owned(),
                }),
                "feature",
            ),
            (
                "commit",
                Some(GitCheckout::Commit { sha: first.clone() }),
                "first",
            ),
            (
                "short-commit",
                Some(GitCheckout::Commit {
                    sha: first[..9].to_owned(),
                }),
                "first",
            ),
        ];
        for (name, checkout, contents) in cases {
            let target = workspace.join(name);
            let resource = source(&origin, &target, checkout);
            handler
                .prepare_managed_repository(&CancellationToken::new(), &resource, &[], &workspace)
                .await
                .unwrap();
            assert_eq!(
                std::fs::read_to_string(target.join("file.txt")).unwrap(),
                contents
            );
            assert_eq!(git(&target, &["rev-list", "--count", "HEAD"]), "1");
            assert_eq!(
                git(&target, &["rev-parse", "--is-shallow-repository"]),
                "true"
            );
            assert_eq!(git(&target, &["tag", "--list"]), "");
            assert_eq!(
                git(&target, &["remote", "get-url", "origin"]),
                resource.git_info.url.as_deref().unwrap()
            );
            if name.ends_with("commit") {
                assert_eq!(git(&target, &["rev-parse", "HEAD"]), first);
                assert_eq!(git(&target, &["branch", "--show-current"]), "");
            }
            if name == "branch" {
                assert_eq!(
                    git(&target, &["symbolic-ref", "HEAD"]),
                    "refs/heads/feature"
                );
            }
        }
        assert_eq!(
            git(
                &workspace.join("branch"),
                &["config", "--get", "remote.origin.fetch"]
            ),
            "+refs/heads/feature:refs/remotes/origin/feature"
        );
    }

    /// 重启不改写工作树、未跟踪文件或本地分支，且拒绝另一仓库冒充恢复目标。
    #[tokio::test]
    async fn recovery_preserves_worktree_and_rejects_wrong_repository() {
        let _guard = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
        let (fixture, origin, _) = fixture();
        let workspace = fixture.path().join("workspace");
        let target = workspace.join("repo");
        let handler = new_git_handler(
            &workspace,
            None,
            "",
            None,
            None,
            ProcessMode::from("fresh"),
            false,
        );
        let resource = source(&origin, &target, None);
        handler
            .prepare_managed_repository(&CancellationToken::new(), &resource, &[], &workspace)
            .await
            .unwrap();
        git(&target, &["checkout", "-b", "my-work"]);
        std::fs::write(target.join("file.txt"), "uncommitted").unwrap();
        std::fs::write(target.join("new.txt"), "untracked").unwrap();
        handler
            .prepare_managed_repository(&CancellationToken::new(), &resource, &[], &workspace)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(target.join("file.txt")).unwrap(),
            "uncommitted"
        );
        assert!(target.join("new.txt").exists());
        assert_eq!(git(&target, &["branch", "--show-current"]), "my-work");
        git(
            &target,
            &[
                "remote",
                "set-url",
                "origin",
                "https://example.test/other.git",
            ],
        );
        assert!(
            handler
                .prepare_managed_repository(&CancellationToken::new(), &resource, &[], &workspace)
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read_to_string(target.join("file.txt")).unwrap(),
            "uncommitted"
        );
    }
}
