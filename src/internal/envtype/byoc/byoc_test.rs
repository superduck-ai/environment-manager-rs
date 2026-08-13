//! BYOC 的配置、初始化、source/lease 适配与认证回归测试。

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::internal::PROCESS_STATE_TEST_LOCK;
use crate::internal::api::{LogCategory, LogLevel, SessionError};
use crate::internal::config::{
    BaseSource, GitOutcomeInfo, GitRepositorySource, OutcomeField, SessionMode, Source,
    StartupContext,
};
use crate::internal::envtype::{EnvironmentType, LeaseManagerHandle};
use crate::internal::session::{ActivityRecorder, ActivityRecorderError, NoopStopper};
use crate::internal::util::{BoxError, Stopper};

use super::*;

/// 验证有效配置、默认 session mode 和三个生命周期 setter。
#[test]
fn test_new_validates_config_and_setters() {
    let cwd = std::env::temp_dir().join("byoc-valid-workspace");
    let raw = must_json(serde_json::json!({
        "environment_type": "byoc",
        "cwd": cwd,
        "task_setup_script": "echo setup",
        "environment": {"FOO": "bar"}
    }));
    let mut environment = new(&raw).expect("valid BYOC config should parse");
    assert_eq!(
        environment.get_cwd(),
        cwd.to_str().expect("temp path is UTF-8")
    );
    assert_eq!(environment.session_mode, SessionMode::New);
    assert!(
        format!("{environment:?}").contains("source_processor: \"configured\""),
        "production BYOC should have the sources adapter wired by default"
    );

    let startup = StartupContext {
        entrypoint: "bun run dev".to_owned(),
        ..StartupContext::default()
    };
    environment.set_startup_context(Some(startup.clone()));
    assert_eq!(environment.startup_context, Some(startup));

    let mut auth_context = crate::internal::auth::AuthContext::default();
    auth_context.set_session_id("session-1");
    environment.set_auth_context(Some(auth_context));
    assert_eq!(
        environment
            .auth_context
            .as_ref()
            .map(crate::internal::auth::AuthContext::get_session_id),
        Some("session-1")
    );
    environment.set_session_mode(SessionMode::ResumeCached);
    assert_eq!(environment.session_mode, SessionMode::ResumeCached);
}

/// 验证 JSON、类型、绝对路径、路径 clean 与 1 MiB 字节上限。
#[test]
fn new_rejects_invalid_config() {
    let error = new(b"{bad json").expect_err("bad JSON must fail");
    assert!(error.to_string().contains("failed to unmarshal"));

    let wrong_type = serde_json::to_vec(&serde_json::json!({
        "environment_type": "anthropic"
    }))
    .unwrap();
    assert!(
        new(&wrong_type)
            .expect_err("wrong environment type must fail")
            .to_string()
            .contains("invalid environment_type")
    );

    for cwd in ["relative", "/tmp/a/../b", "/tmp//b", "/tmp/b/"] {
        let raw = serde_json::to_vec(&serde_json::json!({
            "environment_type": "byoc",
            "cwd": cwd
        }))
        .unwrap();
        let message = new(&raw)
            .expect_err("non-absolute or non-clean CWD must fail")
            .to_string();
        if cwd == "relative" {
            assert!(message.contains("absolute path"), "{message}");
        } else {
            assert!(message.contains("path traversal"), "{message}");
        }
    }

    let huge = "x".repeat(MAX_TASK_SETUP_SCRIPT_BYTES + 1);
    let raw = serde_json::to_vec(&serde_json::json!({
        "environment_type": "byoc",
        "task_setup_script": huge
    }))
    .unwrap();
    assert!(
        new(&raw)
            .expect_err("oversized setup script must fail")
            .to_string()
            .contains("too large")
    );
}

/// 验证 null string 和 map 被映射为配置零值。
#[test]
fn accepts_null_and_missing_defaults() {
    let environment = new(br#"{
            "environment_type":"byoc",
            "cwd":null,
            "task_setup_script":null,
            "environment":{"EMPTY":null}
        }"#)
    .expect("Go-compatible null values should parse");
    assert_eq!(environment.get_cwd(), "");
    assert_eq!(environment.config.task_setup_script, "");
    assert_eq!(
        environment
            .config
            .environment
            .get("EMPTY")
            .map(String::as_str),
        Some("")
    );

    let environment = new(br#"{"environment_type":"byoc","environment":null}"#)
        .expect("null environment map should become empty");
    assert!(environment.config.environment.is_empty());
}

/// 验证只提取单 branch Git outcome，且重复 repo 以后者为准。
#[test]
fn test_extract_repo_branch_mapping() {
    let mut environment = minimal_environment();
    environment.set_startup_context(Some(StartupContext {
        outcomes: vec![
            git_outcome("anthropic/demo", &["first"]),
            OutcomeField {
                outcome_type: "other".to_owned(),
                git_info: GitOutcomeInfo {
                    repo: "ignored/other".to_owned(),
                    branches: vec!["ignored".to_owned()],
                    ..GitOutcomeInfo::default()
                },
            },
            git_outcome("ignored/no-branch", &[]),
            git_outcome("anthropic/demo", &["task-branch"]),
        ],
        ..StartupContext::default()
    }));
    let mapping = environment
        .extract_repo_branch_mapping(&CancellationToken::new())
        .expect("single branch outcomes should map");
    assert_eq!(mapping.len(), 1);
    assert_eq!(
        mapping.get("anthropic/demo").map(String::as_str),
        Some("task-branch")
    );

    environment
        .startup_context
        .as_mut()
        .expect("startup context is present")
        .outcomes
        .push(git_outcome("bad/repo", &["one", "two"]));
    let error = environment
        .extract_repo_branch_mapping(&CancellationToken::new())
        .expect_err("multiple branches must fail");
    assert!(error.to_string().contains("expected 0 or 1"));
}

/// 验证默认值、ambient 延迟、startup entrypoint 和 config 覆盖的优先级。
#[tokio::test]
async fn test_get_claude_environment_variables() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_get_claude_environment_variables"
    )) {
        return;
    }
    let _lock = PROCESS_STATE_TEST_LOCK.lock().await;
    let _delay = EnvVarGuard::set("CLAUDE_CODE_EXIT_AFTER_STOP_DELAY", "123");
    let mut environment = minimal_environment();
    environment.config.environment.extend([
        ("FOO".to_owned(), "bar".to_owned()),
        ("CLAUDE_CODE_DEBUG".to_owned(), "override".to_owned()),
        (
            "ANTHROPIC_BASE_URL".to_owned(),
            "https://api.example.test".to_owned(),
        ),
    ]);
    environment.set_startup_context(Some(StartupContext {
        entrypoint: "bun run dev".to_owned(),
        environment_variables: std::collections::HashMap::from([(
            "STARTUP_ONLY".to_owned(),
            "not-merged-here".to_owned(),
        )]),
        ..StartupContext::default()
    }));
    let variables = environment.get_claude_environment_variables();
    assert_eq!(
        variables.get("CLAUDE_CODE_REMOTE").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        variables
            .get("CLAUDE_CODE_EXIT_AFTER_STOP_DELAY")
            .map(String::as_str),
        Some("123")
    );
    assert_eq!(
        variables.get("CLAUDE_CODE_DEBUG").map(String::as_str),
        Some("override")
    );
    assert_eq!(
        variables.get("CLAUDE_CODE_ENTRYPOINT").map(String::as_str),
        Some("bun run dev")
    );
    assert_eq!(variables.get("FOO").map(String::as_str), Some("bar"));
    assert_eq!(
        variables.get("ANTHROPIC_BASE_URL").map(String::as_str),
        Some("https://api.example.test")
    );
    assert!(!variables.contains_key("STARTUP_ONLY"));
}

/// 验证无 shebang 脚本使用 bash/set-e，非零退出保留脚本名。
#[tokio::test]
async fn test_run_script_success_and_failure() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("script-output");
    let environment = minimal_environment();
    environment
        .run_script(
            &CancellationToken::new(),
            "task_setup",
            &format!("printf ok > {}", shell_quote(&output)),
        )
        .await
        .expect("setup script should succeed");
    assert_eq!(tokio::fs::read_to_string(&output).await.unwrap(), "ok");

    let error = environment
        .run_script(&CancellationToken::new(), "task_setup", "exit 3")
        .await
        .expect_err("nonzero script must fail");
    assert!(error.to_string().contains("task_setup script failed"));
}

/// 验证 settings/hook 的内容、mode 以及“已存在则保留”语义。
#[tokio::test]
async fn test_bootstrap_claude_settings_writes_and_preserves_files() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_bootstrap_claude_settings_writes_and_preserves_files"
    )) {
        return;
    }
    let _lock = PROCESS_STATE_TEST_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let _home = EnvVarGuard::set("HOME", home.path());
    let environment = minimal_environment();
    environment
        .bootstrap_claude_settings(&CancellationToken::new())
        .await
        .expect("bootstrap should succeed");

    let settings = home.path().join(".claude/settings.json");
    let hook = home.path().join(".claude/stop-hook-git-check.sh");
    assert_eq!(
        tokio::fs::read(&settings).await.unwrap(),
        crate::internal::envtype::shared::DEFAULT_SETTINGS_JSON
    );
    assert_eq!(
        tokio::fs::read(&hook).await.unwrap(),
        crate::internal::envtype::shared::STOP_HOOK_SCRIPT
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&settings).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&hook).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }

    tokio::fs::write(&settings, b"existing").await.unwrap();
    environment
        .bootstrap_claude_settings(&CancellationToken::new())
        .await
        .expect("second bootstrap should preserve files");
    assert_eq!(tokio::fs::read(&settings).await.unwrap(), b"existing");
}

/// `SKIP_GIT_CONFIG=true` 必须在创建 `.ssh` 之前无副作用返回。
#[tokio::test]
async fn test_setup_git_config_skip() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_setup_git_config_skip"
    )) {
        return;
    }
    let _lock = PROCESS_STATE_TEST_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let _home = EnvVarGuard::set("HOME", home.path());
    let _skip = EnvVarGuard::set("SKIP_GIT_CONFIG", "true");
    minimal_environment()
        .setup_git_config(&CancellationToken::new())
        .await
        .expect("skip path should succeed");
    assert!(!home.path().join(".ssh").exists());
}

/// 验证 new session 执行 setup，resume-cached 跳过，且切换 CWD 的副作用明确保留。
#[tokio::test]
async fn test_initialize_runs_setup_script_and_skips_on_resume_cached() {
    // BYOC initialize 同时修改 HOME 和 CWD；整个双分支场景必须留在同一隔离进程。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_initialize_runs_setup_script_and_skips_on_resume_cached"
    )) {
        return;
    }
    let _lock = PROCESS_STATE_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let _cwd = CurrentDirectoryGuard::capture();
    let _home = EnvVarGuard::set("HOME", home.path());
    let _skip = EnvVarGuard::set("SKIP_GIT_CONFIG", "true");

    let workspace = root.path().join("workspace");
    let mut environment = environment_for_cwd(&workspace, "printf initialized > init.txt");
    environment
        .initialize(&CancellationToken::new(), None)
        .await
        .expect("new session should initialize");
    assert_eq!(
        tokio::fs::read_to_string(workspace.join("init.txt"))
            .await
            .unwrap(),
        "initialized"
    );
    assert_eq!(
        std::env::current_dir().unwrap(),
        std::fs::canonicalize(&workspace).unwrap()
    );

    let resume_workspace = root.path().join("resume-workspace");
    let mut resumed = environment_for_cwd(&resume_workspace, "printf skipped > skipped.txt");
    resumed.set_session_mode(SessionMode::ResumeCached);
    resumed
        .initialize(&CancellationToken::new(), None)
        .await
        .expect("cached resume should initialize");
    assert!(!resume_workspace.join("skipped.txt").exists());
}

/// 预先取消时必须在 mkdir/chdir 之前返回，不留下工作目录。
#[tokio::test]
async fn pre_cancelled_initialize_has_no_filesystem_side_effect() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("never-created");
    let mut environment = environment_for_cwd(&workspace, "printf no");
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = environment
        .initialize(&cancellation, None)
        .await
        .expect_err("pre-cancelled initialization must fail");
    assert!(matches!(error, ByocError::Cancelled));
    assert!(!workspace.exists());
}

/// 默认 adapter 直接调用真实 sources manager，并保留其 base directory 校验。
#[tokio::test]
async fn default_source_processor_delegates_to_sources_manager() {
    let environment = minimal_environment();
    let processor = environment.source_processor.clone();
    let mut request = SourceProcessRequest {
        cwd: String::new(),
        auth_context: None,
        api_base_url: "https://api.example.test".to_owned(),
        repo_branches: std::collections::BTreeMap::from([(
            "anthropic/demo".to_owned(),
            "task-branch".to_owned(),
        )]),
        sources: Vec::new(),
        mode: SourceProcessMode::AllowPrefetched,
        should_validate: true,
    };
    let error = processor
        .process_sources(&CancellationToken::new(), &request, None)
        .await
        .expect_err("real sources manager should reject an empty base directory");
    assert_eq!(error.to_string(), "base directory cannot be empty");

    let directory = tempfile::tempdir().unwrap();
    request.cwd = directory.path().to_string_lossy().into_owned();
    processor
        .process_sources(&CancellationToken::new(), &request, None)
        .await
        .expect("empty source list should be a successful production no-op");

    request.repo_branches.clear();
    request.mode = SourceProcessMode::Fresh;
    request.should_validate = false;
    processor
        .setup_git_proxy_after_sources_processed(&CancellationToken::new(), &request, None)
        .await
        .expect("empty proxy source list should be a successful production no-op");
}

/// 默认 adapter 应把 manager 构造失败保留为独立阶段，而不是误包成 checkout 失败。
#[tokio::test]
async fn default_source_processor_preserves_manager_creation_error() {
    let mut environment = minimal_environment();
    environment.set_startup_context(Some(startup_with_source_and_outcome()));
    let error = environment
        .handle_branch_checkout(&CancellationToken::new(), None)
        .await
        .expect_err("empty BYOC CWD should fail manager construction");
    assert_eq!(
        error.to_string(),
        "failed to create source handler manager: base directory cannot be empty"
    );
}

/// 生产 adapter 必须把 initialize 的借用 recorder 传给真实 sources manager。
#[tokio::test]
async fn default_source_processor_forwards_activity_recorder() {
    let directory = tempfile::tempdir().unwrap();
    let mut environment = environment_for_cwd(directory.path(), "");
    let mut startup = startup_with_source_and_outcome();
    let Source::GitRepository(source) = &mut startup.sources[0];
    source.git_info.repo = "anthropic/demo".to_owned();
    source.git_info.host = "unsupported".to_owned();
    environment.set_startup_context(Some(startup));

    let recorder = CapturingActivityRecorder::default();
    let error = environment
        .handle_branch_checkout(&CancellationToken::new(), Some(&recorder))
        .await
        .expect_err("unsupported real Git source should fail");
    assert!(
        error
            .to_string()
            .contains("failed to process source type git_repository"),
        "{error}"
    );
    assert_eq!(recorder.failure_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        recorder
            .messages
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_slice(),
        ["Attempting to checkout branches for 1 repository"]
    );
}

/// 测试 setter 可替换默认 adapter，并传递 branch 映射和 allow-prefetched 参数。
#[tokio::test]
async fn branch_checkout_uses_replaceable_source_processor() {
    let mut environment = minimal_environment();
    environment.set_startup_context(Some(startup_with_source_and_outcome()));
    let processor = Arc::new(CapturingSourceProcessor::default());
    environment.set_source_processor(processor.clone());
    environment
        .handle_branch_checkout(&CancellationToken::new(), None)
        .await
        .expect("injected source processor should run");
    let requests = processor
        .requests
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].mode, SourceProcessMode::AllowPrefetched);
    assert!(requests[0].should_validate);
    assert_eq!(
        requests[0]
            .repo_branches
            .get("anthropic/demo")
            .map(String::as_str),
        Some("task-branch")
    );
}

/// 认证 transport 必须注入 Bearer、organization、runner/Claude 版本和 beta header。
#[tokio::test]
async fn test_contain_provide_auth_round_tripper_adds_headers() {
    use wiremock::matchers::{header, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let transport = ContainProvideAuthTransport::new("ingress-token", "org-1").unwrap();
    let mut request = reqwest::Client::new()
        .get("https://example.test/heartbeat")
        .build()
        .unwrap();
    transport.authorize_request(&mut request).unwrap();
    let capture = CaptureRoundTripper::default();
    capture.round_trip(&request);
    let captured_headers = capture.headers.lock().unwrap().clone().unwrap();
    assert_eq!(captured_headers["authorization"], "Bearer ingress-token");
    assert_eq!(captured_headers["x-organization-uuid"], "org-1");
    assert_eq!(request.headers()["authorization"], "Bearer ingress-token");
    assert_eq!(request.headers()["x-organization-uuid"], "org-1");
    assert_eq!(
        request.headers()["anthropic-beta"],
        "environments-2025-11-01"
    );
    assert!(
        request
            .headers()
            .contains_key("x-environment-runner-version")
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer ingress-token"))
        .and(header("x-organization-uuid", "org-1"))
        .and(header("anthropic-beta", "environments-2025-11-01"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    transport
        .client()
        .get(server.uri())
        .send()
        .await
        .expect("authenticated client should send default headers");
    server.verify().await;

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let request = reqwest::Client::new()
        .get("http://127.0.0.1:9/must-not-send")
        .build()
        .unwrap();
    let error = transport
        .round_trip(&cancellation, request)
        .await
        .expect_err("pre-cancelled request must not be sent");
    assert!(matches!(error, ByocError::Cancelled));
}

/// lease 默认接入真实 podmonitor，测试覆盖后仍传递认证 transport 和两个 30 秒参数。
#[tokio::test]
async fn lease_manager_delegates_without_copying_podmonitor_state_machine() {
    let mut environment = minimal_environment();
    let cancellation = CancellationToken::new();
    assert!(environment.lease_manager_factory.is_some());

    let factory = Arc::new(CapturingLeaseFactory::default());
    environment.set_lease_manager_factory(factory.clone());
    let handle = environment
        .create_lease_manager(
            &cancellation,
            "session-1",
            "env-1",
            "https://api.example.test",
            "ingress-token",
            "org-1",
        )
        .expect("injected lease factory should create a handle");
    handle.start().await.unwrap();
    handle.stop().await;

    let request = factory
        .request
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
        .expect("factory should capture request");
    assert_eq!(request.session_id, "session-1");
    assert_eq!(request.environment_id, "env-1");
    assert_eq!(request.organization_uuid, "org-1");
    assert_eq!(request.heartbeat_interval, Duration::from_secs(30));
    assert_eq!(request.safety_window, Duration::from_secs(30));
    let mut authenticated = reqwest::Client::new()
        .get("https://example.test")
        .build()
        .unwrap();
    request
        .transport
        .authorize_request(&mut authenticated)
        .unwrap();
    assert_eq!(
        authenticated.headers()["authorization"],
        "Bearer ingress-token"
    );
}

/// 动态 `EnvironmentType` 必须暴露 BYOC provider，且句柄 token 是父 token 的 direct child。
#[test]
fn dynamic_environment_exposes_real_lease_cancellation_token() {
    let parent_cancellation = CancellationToken::new();
    let environment: Box<dyn EnvironmentType> = Box::new(minimal_environment());
    let provider = environment
        .lease_manager_provider()
        .expect("BYOC trait object should expose its lease provider");
    let handle = provider
        .create_lease_manager(
            &parent_cancellation,
            "session-1",
            "env-1",
            "https://api.example.test",
            "ingress-token",
            "org-1",
        )
        .expect("default provider should create the real podmonitor handle");
    let lease_cancellation = handle.cancellation_token();
    assert!(!lease_cancellation.is_cancelled());

    parent_cancellation.cancel();

    assert!(lease_cancellation.is_cancelled());
}

/// 默认 factory 创建的真实 podmonitor 必须在 heartbeat 请求上保留全部 BYOC 认证头。
#[tokio::test]
async fn test_create_lease_manager_sends_authenticated_heartbeat() {
    use wiremock::matchers::{header, header_exists, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/environments/env-1/work/session-1/heartbeat"))
        .and(header("authorization", "Bearer ingress-token"))
        .and(header("x-organization-uuid", "org-1"))
        .and(header("anthropic-beta", "environments-2025-11-01"))
        .and(header_exists("x-environment-runner-version"))
        .and(query_param("organization_uuid", "org-1"))
        // 不延长租约可以让测试避开默认健康文件写入，同时 Start 仍按 成功。
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "lease_extended": false,
            "state": "running"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let environment = minimal_environment();
    let handle = environment
        .create_lease_manager(
            &CancellationToken::new(),
            "session-1",
            "env-1",
            &server.uri(),
            "ingress-token",
            "org-1",
        )
        .expect("default factory should create the real podmonitor");
    handle
        .start()
        .await
        .expect("authenticated initial heartbeat should succeed");
    handle.stop().await;
    server.verify().await;
}

///
/// 保存认证完成后的请求头。通过自定义 HTTP transport 截获请求；
/// Rust 的 reqwest client 不开放 transport 替换，因此这里在发包前的同一边界捕获。
#[derive(Default)]
struct CaptureRoundTripper {
    headers: Mutex<Option<reqwest::header::HeaderMap>>,
}

impl CaptureRoundTripper {
    ///
    /// 克隆请求头供认证断言使用，不发送网络请求，也不修改原始 request。
    fn round_trip(&self, request: &reqwest::Request) {
        *self.headers.lock().unwrap() = Some(request.headers().clone());
    }
}

///
/// 把测试 fixture 序列化为 JSON 字节；不可序列化说明 fixture 本身错误，因此直接 panic。
fn must_json(value: Value) -> Vec<u8> {
    serde_json::to_vec(&value).expect("test JSON should serialize")
}

/// 创建一份不触碰文件系统的最小 BYOC 环境。
fn minimal_environment() -> ByocEnvironmentType {
    new(br#"{"environment_type":"byoc"}"#).expect("minimal BYOC config is valid")
}

/// 为初始化测试创建指定 CWD 和 setup 脚本的环境。
fn environment_for_cwd(cwd: &Path, script: &str) -> ByocEnvironmentType {
    let raw = serde_json::to_vec(&serde_json::json!({
        "environment_type": "byoc",
        "cwd": cwd,
        "task_setup_script": script
    }))
    .unwrap();
    new(&raw).expect("test CWD should be an absolute clean path")
}

/// 构造 Git outcome。
fn git_outcome(repo: &str, branches: &[&str]) -> OutcomeField {
    OutcomeField {
        outcome_type: "git_repository".to_owned(),
        git_info: GitOutcomeInfo {
            repo: repo.to_owned(),
            branches: branches.iter().map(|branch| (*branch).to_owned()).collect(),
            ..GitOutcomeInfo::default()
        },
    }
}

/// 构造含一个 source 和一个目标 branch 的 startup context。
fn startup_with_source_and_outcome() -> StartupContext {
    StartupContext {
        api_base_url: "https://api.example.test".to_owned(),
        sources: vec![Source::GitRepository(GitRepositorySource {
            base: BaseSource {
                source_type: "git_repository".to_owned(),
            },
            ..GitRepositorySource::default()
        })],
        outcomes: vec![git_outcome("anthropic/demo", &["task-branch"])],
        ..StartupContext::default()
    }
}

/// 使 shell 路径作为单个字面量参数。
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\"'\"'"))
}

/// 串行测试期间替换一个环境变量，并在 drop 时还原。
struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
}

impl EnvVarGuard {
    /// 保存原值并设置新值。
    fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
        let original = std::env::var_os(key);
        // SAFETY: 本模块所有修改进程环境的 async 测试都持有
        // PROCESS_STATE_TEST_LOCK，设置与恢复期间不会并发读写这些键。
        unsafe { std::env::set_var(key, value) };
        Self { key, original }
    }
}

impl Drop for EnvVarGuard {
    /// 恢复创建 guard 前的进程环境。
    fn drop(&mut self) {
        // SAFETY: 与 set 相同，guard 在 PROCESS_STATE_TEST_LOCK 保护的作用域内 drop。
        unsafe {
            if let Some(original) = &self.original {
                std::env::set_var(self.key, original);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}

/// 保存并恢复进程全局 CWD。
struct CurrentDirectoryGuard(PathBuf);

impl CurrentDirectoryGuard {
    /// 捕获当前目录。
    fn capture() -> Self {
        Self(std::env::current_dir().expect("current directory should be readable"))
    }
}

impl Drop for CurrentDirectoryGuard {
    /// 在临时目录被删除前切回原 CWD。
    fn drop(&mut self) {
        std::env::set_current_dir(&self.0).expect("original CWD should remain available");
    }
}

/// 捕获 source 请求的测试 adapter。
#[derive(Default)]
struct CapturingSourceProcessor {
    requests: Mutex<Vec<SourceProcessRequest>>,
}

/// 同时捕获 BYOC activity 和 sources failure result 的测试 recorder。
#[derive(Default)]
struct CapturingActivityRecorder {
    messages: Mutex<Vec<String>>,
    failure_count: AtomicUsize,
}

#[async_trait]
impl ActivityRecorder for CapturingActivityRecorder {
    /// 保存普通 activity 文本。
    fn record_activity(
        &self,
        _level: LogLevel,
        _category: LogCategory,
        message: String,
        _extra: std::collections::BTreeMap<String, Value>,
    ) {
        self.messages
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(message);
    }

    /// 本测试不产生真实后台 activity。
    fn record_long_running_activity(
        &self,
        _category: LogCategory,
        _message: String,
    ) -> Arc<dyn Stopper> {
        Arc::new(NoopStopper)
    }

    /// 记录 sources manager 是否抵达 failure-result 边界。
    async fn record_failure_result(
        &self,
        _session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        self.failure_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SourceProcessor for CapturingSourceProcessor {
    /// 保存 branch checkout 请求。
    async fn process_sources(
        &self,
        _cancellation: &CancellationToken,
        request: &SourceProcessRequest,
        _activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), SourceProcessorError> {
        self.requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(request.clone());
        Ok(())
    }

    /// 本测试不记录独立 proxy 阶段。
    async fn setup_git_proxy_after_sources_processed(
        &self,
        _cancellation: &CancellationToken,
        _request: &SourceProcessRequest,
        _activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), SourceProcessorError> {
        Ok(())
    }
}

/// 不启动实际 heartbeat 的测试 lease 句柄。
struct FakeLeaseManager {
    cancellation: CancellationToken,
}

#[async_trait]
impl LeaseManagerHandle for FakeLeaseManager {
    /// 测试 start 立即成功。
    async fn start(&self) -> Result<(), BoxError> {
        Ok(())
    }

    /// 测试 stop 没有后台任务需要等待。
    async fn stop(&self) {}

    /// 返回 fake 自己持有的 token，保持与生产句柄相同的边界契约。
    fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

/// 捕获 BYOC 传给 podmonitor 的 factory request。
#[derive(Default)]
struct CapturingLeaseFactory {
    request: Mutex<Option<LeaseManagerRequest>>,
}

impl LeaseManagerFactory for CapturingLeaseFactory {
    /// 保存请求并返回最小句柄。
    fn create(
        &self,
        request: LeaseManagerRequest,
    ) -> Result<Arc<dyn LeaseManagerHandle>, BoxError> {
        let cancellation = request.cancellation.child_token();
        *self
            .request
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(request);
        Ok(Arc::new(FakeLeaseManager { cancellation }))
    }
}
