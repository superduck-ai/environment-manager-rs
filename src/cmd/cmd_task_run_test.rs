//! `task-run` 命令解析与运行时兼容测试。
//!
//! 对应 Go `cmd/cmd_task_run_test.go`；CCR 使用 loopback 真 HTTP，manager
//! 仅在信号包装边界使用 fake，不伪造生产 backend 成功路径。

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{GetEnvironmentConfigResponse, NoopBackend, SessionBackend},
    auth::AuthContext,
    claude::{ClaudeError, Outcomes},
    config::{self, StartupContext},
    logger::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger},
    manager::{ManagerApiClient, ManagerError},
};

use super::task_run::{
    TaskRunManager, TaskRunObservability, acknowledge_work_if_needed,
    apply_ccr_backend_claude_environment, build_task_run_backend, init_diag_logging,
    load_context_from_bytes, run_task_run_manager, task_run_claude_path,
    v0_missing_startup_context_panic_error, validate_api_base_url,
    wrap_claude_code_availability_error,
};
use super::zz_recovered_types::{StdinConfigClient, TaskContext};

use super::*;

/// 测试中丢弃全部日志，避免污染 CLI golden 的 stdout/stderr。
#[derive(Clone, Default)]
struct DiscardHandler;

impl LogHandler for DiscardHandler {
    /// 允许 logger 构造所有级别的记录。
    fn enabled(&self, _level: LogLevel) -> bool {
        true
    }

    /// 丢弃记录并报告成功。
    fn handle(&self, _record: &LogRecord) -> Result<(), LogHandlerError> {
        Ok(())
    }

    /// 派生 handler 仍丢弃附加属性。
    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }

    /// 派生 group handler 仍丢弃全部记录。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 构造无输出的 task-run 测试 logger。
fn test_logger() -> Logger {
    Logger::new(Arc::new(DiscardHandler))
}

/// task-run 缺少 session 时必须是 Cobra 风格的一号退出错误。
#[test]
fn task_run_requires_session() {
    let error = parse_from("dev", ["environment-runner", "task-run"])
        .expect_err("session must be required");
    assert_eq!(error.kind(), CliErrorKind::Usage);
    assert_eq!(error.exit_code(), 1);
    assert_eq!(error.stream(), OutputStream::Stderr);
    assert!(error.rendered().starts_with("Error:"));
    assert!(error.rendered().contains("--session"));
}

/// task-run 默认版本推导和“是否显式设置”状态必须与 Go RunE 一致。
#[test]
fn task_run_defaults_preserve_explicitness() {
    let Invocation::TaskRun(args) = parse_from(
        "dev",
        ["environment-runner", "task-run", "--session", "session-1"],
    )
    .expect("task-run defaults must parse") else {
        panic!("unexpected invocation");
    };

    assert_eq!(args.session_id, "session-1");
    assert_eq!(args.input_format, InputFormat::V0);
    assert_eq!(args.session_mode, SessionMode::New);
    assert!(!args.session_mode_was_set);
    assert!(!args.stdin_was_set);
    assert!(args.upgrade_claude_code);
    assert!(!args.upgrade_claude_code_was_set);
    assert_eq!(args.claude_agent_version, "latest");
}

/// 显式关闭旧升级 flag 时应把目标版本推导为 current。
#[test]
fn task_run_legacy_upgrade_false_maps_to_current() {
    let Invocation::TaskRun(args) = parse_from(
        "dev",
        [
            "environment-runner",
            "task-run",
            "--session",
            "session-1",
            "--upgrade-claude-code=false",
        ],
    )
    .expect("legacy upgrade false must parse") else {
        panic!("unexpected invocation");
    };

    assert!(!args.upgrade_claude_code);
    assert!(args.upgrade_claude_code_was_set);
    assert_eq!(args.claude_agent_version, "current");
}

/// 新旧 Claude 版本 flag 同时显式设置时应保留旧错误文本。
#[test]
fn task_run_rejects_conflicting_version_flags() {
    let error = parse_from(
        "dev",
        [
            "environment-runner",
            "task-run",
            "--session",
            "session-1",
            "--upgrade-claude-code",
            "--claude-agent-version",
            "stable",
        ],
    )
    .expect_err("version flags must conflict");
    assert_eq!(error.kind(), CliErrorKind::Validation);
    assert!(error.rendered().contains("mutually exclusive"));
    assert!(error.rendered().starts_with("Error:"));
}

/// local-testing 应在解析结果中应用旧 RunE 的 verbose/git 派生行为。
#[test]
fn task_run_local_testing_applies_effective_flags() {
    let Invocation::TaskRun(args) = parse_from(
        "dev",
        [
            "environment-runner",
            "task-run",
            "--session",
            "session-1",
            "--local-testing",
            "--stdin",
            "--session-mode",
            "resume-cached",
        ],
    )
    .expect("local testing options must parse") else {
        panic!("unexpected invocation");
    };

    assert!(args.local_testing);
    assert!(args.verbose_claude_logs);
    assert!(args.skip_git_config);
    assert!(args.stdin);
    assert!(args.stdin_was_set);
    assert_eq!(args.session_mode, SessionMode::ResumeCached);
    assert!(args.session_mode_was_set);
}

/// 构造一个保留 Go 零值语义的 task context。
fn empty_task_context() -> TaskContext {
    TaskContext {
        startup_context: None,
        environment: None,
        auth_context: None,
        outcomes: None,
        session_id: String::new(),
        work_id: String::new(),
        environment_id: String::new(),
        is_healthcheck: false,
    }
}

/// 构造 task-run 测试所需的最小 session ingress 认证上下文。
fn new_task_run_auth_context(session_id: &str, token: &str) -> AuthContext {
    let mut context = AuthContext::default();
    context.set_session_id(session_id);
    context.set_session_ingress_token(token);
    context
}

/// 对应 Go `TestValidateAPIBaseURL`，区分空值、无 host 和真正 URL 语法错误。
#[test]
fn test_validate_api_base_url() {
    for valid in ["https://api.example.com", "http://localhost:8080"] {
        assert_eq!(validate_api_base_url(valid), Ok(()), "valid={valid:?}");
    }
    for (invalid, expected) in [
        ("", "empty API base URL"),
        ("not a url", "API base URL has no host"),
        ("https://%", "invalid API base URL"),
    ] {
        let error = validate_api_base_url(invalid).expect_err("URL must be rejected");
        assert!(error.contains(expected), "error={error:?}");
    }
}

/// 对应 Go `TestStdinConfigClient`，验证配置、outcomes 和缺失 auth 的返回语义。
#[tokio::test]
async fn test_stdin_config_client() {
    let environment = GetEnvironmentConfigResponse {
        environment_type: "anthropic".to_owned(),
        config: json!(null),
        allowed_tools: "Bash,Edit".to_owned(),
        disallowed_tools: String::new(),
    };
    let outcomes = Outcomes::new();
    let client = StdinConfigClient {
        config: Some(environment.clone()),
        auth_context: None,
        outcomes: Some(outcomes.clone()),
        logger: test_logger(),
    };

    assert_eq!(
        client
            .get_environment_for_session(&CancellationToken::new(), "session_123")
            .await
            .expect("stdin config must be returned"),
        environment
    );
    assert_eq!(client.get_outcomes(), Some(outcomes));
    assert!(client.get_auth_context().is_none());
}

/// 对应 Go `TestAcknowledgeWorkIfNeededSkipAndValidation`，跳过分支不得误触发网络。
#[tokio::test]
async fn test_acknowledge_work_if_needed_skip_and_validation() {
    let logger = test_logger();
    let cancellation = CancellationToken::new();
    for (work_id, environment_id, skip_ack) in [
        ("", "env", false),
        ("work", "", false),
        ("work", "env", true),
    ] {
        acknowledge_work_if_needed(
            &cancellation,
            &logger,
            &empty_task_context(),
            work_id,
            environment_id,
            skip_ack,
        )
        .await
        .expect("skip branch must succeed");
    }

    let error = acknowledge_work_if_needed(
        &cancellation,
        &logger,
        &empty_task_context(),
        "work",
        "env",
        false,
    )
    .await
    .expect_err("missing auth must fail before network");
    assert!(error.contains("missing auth context"));
}

/// 对应 Go `TestBuildTaskRunBackendNoopModes`，跳过 Claude 和 setup-only 都使用空端点。
#[tokio::test]
async fn test_build_task_run_backend_noop_modes() {
    let logger = test_logger();
    for (mode, skip_claude_code) in [
        (config::SessionMode::New, true),
        (config::SessionMode::SetupOnly, false),
    ] {
        let selected = build_task_run_backend(
            &logger,
            &empty_task_context(),
            "session-1",
            mode,
            skip_claude_code,
        )
        .await
        .expect("noop backend selection must succeed");
        assert_eq!(selected.backend.otlp_endpoints(), Default::default());
        assert!(selected.ccr_backend.is_none());
    }
}

/// 对应 Go `TestBuildTaskRunBackendSessionIngress`，验证普通 backend 的端点与 header。
#[tokio::test]
async fn test_build_task_run_backend_session_ingress() {
    let mut task_context = empty_task_context();
    task_context.startup_context = Some(StartupContext {
        api_base_url: "https://api.example.test".to_owned(),
        ..StartupContext::default()
    });
    task_context.auth_context = Some(new_task_run_auth_context("session-1", "ingress-token"));

    let selected = build_task_run_backend(
        &test_logger(),
        &task_context,
        "session-1",
        config::SessionMode::New,
        false,
    )
    .await
    .expect("session ingress backend must be created");
    assert!(selected.ccr_backend.is_none());
    let endpoints = selected.backend.otlp_endpoints();
    assert_eq!(
        endpoints.metrics_endpoint,
        "https://api.example.test/anthropic.session_ingress.api.v1alpha.SessionService/ExportMetrics"
    );
    assert_eq!(
        endpoints.headers.get("Authorization").map(String::as_str),
        Some("Bearer ingress-token")
    );
    assert_eq!(
        endpoints.headers.get("x-session-id").map(String::as_str),
        Some("session-1")
    );
}

/// 启动仅处理一个 CCR register 请求的 loopback HTTP fixture。
///
/// 返回的 join handle 保留完整 HTTP 字节，使测试能同时核对 method、
/// path、headers 和 JSON body；fixture 故意不使用宽松 mock 匹配器。
async fn start_ccr_register_fixture() -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener");
    let address = listener.local_addr().expect("loopback address");
    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("CCR connection");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let mut expected_length = None;
        loop {
            let read = stream.read(&mut buffer).await.expect("read CCR request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if expected_length.is_none()
                && let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n")
            {
                let headers = String::from_utf8_lossy(&request[..header_end + 4]);
                let content_length = headers.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                });
                expected_length = Some(header_end + 4 + content_length.unwrap_or(0));
            }
            if expected_length.is_some_and(|length| request.len() >= length) {
                break;
            }
        }

        let response_body = br#"{"worker_epoch":"42"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            response_body.len()
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write CCR headers");
        stream
            .write_all(response_body)
            .await
            .expect("write CCR body");
        String::from_utf8(request).expect("reqwest request is ASCII/JSON")
    });
    (format!("http://{address}"), handle)
}

/// 对应 Go `TestBuildTaskRunBackendCCRRegistersWorker`，真实校验 register wire 和 epoch 传递。
#[tokio::test]
async fn test_build_task_run_backend_ccr_registers_worker() {
    let (base_url, request_handle) = start_ccr_register_fixture().await;
    let mut task_context = empty_task_context();
    task_context.startup_context = Some(StartupContext {
        api_base_url: base_url,
        use_code_sessions: true,
        ..StartupContext::default()
    });
    task_context.auth_context = Some(new_task_run_auth_context("session-ccr", "ingress-token"));

    let selected = build_task_run_backend(
        &test_logger(),
        &task_context,
        "session-ccr",
        config::SessionMode::New,
        false,
    )
    .await
    .expect("CCR backend must register");
    let ccr = selected.ccr_backend.expect("CCR handle must be retained");
    assert_eq!(ccr.worker_epoch(), 42);
    let request = request_handle.await.expect("CCR fixture task");
    let (headers, body) = request.split_once("\r\n\r\n").expect("HTTP request shape");
    let lowercase_headers = headers.to_ascii_lowercase();
    assert!(headers.starts_with("POST /v1/code/sessions/session-ccr/worker/register HTTP/1.1"));
    assert!(lowercase_headers.contains("authorization: bearer ingress-token"));
    assert!(lowercase_headers.contains("anthropic-version: 2023-06-01"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(body).expect("register JSON"),
        json!({"session_id": "session-ccr"})
    );

    let mut configuration = config::default_claude_config();
    apply_ccr_backend_claude_environment(&mut configuration, Some(ccr.as_ref()));
    assert_eq!(
        configuration
            .environment
            .get("CLAUDE_CODE_USE_CCR_V2")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        configuration
            .environment
            .get("CLAUDE_CODE_WORKER_EPOCH")
            .map(String::as_str),
        Some("42")
    );
}

/// 对应 Go `TestTaskRunClaudePath`，验证 flag、环境变量和默认值的优先级。
#[tokio::test]
async fn test_task_run_claude_path() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_task_run_claude_path"
    )) {
        return;
    }
    let _guard = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let original = std::env::var_os("CLAUDE_DEFAULT_PATH");
    // SAFETY: 全仓库进程状态锁防止其他测试并发读写该环境变量。
    unsafe { std::env::set_var("CLAUDE_DEFAULT_PATH", "/env/claude") };
    let logger = test_logger();
    assert_eq!(task_run_claude_path(&logger, None), "/env/claude");
    assert_eq!(
        task_run_claude_path(&logger, Some(std::path::Path::new("/flag/claude"))),
        "/flag/claude"
    );
    // SAFETY: 仍持有同一全局测试锁。
    unsafe { std::env::set_var("CLAUDE_DEFAULT_PATH", "") };
    assert_eq!(task_run_claude_path(&logger, None), "claude");
    match original {
        Some(value) => {
            // SAFETY: 仍持有全局测试锁，正在恢复原始值。
            unsafe { std::env::set_var("CLAUDE_DEFAULT_PATH", value) };
        }
        None => {
            // SAFETY: 仍持有全局测试锁，正在恢复原始缺失状态。
            unsafe { std::env::remove_var("CLAUDE_DEFAULT_PATH") };
        }
    }
}

/// 验证 manager 启动前的 Claude 安装失败同样不会在 task-run 边界丢失原始字节。
#[test]
fn task_run_preserves_raw_claude_install_error() {
    let source = ClaudeError::message_bytes(b"npm failed: \xff\xfe\0".to_vec());
    let error = wrap_claude_code_availability_error(&source);
    let expected: &[u8] = b"failed to ensure Claude Code is available: npm failed: \xff\xfe\0";
    assert_eq!(error.as_bytes(), expected);
    let rendered = render_runtime_error_bytes(CobraCommand::TaskRun, error.as_bytes());
    let raw_error_occurrences = rendered
        .windows(expected.len())
        .filter(|window| *window == expected)
        .count();
    assert_eq!(raw_error_occurrences, 2);
}

/// 验证 V0 缺失 startup context 只走 Go runtime 直出分支。
///
/// 该测试从 task-run 的真实字节解析边界进入，确保 sentinel 不会被
/// `failed to parse/load` 包装，也不会被 Cobra renderer 添加 usage 或双 `Error:`。
#[tokio::test]
async fn task_run_v0_missing_startup_context_uses_direct_go_panic_output() {
    let error = load_context_from_bytes(
        &CancellationToken::new(),
        &test_logger(),
        "session-missing-startup",
        InputFormat::V0,
        std::path::Path::new("/workspace"),
        br#"{"environment":{"environment_type":"byoc"}}"#,
    )
    .await
    .expect_err("缺失 startup_context 必须进入 Go runtime panic 兼容分支");

    assert!(error.writes_directly_to_stderr());
    assert_eq!(error.exit_code(), 2);
    assert!(
        error.as_bytes().starts_with(
            b"panic: runtime error: invalid memory address or nil pointer dereference\n"
        )
    );
    let required_frames: &[&[u8]] = &[
        b"[signal SIGSEGV: segmentation violation code=0x1 addr=0xc0 pc=0x61a128]\n",
        b"goroutine 1 [running]:\n",
        b"internal/input.(*V0Parser).Parse(0x0, {0xa01f08, 0xfc8540}, {0x0, 0x2b, 0x200})\n",
        b"internal/input/v0_parser.go:58 +0x188\n",
        b"cmd.loadContextFromStdin({0xa01f08, 0xfc8540}, 0x0, {0x0, 0x1a}, {0x8a8721, 0x2}, {0x0, 0x48})\n",
        b"cmd/cmd_task_run.go:576 +0x2b4\n",
        b"cmd.AddTaskRunCommand.func1(0x0, {0x8a8c93?, 0x4?, 0x8a8c97?})\n",
        b"cmd/cmd_task_run.go:152 +0x378\n",
        b"github.com/spf13/cobra.(*Command).execute(0x0, {0x0, 0x3, 0x3})\n",
        b"cobra@v1.9.1/command.go:1015 +0x7d4\n",
        b"github.com/spf13/cobra.(*Command).ExecuteC(0x0)\n",
        b"cobra@v1.9.1/command.go:1148 +0x350\n",
        b"github.com/spf13/cobra.(*Command).Execute(...)\n",
        b"cobra@v1.9.1/command.go:1071\n",
        b"main.main()\n",
        b"./main.go:44 +0x2bc\n",
    ];
    let mut cursor = 0;
    for frame in required_frames {
        let relative = error.as_bytes()[cursor..]
            .windows(frame.len())
            .position(|window| window == *frame)
            .unwrap_or_else(|| panic!("缺少或乱序的 Go panic frame: {frame:?}"));
        cursor += relative + frame.len();
    }
    assert!(error.as_bytes().ends_with(b"\n"));
    assert!(!error.as_bytes().starts_with(b"failed to parse stdin:"));

    let (rendered, exit_code) = render_task_run_runtime_error(&error);
    assert_eq!(exit_code, 2);
    assert_eq!(rendered, error.as_bytes());
    assert!(
        !rendered
            .windows(b"Error: ".len())
            .any(|part| part == b"Error: ")
    );
    assert!(
        !rendered
            .windows(b"Usage:\n".len())
            .any(|part| part == b"Usage:\n")
    );
    assert!(
        !rendered
            .windows(b"thread 'main' panicked".len())
            .any(|part| { part == b"thread 'main' panicked" })
    );
}

/// 验证 V0 其他解析错误不会被误分类为 Go runtime panic。
///
/// 显式 `null` 在 Go 中会得到非 nil 的零值 context，随后返回普通领域校验错误；
/// 非法 JSON 形状更早失败。两者都必须继续进入 Cobra/exit 1 路径。
#[tokio::test]
async fn task_run_v0_only_missing_startup_context_uses_exit_two() {
    for payload in [
        br#"{"startup_context":null,"environment":{"environment_type":"byoc"}}"#.as_slice(),
        br#"{"startup_context":"not-an-object"}"#.as_slice(),
    ] {
        let error = load_context_from_bytes(
            &CancellationToken::new(),
            &test_logger(),
            "session-v0-ordinary-error",
            InputFormat::V0,
            std::path::Path::new("/workspace"),
            payload,
        )
        .await
        .expect_err("无效 V0 输入必须返回普通解析错误");
        assert!(!error.writes_directly_to_stderr());
        assert_eq!(error.exit_code(), 1);
        assert!(error.as_bytes().starts_with(b"failed to parse stdin: "));
    }
}

/// 验证普通 task-run RuntimeError 仍使用原 Cobra 三段输出和退出码 1。
#[test]
fn task_run_normal_runtime_error_keeps_cobra_rendering() {
    let error = RuntimeError::message("ordinary failure");
    let (rendered, exit_code) = render_task_run_runtime_error(&error);
    assert_eq!(exit_code, 1);
    assert_eq!(
        rendered,
        render_runtime_error_bytes(CobraCommand::TaskRun, b"ordinary failure")
    );
}

/// 验证外层上下文包装不会污染已经代表 Go 进程终止的 panic transcript。
#[test]
fn task_run_go_panic_bypasses_error_context_wrapping() {
    let error = v0_missing_startup_context_panic_error();
    let expected = error.as_bytes().to_vec();
    let wrapped = error.context(b"failed to load context from stdin: ");
    assert_eq!(wrapped.as_bytes(), expected);
    assert!(wrapped.writes_directly_to_stderr());
}

/// `runTaskRunManager` 测试双：立即返回指定结果，不触发真实信号。
struct FakeTaskRunManager {
    result: Option<Result<(), ManagerError>>,
}

#[async_trait]
impl TaskRunManager for FakeTaskRunManager {
    /// 消费一次预置结果，如重复运行则显式暴露测试错误。
    async fn run(&mut self, _cancellation: &CancellationToken) -> Result<(), ManagerError> {
        self.result.take().expect("fake manager ran twice")
    }
}

/// 对应 Go `TestRunTaskRunManagerWrapsFailure`，普通失败必须添加稳定前缀。
#[tokio::test]
async fn test_run_task_run_manager_wraps_failure() {
    // Tokio Unix signal driver 会安装进程级 handler；隔离后不会改变父 libtest 的状态。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_run_task_run_manager_wraps_failure"
    )) {
        return;
    }
    let mut manager = FakeTaskRunManager {
        result: Some(Err(ManagerError::message("boom"))),
    };
    let error = run_task_run_manager(
        &CancellationToken::new(),
        &test_logger(),
        &mut manager,
        config::SessionMode::New,
        Instant::now(),
        &TaskRunObservability::default(),
    )
    .await
    .expect_err("manager failure must propagate");
    assert_eq!(error.as_bytes(), b"environment manager failed: boom");
}

/// 对应 Go `TestRunTaskRunManagerTreatsContextCanceledAsSuccess`，取消属于正常关闭。
#[tokio::test]
async fn test_run_task_run_manager_treats_context_canceled_as_success() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_run_task_run_manager_treats_context_canceled_as_success"
    )) {
        return;
    }
    let mut manager = FakeTaskRunManager {
        result: Some(Err(ManagerError::message("context canceled"))),
    };
    run_task_run_manager(
        &CancellationToken::new(),
        &test_logger(),
        &mut manager,
        config::SessionMode::New,
        Instant::now(),
        &TaskRunObservability::default(),
    )
    .await
    .expect("context cancellation must be successful shutdown");
}

/// 验证 manager 原始错误经过 task-run `%w` 包装和 Cobra 三段 renderer 后仍逐字节一致。
///
/// `ff fe 00` 同时覆盖连续非法 UTF-8 与 NUL；日志/Display 应逐字节替换前两者，
/// 但最终 stderr 中的两份错误正文必须保留原值。
#[tokio::test]
async fn task_run_preserves_raw_manager_error_through_cobra_rendering() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::task_run_preserves_raw_manager_error_through_cobra_rendering"
    )) {
        return;
    }
    let mut manager = FakeTaskRunManager {
        result: Some(Err(ManagerError::message_bytes(
            b"execution failed: \xff\xfe\0".to_vec(),
        ))),
    };
    let error = run_task_run_manager(
        &CancellationToken::new(),
        &test_logger(),
        &mut manager,
        config::SessionMode::New,
        Instant::now(),
        &TaskRunObservability::default(),
    )
    .await
    .expect_err("raw manager failure must propagate");
    let expected: &[u8] = b"environment manager failed: execution failed: \xff\xfe\0";
    assert_eq!(error.as_bytes(), expected);
    assert_eq!(
        error.to_string(),
        "environment manager failed: execution failed: \u{fffd}\u{fffd}\0"
    );

    let rendered = render_runtime_error_bytes(CobraCommand::TaskRun, error.as_bytes());
    assert!(rendered.starts_with(b"Error: "));
    assert!(rendered.ends_with(b"\n"));
    assert_eq!(
        &rendered[rendered.len() - expected.len() - 1..rendered.len() - 1],
        expected
    );
    let raw_error_occurrences = rendered
        .windows(expected.len())
        .filter(|window| *window == expected)
        .count();
    assert_eq!(raw_error_occurrences, 2);
    assert!(
        rendered
            .windows(b"Usage:\n  environment-runner task-run".len())
            .any(|window| window == b"Usage:\n  environment-runner task-run")
    );
    assert_eq!(
        render_runtime_error_bytes(CobraCommand::TaskRun, b"ordinary UTF-8 error"),
        render_runtime_error(CobraCommand::TaskRun, "ordinary UTF-8 error").as_bytes()
    );
}

/// 对应 Go `TestInitDiagLoggingDisabled`，关闭开关时不创建文件或后台任务。
#[tokio::test]
async fn test_init_diag_logging_disabled() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_init_diag_logging_disabled"
    )) {
        return;
    }
    let _guard = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let original = std::env::var_os("ENV_MANAGER_ENABLE_DIAG_LOGS");
    // SAFETY: 全仓库进程状态锁防止并发测试观察中间值。
    unsafe { std::env::remove_var("ENV_MANAGER_ENABLE_DIAG_LOGS") };
    let backend: Arc<dyn SessionBackend> = Arc::new(NoopBackend::new());
    let result = init_diag_logging(&CancellationToken::new(), backend, &test_logger())
        .await
        .expect("disabled diagnostic logging must succeed");
    assert!(result.is_none());
    match original {
        Some(value) => {
            // SAFETY: 仍持有全局测试锁，正在恢复原始值。
            unsafe { std::env::set_var("ENV_MANAGER_ENABLE_DIAG_LOGS", value) };
        }
        None => {
            // SAFETY: 仍持有全局测试锁，缺失状态已经恢复。
            unsafe { std::env::remove_var("ENV_MANAGER_ENABLE_DIAG_LOGS") };
        }
    }
}
