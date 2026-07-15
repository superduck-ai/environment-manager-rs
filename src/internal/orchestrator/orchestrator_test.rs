//! `internal/orchestrator` 的行为兼容与状态机测试。

use std::{
    collections::VecDeque,
    io::Write as _,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use flate2::{Compression, write::GzEncoder};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::internal::logger::{
    LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger,
};

/// 验证普通 Hook 的空命令、成功及带输出非零退出。
#[tokio::test]
async fn test_hook_execute_noop_success_and_failure() {
    let logger = orchestrator_test_logger();
    Hook {
        name: "noop".to_owned(),
        ..Hook::default()
    }
    .execute(&CancellationToken::new(), &logger)
    .await
    .unwrap();

    let temp = TempDir::new().unwrap();
    let success = write_orchestrator_script(&temp, "success.sh", "#!/bin/sh\nprintf 'ok'\n");
    Hook {
        name: "success".to_owned(),
        command: success.to_string_lossy().into_owned(),
        ..Hook::default()
    }
    .execute(&CancellationToken::new(), &logger)
    .await
    .unwrap();

    let failure =
        write_orchestrator_script(&temp, "failure.sh", "#!/bin/sh\nprintf 'boom'\nexit 9\n");
    let error = Hook {
        name: "fail".to_owned(),
        command: failure.to_string_lossy().into_owned(),
        ..Hook::default()
    }
    .execute(&CancellationToken::new(), &logger)
    .await
    .unwrap_err();
    assert!(error.to_string().contains("hook fail failed"));
    assert!(error.to_string().contains("exit status 9"));
    assert!(error.to_string().contains("boom"));
}

/// 验证 execute Hook 注入 session 参数，同时不改变 stdin 原始字节。
#[tokio::test]
async fn test_hook_execute_with_stdin_injects_session_id() {
    // capture 脚本依赖 PATH 查找 cat；保护整个子进程生命周期，不能只保护启动瞬间。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let args_path = temp.path().join("args.txt");
    let stdin_path = temp.path().join("stdin.json");
    let script = write_orchestrator_script(
        &temp,
        "capture.sh",
        "#!/bin/sh\nargs_path=$1\nstdin_path=$2\nshift 2\nprintf '%s\\n' \"$@\" > \"$args_path\"\ncat > \"$stdin_path\"\n",
    );
    let stdin_data = br#"{"id":"session-1","payload":true}"#;
    Hook {
        name: "execute".to_owned(),
        command: script.to_string_lossy().into_owned(),
        args: vec![
            args_path.to_string_lossy().into_owned(),
            stdin_path.to_string_lossy().into_owned(),
            "--fixed".to_owned(),
        ],
        timeout: Duration::from_secs(1),
        ..Hook::default()
    }
    .execute_with_stdin(
        &CancellationToken::new(),
        &orchestrator_test_logger(),
        stdin_data,
    )
    .await
    .unwrap();

    let args = std::fs::read_to_string(args_path).unwrap();
    assert_eq!(
        args.lines().collect::<Vec<_>>(),
        ["--fixed", "--session", "session-1"]
    );
    assert_eq!(std::fs::read(stdin_path).unwrap(), stdin_data);
    assert_eq!(shell_quote("a'b"), "'a'\"'\"'b'");
}

/// 验证 sandbox 启用却未接入 runtime 时不会静默回退宿主机执行。
#[tokio::test]
async fn hook_requires_sandbox_adapter_when_enabled() {
    let error = Hook {
        name: "execute".to_owned(),
        command: "/bin/true".to_owned(),
        sandbox_enabled: true,
        ..Hook::default()
    }
    .execute_with_stdin(
        &CancellationToken::new(),
        &default_orchestrator_logger(),
        br#"{"id":"session-1"}"#,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("sandbox adapter is not configured")
    );
}

/// 验证 Hook deadline 会终止长时间运行的子进程。
#[tokio::test]
async fn hook_timeout_kills_child() {
    // Hook 按名称解析 sleep，必须与并行修改 PATH 的测试串行。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let started = Instant::now();
    let error = Hook {
        name: "timeout".to_owned(),
        command: "sleep".to_owned(),
        args: vec!["30".to_owned()],
        timeout: Duration::from_millis(20),
        ..Hook::default()
    }
    .execute(&CancellationToken::new(), &default_orchestrator_logger())
    .await
    .unwrap_err();
    assert!(error.to_string().contains("context deadline exceeded"));
    assert!(started.elapsed() < Duration::from_secs(2));
}

/// 验证 PollHook 的 stdin context、work、空队列与非法 JSON 分支。
#[tokio::test]
async fn test_poll_hook_poll_returns_work_empty_and_errors() {
    // work fixture 在 shell 中调用 cat，因此持锁直到所有 poll 分支和子进程结束。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let stdin_path = temp.path().join("poll-stdin.json");
    let work_script = write_orchestrator_script(
        &temp,
        "work.sh",
        "#!/bin/sh\ncat > \"$1\"\nprintf '%s\\n' '{\"id\":\"work-1\",\"type\":\"work\",\"environment_id\":\"env-1\",\"data\":{\"type\":\"session\",\"id\":\"session-1\"}}'\n",
    );
    let mut poll_hook = new_poll_hook(
        work_script.to_string_lossy(),
        Duration::from_secs(1),
        "org-1",
        "env-1",
        "worker-1",
        orchestrator_test_logger(),
    );
    poll_hook
        .args
        .push(stdin_path.to_string_lossy().into_owned());
    let raw = poll_hook
        .poll(&CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    let work: WorkResponse = serde_json::from_slice(&raw).unwrap();
    assert_eq!(work.id, "work-1");
    assert_eq!(work.data.id, "session-1");
    let context: PollHookContext =
        serde_json::from_slice(&std::fs::read(stdin_path).unwrap()).unwrap();
    assert_eq!(
        context,
        PollHookContext {
            organization_uuid: "org-1".to_owned(),
            environment_id: "env-1".to_owned(),
            worker_id: "worker-1".to_owned(),
        }
    );

    let empty_script = write_orchestrator_script(&temp, "empty.sh", "#!/bin/sh\nprintf 'null'\n");
    let empty_hook = new_poll_hook(
        empty_script.to_string_lossy(),
        Duration::from_secs(1),
        "",
        "",
        "",
        orchestrator_test_logger(),
    );
    assert!(
        empty_hook
            .poll(&CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );

    let invalid_script =
        write_orchestrator_script(&temp, "invalid.sh", "#!/bin/sh\nprintf 'not json'\n");
    let invalid_hook = new_poll_hook(
        invalid_script.to_string_lossy(),
        Duration::from_secs(1),
        "",
        "",
        "",
        orchestrator_test_logger(),
    );
    let error = invalid_hook
        .poll(&CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid JSON"));
}

/// 验证 PollHook jitter 能被预先取消的 token 立即打断。
#[tokio::test]
async fn test_poll_hook_sleep_with_jitter_cancellation() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let hook = new_poll_hook(
        "/bin/true",
        Duration::from_secs(1),
        "",
        "",
        "",
        orchestrator_test_logger(),
    );
    let error = hook.sleep_with_jitter(&cancellation).await.unwrap_err();
    assert!(error.to_string().contains("jitter sleep interrupted"));
}

/// 验证 HTTP Poller 的 URL、header、原始 work 响应和空队列。
#[tokio::test]
async fn test_poller_poll_requests_work_and_handles_empty_queue() {
    let (base_url, request) = spawn_http_response(
        200,
        &[("request-id", "req-1")],
        br#"{"id":"work-1","type":"work","environment_id":"env-1","state":"available","data":{"type":"session","id":"session-1"},"secret":"secret"}"#,
    )
    .await;
    let poller = new_poller_with_worker_id(
        base_url,
        "env-1",
        "auth-token",
        "worker-1",
        123,
        orchestrator_test_logger(),
    );
    let raw = poller
        .poll(&CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&raw).contains("work-1"));
    let request = request.await.unwrap();
    let lower = request.to_ascii_lowercase();
    assert!(request.starts_with("GET /v1/environments/env-1/work/poll?reclaim_older_than_ms=123 "));
    assert!(lower.contains("authorization: bearer auth-token"));
    assert!(lower.contains("anthropic-worker-id: worker-1"));
    assert!(lower.contains("anthropic-beta: environments-2025-11-01"));
    assert!(lower.contains("x-environment-runner-version:"));

    let (base_url, _request) = spawn_http_response(200, &[], b"null").await;
    let empty = new_poller_with_worker_id(
        base_url,
        "env-1",
        "auth-token",
        "worker-1",
        0,
        orchestrator_test_logger(),
    );
    assert!(
        empty
            .poll(&CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
}

/// 验证 HTTP Poller 的 request-id 状态错误和 envelope 类型校验。
#[tokio::test]
async fn test_poller_poll_reports_http_and_validation_errors() {
    let (base_url, _request) =
        spawn_http_response(418, &[("x-request-id", "req-err")], b"nope").await;
    let poller = new_poller_with_worker_id(
        base_url,
        "env-1",
        "auth-token",
        "worker-1",
        0,
        orchestrator_test_logger(),
    );
    let error = poller.poll(&CancellationToken::new()).await.unwrap_err();
    assert!(error.to_string().contains("status 418"));
    assert!(error.to_string().contains("req-err"));

    let (base_url, _request) = spawn_http_response(200, &[], br#"{"type":"other"}"#).await;
    let poller = new_poller_with_worker_id(
        base_url,
        "env-1",
        "auth-token",
        "worker-1",
        0,
        orchestrator_test_logger(),
    );
    let error = poller.poll(&CancellationToken::new()).await.unwrap_err();
    assert!(error.to_string().contains("unexpected response type"));
}

/// 验证 HTTP Poller jitter 的取消分支。
#[tokio::test]
async fn test_poller_sleep_with_jitter_cancellation() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let poller = new_poller_with_worker_id(
        "example.invalid",
        "env-1",
        "token",
        "worker-1",
        0,
        orchestrator_test_logger(),
    );
    let error = poller.sleep_with_jitter(&cancellation).await.unwrap_err();
    assert!(error.to_string().contains("jitter sleep interrupted"));
}

/// 验证 Whoami 请求和身份字段。
#[tokio::test]
async fn test_whoami_client_get_identity() {
    let compressed_body = gzip_orchestrator_bytes(
        br#"{"type":"environment_whoami","organization_uuid":"org-1","environment_id":"env-1"}"#,
    );
    let (base_url, request) =
        spawn_http_response(200, &[("Content-Encoding", "gzip")], &compressed_body).await;
    let client = new_whoami_client(base_url, "auth-token", orchestrator_test_logger());
    let identity = client
        .get_identity(&CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(identity.organization_uuid, "org-1");
    assert_eq!(identity.environment_id, "env-1");
    let request = request.await.unwrap();
    assert!(request.starts_with("GET /v1/environments/whoami "));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("authorization: bearer auth-token")
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("user-agent: go-http-client/1.1\r\n")
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("accept-encoding: gzip\r\n")
    );
}

/// 验证 Whoami 构造器保留 Go 的原始尾随斜杠，因此请求路径可见双斜杠。
#[tokio::test]
async fn whoami_preserves_trailing_base_url_slash() {
    let (base_url, request) = spawn_http_response(
        200,
        &[],
        br#"{"type":"environment_whoami","organization_uuid":"org-1","environment_id":"env-1"}"#,
    )
    .await;
    let client = new_whoami_client(
        format!("{base_url}/"),
        "auth-token",
        orchestrator_test_logger(),
    );
    client
        .get_identity(&CancellationToken::new())
        .await
        .expect("尾随斜杠只改变 wire path，不应影响身份解析");

    let request = request.await.unwrap();
    assert!(request.starts_with("GET //v1/environments/whoami "));
}

/// 验证 Whoami 的状态、JSON 与 response type 错误。
#[tokio::test]
async fn test_whoami_client_reports_status_parse_and_type_errors() {
    let (base_url, _request) = spawn_http_response(401, &[], b"nope").await;
    let client = new_whoami_client(base_url, "token", orchestrator_test_logger());
    let error = client
        .get_identity(&CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("status 401"));
    assert!(error.to_string().contains("nope"));

    let (base_url, _request) = spawn_http_response(200, &[], b"{bad json").await;
    let client = new_whoami_client(base_url, "token", orchestrator_test_logger());
    let error = client
        .get_identity(&CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("failed to unmarshal whoami response")
    );

    let (base_url, _request) = spawn_http_response(200, &[], br#"{"type":"other"}"#).await;
    let client = new_whoami_client(base_url, "token", orchestrator_test_logger());
    let error = client
        .get_identity(&CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unexpected whoami response type")
    );

    // Go 会把顶层 null 以及 string 字段的显式 null 都解码为结构体零值；两种输入
    // 都应进入 response type 校验，而不是提前暴露 serde 的类型错误。
    for body in [
        b"null".as_slice(),
        br#"{"type":null,"organization_uuid":null,"environment_id":null}"#.as_slice(),
    ] {
        let (base_url, _request) = spawn_http_response(200, &[], body).await;
        let client = new_whoami_client(base_url, "token", orchestrator_test_logger());
        let error = client
            .get_identity(&CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "unexpected whoami response type: ");
    }
}

/// 验证状态机把 work 原文写到输出，并按 start/end 顺序发 observer 事件。
#[tokio::test]
async fn orchestrator_runs_one_session_to_output() {
    let poller = Arc::new(SequencePoller::new([Ok(Some(
        br#"{"id":"session-1"}"#.to_vec(),
    ))]));
    let output = Arc::new(MemoryOutput::default());
    let observer = Arc::new(RecordingObserver::default());
    let orchestrator = new_orchestrator(OrchestratorConfig {
        poller: Some(poller),
        poll_timeout: Duration::from_secs(1),
        loop_timeout: Duration::from_secs(1),
        observer: Some(observer.clone()),
        session_output: Some(output.clone()),
        ..OrchestratorConfig::default()
    })
    .unwrap();
    orchestrator.run(&CancellationToken::new()).await.unwrap();
    assert_eq!(
        output.bytes.lock().unwrap().as_slice(),
        b"{\"id\":\"session-1\"}\n"
    );
    assert_eq!(
        observer.events.lock().unwrap().as_slice(),
        [
            OrchestratorEvent::PollAttempt,
            OrchestratorEvent::SessionStart,
            OrchestratorEvent::SessionEnd,
        ]
    );
}

/// 验证 max failures 是终止条件，错误保留最后一次 poll 原因。
#[tokio::test]
async fn orchestrator_stops_at_max_poll_failures() {
    let poller = Arc::new(SequencePoller::new([Err("backend unavailable".to_owned())]));
    let orchestrator = new_orchestrator(OrchestratorConfig {
        poller: Some(poller),
        poll_timeout: Duration::from_secs(1),
        loop_timeout: Duration::from_secs(1),
        max_poll_failures: 1,
        ..OrchestratorConfig::default()
    })
    .unwrap();
    let error = orchestrator
        .run(&CancellationToken::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("exceeded max poll failures (1)"));
    assert!(error.to_string().contains("backend unavailable"));
}

/// 验证 poll 失败后的退避可立即被取消，而不会等待完整两秒。
#[tokio::test]
async fn orchestrator_cancellation_interrupts_backoff() {
    let poller = Arc::new(CancellingErrorPoller);
    let orchestrator = new_orchestrator(OrchestratorConfig {
        poller: Some(poller),
        poll_timeout: Duration::from_secs(30),
        loop_timeout: Duration::from_secs(30),
        ..OrchestratorConfig::default()
    })
    .unwrap();
    let cancellation = CancellationToken::new();
    let started = Instant::now();
    let error = orchestrator.run(&cancellation).await.unwrap_err();
    assert!(error.to_string().contains("stopped during backoff"));
    assert!(started.elapsed() < Duration::from_secs(1));
}

/// 验证 orchestrator 自身的所有 Go `slog.Duration` 属性保留原字段名和纳秒单位。
#[tokio::test]
async fn orchestrator_duration_logs_match_go_slog_duration() {
    let (logger, records) = recording_orchestrator_logger();
    let output = Arc::new(MemoryOutput::default());
    let orchestrator = new_orchestrator(OrchestratorConfig {
        poller: Some(Arc::new(SequencePoller::new([Ok(Some(
            br#"{"id":"session-1"}"#.to_vec(),
        ))]))),
        poll_timeout: Duration::from_millis(1_500),
        loop_timeout: Duration::from_secs(2),
        session_output: Some(output),
        logger: Some(logger),
        ..OrchestratorConfig::default()
    })
    .unwrap();
    orchestrator.run(&CancellationToken::new()).await.unwrap();

    let records = records.lock().unwrap();
    let starting = records
        .iter()
        .find(|record| record.message == "starting orchestrator")
        .unwrap();
    assert_eq!(starting.attributes["poll_timeout"], 1_500_000_000_i64);
    assert_eq!(starting.attributes["loop_timeout"], 2_000_000_000_i64);
    assert!(!starting.attributes.contains_key("poll_timeout_ms"));
    assert!(!starting.attributes.contains_key("loop_timeout_ms"));

    let polling = records
        .iter()
        .find(|record| record.message == "starting poll loop")
        .unwrap();
    assert_eq!(polling.attributes["poll_timeout"], 1_500_000_000_i64);
    let remaining = polling.attributes["remaining_loop_time"].as_i64().unwrap();
    assert!((1..=2_000_000_000).contains(&remaining));
    assert!(!polling.attributes.contains_key("remaining_loop_time_ms"));
}

/// 验证失败耗时和退避时长不再以 Rust 专用 `_ms` 字段写入。
#[tokio::test]
async fn orchestrator_failure_duration_logs_match_go_slog_duration() {
    let (logger, records) = recording_orchestrator_logger();
    let orchestrator = new_orchestrator(OrchestratorConfig {
        poller: Some(Arc::new(CancellingErrorPoller)),
        poll_timeout: Duration::from_secs(30),
        loop_timeout: Duration::from_secs(30),
        logger: Some(logger),
        ..OrchestratorConfig::default()
    })
    .unwrap();
    orchestrator
        .run(&CancellationToken::new())
        .await
        .unwrap_err();

    let records = records.lock().unwrap();
    let failed = records
        .iter()
        .find(|record| record.message == "poll failed")
        .unwrap();
    assert!(failed.attributes["duration"].as_i64().is_some());
    assert!(!failed.attributes.contains_key("duration_ms"));

    let backoff = records
        .iter()
        .find(|record| record.message == "backing off after poll failure")
        .unwrap();
    assert_eq!(backoff.attributes["backoff"], 2_000_000_000_i64);
    assert!(!backoff.attributes.contains_key("backoff_ms"));
}

/// 验证命令 PollHook 与 HTTP Poller 的时长属性也使用 Go 纳秒整数。
#[tokio::test]
async fn poller_duration_logs_match_go_slog_duration() {
    let (hook_logger, hook_records) = recording_orchestrator_logger();
    let hook = new_poll_hook("/bin/true", Duration::from_secs(1), "", "", "", hook_logger);
    assert!(
        hook.poll(&CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    hook.sleep_with_jitter(&cancelled).await.unwrap_err();

    {
        let records = hook_records.lock().unwrap();
        let empty = records
            .iter()
            .find(|record| record.message == "poll hook returned empty queue")
            .unwrap();
        assert!(empty.attributes["duration"].as_i64().is_some());
        assert!(!empty.attributes.contains_key("duration_ms"));
        assert_go_jitter_duration(&records, "sleep_duration");
    }

    let (base_url, _request) = spawn_http_response(200, &[], b"null").await;
    let (poller_logger, poller_records) = recording_orchestrator_logger();
    let poller = new_poller_with_worker_id(base_url, "env", "token", "worker", 0, poller_logger);
    assert!(
        poller
            .poll(&CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    poller.sleep_with_jitter(&cancelled).await.unwrap_err();

    let records = poller_records.lock().unwrap();
    let empty = records
        .iter()
        .find(|record| record.message == "queue empty")
        .unwrap();
    assert!(empty.attributes["poll_duration"].as_i64().is_some());
    assert!(!empty.attributes.contains_key("poll_duration_ms"));
    assert_go_jitter_duration(&records, "sleep_duration");
}

/// 验证 loop timeout 事件触发后重置完整预算，timeout Hook 失败不会上抛。
#[tokio::test]
async fn orchestrator_timeout_hook_failure_is_non_fatal() {
    let temp = TempDir::new().unwrap();
    let script = write_orchestrator_script(&temp, "timeout-fail.sh", "#!/bin/sh\nexit 7\n");
    let observer = Arc::new(RecordingObserver::default());
    let orchestrator = new_orchestrator(OrchestratorConfig {
        poller: Some(Arc::new(SequencePoller::new([]))),
        timeout_hook: Some(Arc::new(Hook {
            name: "timeout".to_owned(),
            command: script.to_string_lossy().into_owned(),
            ..Hook::default()
        })),
        poll_timeout: Duration::from_secs(2),
        loop_timeout: Duration::from_millis(10),
        observer: Some(observer.clone()),
        ..OrchestratorConfig::default()
    })
    .unwrap();
    let (new_start, remaining) = orchestrator
        .handle_loop_timeout(
            &CancellationToken::new(),
            Instant::now() - Duration::from_millis(20),
        )
        .await;
    assert!(new_start.elapsed() < Duration::from_millis(20));
    assert_eq!(remaining, Duration::from_millis(10));
    assert_eq!(
        observer.events.lock().unwrap().as_slice(),
        [OrchestratorEvent::Timeout]
    );
}

/// 写入 0700 权限的 POSIX shell 脚本。
fn write_orchestrator_script(temp: &TempDir, name: &str, body: &str) -> PathBuf {
    let path = temp.path().join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}

/// 对应 Go：`shellQuote`，按 POSIX shell 单引号规则转义任意参数。
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

/// 对应 Go：`orchestratorTestLogger`，复用内存/丢弃兼容 logger。
fn orchestrator_test_logger() -> crate::internal::logger::Logger {
    default_orchestrator_logger()
}

/// 创建会保存所有结构化属性的 logger，供 duration schema 测试逐字段检查。
fn recording_orchestrator_logger() -> (Logger, Arc<Mutex<Vec<LogRecord>>>) {
    let records = Arc::new(Mutex::new(Vec::new()));
    let logger = Logger::new(Arc::new(RecordingLogHandler {
        records: Arc::clone(&records),
        attributes: LogAttributes::new(),
    }));
    (logger, records)
}

/// 断言 jitter 日志使用 Go 字段名，并且随机值仍是 1、2、3 秒之一的纳秒数。
fn assert_go_jitter_duration(records: &[LogRecord], field: &str) {
    let record = records
        .iter()
        .find(|record| record.message == "sleeping with jitter after empty queue")
        .unwrap();
    assert!(matches!(
        record.attributes[field].as_i64(),
        Some(1_000_000_000 | 2_000_000_000 | 3_000_000_000)
    ));
    assert!(!record.attributes.contains_key("sleep_duration_ms"));
}

/// 生成测试响应使用的 gzip member，覆盖 Whoami 的 Go Transport 自动解压路径。
fn gzip_orchestrator_bytes(body: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(body).unwrap();
    encoder.finish().unwrap()
}

/// 启动只处理一个请求的最小 HTTP/1.1 server，并返回捕获请求。
async fn spawn_http_response(
    status: u16,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (String, oneshot::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let headers = headers
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect::<Vec<_>>();
    let body = body.to_vec();
    let (request_tx, request_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let _ = request_tx.send(String::from_utf8_lossy(&request).into_owned());
        let reason = match status {
            200 => "OK",
            401 => "Unauthorized",
            418 => "I'm a teapot",
            _ => "Status",
        };
        let mut response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n",
            body.len()
        );
        for (name, value) in headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
        stream.shutdown().await.unwrap();
    });
    (format!("http://{address}"), request_rx)
}

/// 按预置顺序返回 poll 结果，jitter 不等待。
struct SequencePoller {
    responses: Mutex<VecDeque<Result<Option<RawMessage>, String>>>,
}

impl SequencePoller {
    /// 创建顺序 poller。
    fn new(responses: impl IntoIterator<Item = Result<Option<RawMessage>, String>>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().collect()),
        }
    }
}

#[async_trait]
impl PollerInterface for SequencePoller {
    /// 返回下一条预置结果；耗尽后表示空队列。
    async fn poll(
        &self,
        _cancellation: &CancellationToken,
    ) -> OrchestratorResult<Option<RawMessage>> {
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(None))
            .map_err(OrchestratorError::message)
    }

    /// 测试中跳过随机等待。
    async fn sleep_with_jitter(&self, _cancellation: &CancellationToken) -> OrchestratorResult<()> {
        Ok(())
    }
}

/// 首次 poll 先取消 token 再返回错误，用于命中退避取消竞态。
struct CancellingErrorPoller;

#[async_trait]
impl PollerInterface for CancellingErrorPoller {
    /// 取消并返回模拟后端错误。
    async fn poll(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<Option<RawMessage>> {
        cancellation.cancel();
        Err(OrchestratorError::message("backend unavailable"))
    }

    /// 该测试不应进入 jitter。
    async fn sleep_with_jitter(&self, _cancellation: &CancellationToken) -> OrchestratorResult<()> {
        Ok(())
    }
}

/// 把 session 原文和换行保存到内存。
#[derive(Default)]
struct MemoryOutput {
    bytes: Mutex<Vec<u8>>,
}

#[async_trait]
impl SessionOutput for MemoryOutput {
    /// 记录一条 session。
    async fn write_session(&self, session_json: &[u8]) -> OrchestratorResult<()> {
        let mut bytes = self.bytes.lock().unwrap();
        bytes.extend_from_slice(session_json);
        bytes.push(b'\n');
        Ok(())
    }
}

/// 记录 observer 事件的线程安全实现。
#[derive(Default)]
struct RecordingObserver {
    events: Mutex<Vec<OrchestratorEvent>>,
}

/// 保存日志及 `Logger::with_attrs` 固定属性的测试 handler。
#[derive(Clone)]
struct RecordingLogHandler {
    records: Arc<Mutex<Vec<LogRecord>>>,
    attributes: LogAttributes,
}

impl LogHandler for RecordingLogHandler {
    /// duration 测试需要观察 debug 到 error 的全部级别。
    fn enabled(&self, _level: LogLevel) -> bool {
        true
    }

    /// 合并组件固定属性后保存记录，模拟生产 handler 最终看到的字段集合。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        let mut record = record.clone();
        record.attributes.extend(self.attributes.clone());
        self.records.lock().unwrap().push(record);
        Ok(())
    }

    /// 创建共享记录槽且追加固定属性的派生 handler。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut combined = self.attributes.clone();
        combined.extend(attributes);
        Arc::new(Self {
            records: Arc::clone(&self.records),
            attributes: combined,
        })
    }

    /// 测试不改变 group 编码，只保留共享记录槽和固定属性。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

impl OrchestratorObserver for RecordingObserver {
    /// 按发生顺序保存事件。
    fn record_event(&self, event: OrchestratorEvent) {
        self.events.lock().unwrap().push(event);
    }
}
