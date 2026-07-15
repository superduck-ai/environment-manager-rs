//! Claude 执行器的参数、环境、MCP、token FD 与 direct-child 取消测试。

use std::{
    fs,
    io::Read,
    os::{
        fd::{AsFd as _, AsRawFd as _},
        unix::{fs::PermissionsExt as _, process::CommandExt as _},
    },
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use nix::fcntl::{FcntlArg, fcntl};
use tempfile::TempDir;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    config::{ClaudeConfig, McpConfigFile, StartupContext},
    logger::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger},
    session::NoopActivityRecorder,
};

use super::{
    Executor, build_session_urls,
    claude_code_executor::{
        add_token_via_file_descriptor, filter_init_only_from_environ, install_token_descriptors,
        new_claude_code_executor, tail_string, write_ccr_upstream_proxy_token,
    },
};

/// 线程安全收集 handler 收到的结构化记录。
#[derive(Clone, Default)]
struct RecordingSlogHandler {
    records: Arc<Mutex<Vec<LogRecord>>>,
    attributes: LogAttributes,
}

impl LogHandler for RecordingSlogHandler {
    /// 测试 handler 接收全部级别。
    fn enabled(&self, _level: LogLevel) -> bool {
        true
    }

    /// 克隆记录并合并派生属性。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        let mut record = record.clone();
        record.attributes.extend(self.attributes.clone());
        self.records.lock().unwrap().push(record);
        Ok(())
    }

    /// 创建共享记录缓冲且附加固定属性的派生 handler。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut combined = self.attributes.clone();
        combined.extend(attributes);
        Arc::new(Self {
            records: Arc::clone(&self.records),
            attributes: combined,
        })
    }

    /// 测试不依赖 group 编码，仍返回共享记录缓冲的克隆。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 创建全部 Go 零值字段的 Claude config。
fn empty_config() -> ClaudeConfig {
    ClaudeConfig {
        command: String::new(),
        args: Vec::new(),
        environment: Default::default(),
        working_dir: String::new(),
        timeout: Duration::ZERO,
        allowed_tools: String::new(),
        local_testing_mode: false,
        disallowed_tools: String::new(),
        verbose_logging: false,
        local_append_system_prompt: String::new(),
        claude_path: String::new(),
        print_code_logs: false,
        skip_claude_code: false,
    }
}

/// 写入 mode 0700 的 shell 脚本。
fn script(temp: &TempDir, name: &str, body: &str) -> PathBuf {
    let path = temp.path().join(name);
    fs::write(&path, body).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}

/// 构造测试执行器并返回独立的 OTEL 记录缓冲。
fn make_executor(
    cfg: ClaudeConfig,
    startup: StartupContext,
) -> (super::ClaudeCodeExecutor, RecordingSlogHandler) {
    let app_handler = Arc::new(RecordingSlogHandler::default());
    let otel_handler = RecordingSlogHandler::default();
    let executor = new_claude_code_executor(
        Logger::new(app_handler),
        Some(cfg),
        "session-123",
        startup,
        None,
        Vec::new(),
        None,
        Arc::new(NoopActivityRecorder),
        Arc::new(otel_handler.clone()),
        None,
        "latest",
    )
    .unwrap();
    (executor, otel_handler)
}

/// 对应 Go：`TestWriteMCPConfigFileFromGateway`。
///
/// 验证 gateway MCP Base64 内容、默认 mode 与显式 path。
#[tokio::test]
async fn test_write_mcp_config_file_from_gateway() {
    use base64::Engine as _;

    let temp = TempDir::new().unwrap();
    let path = temp.path().join("mcp.json");
    let content = br#"{"mcpServers":{}}"#;
    let startup = StartupContext {
        mcp_config_file: Some(McpConfigFile {
            path: path.to_string_lossy().into_owned(),
            content: base64::engine::general_purpose::STANDARD.encode(content),
            mode: 0,
        }),
        ..StartupContext::default()
    };
    let (executor, _) = make_executor(empty_config(), startup);
    assert_eq!(
        executor.write_mcp_config_file_from_gateway().await.unwrap(),
        path.to_string_lossy()
    );
    assert_eq!(fs::read(&path).unwrap(), content);
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

/// 对应 Go：`TestAddTokenViaFileDescriptor`。
///
/// 验证首个 token 获得 FD 3，pipe 内容带唯一换行。
#[test]
fn test_add_token_via_file_descriptor() {
    let logger = discard_logger();
    let mut descriptors = Vec::new();
    let fd = add_token_via_file_descriptor(&logger, "secret-token", "test token", &mut descriptors)
        .unwrap();
    assert_eq!(fd, 3);
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].target, 3);
    let cloned = descriptors[0].source.as_fd().try_clone_to_owned().unwrap();
    let mut reader = fs::File::from(cloned);
    let mut content = String::new();
    reader.read_to_string(&mut content).unwrap();
    assert_eq!(content, "secret-token\n");
}

/// 验证 stderr tail 在截断前后使用 Go 的两套无效 UTF-8 规则。
#[test]
fn stderr_tail_matches_go_invalid_utf8_rules() {
    assert_eq!(tail_string(b"\xff\xfe", 8), "\u{fffd}\u{fffd}");
    assert_eq!(tail_string(b"a\xff\xfeb", 3), "b");
    assert_eq!(tail_string("有效\u{fffd}".as_bytes(), 64), "有效\u{fffd}");
}

/// 验证三个 token 在真实 exec 后固定继承为 FD 3/4/5，且高位中转 FD 被关闭。
///
/// 第一段 `pre_exec` 先用 stdin 占据 FD 3/4，模拟 Go `ExtraFiles` 安装时目标槽位
/// 已被 runtime 内部 FD 使用的情况；生产映射必须随后用 `dup2` 正确覆盖它们。
#[tokio::test]
async fn token_descriptors_survive_exec_without_leaking_source_fds() {
    let logger = discard_logger();
    let mut descriptors = Vec::new();
    assert_eq!(
        add_token_via_file_descriptor(&logger, "first-token", "first", &mut descriptors).unwrap(),
        3
    );
    assert_eq!(
        add_token_via_file_descriptor(&logger, "second-token", "second", &mut descriptors).unwrap(),
        4
    );
    assert_eq!(
        add_token_via_file_descriptor(&logger, "third-token", "third", &mut descriptors).unwrap(),
        5
    );
    let source_fds = descriptors
        .iter()
        .map(|descriptor| descriptor.source.as_raw_fd())
        .collect::<Vec<_>>();
    assert!(source_fds.iter().all(|fd| *fd >= 64));
    for descriptor in &descriptors {
        let flags = fcntl(&descriptor.source, FcntlArg::F_GETFD).unwrap();
        assert_ne!(
            flags & libc::FD_CLOEXEC,
            0,
            "父进程高位 source FD 必须带 CLOEXEC"
        );
    }

    let source_checks = source_fds
        .iter()
        .map(|fd| format!("[ ! -e /proc/self/fd/{fd} ] || exit 90;"))
        .collect::<Vec<_>>()
        .join(" ");
    let shell = format!(
        "{source_checks} IFS= read -r first <&3 || exit 91; IFS= read -r second <&4 || exit 92; IFS= read -r third <&5 || exit 93; printf '%s|%s|%s' \"$first\" \"$second\" \"$third\""
    );
    let mut command = Command::new("/bin/sh");
    command
        .args(["-c", &shell])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: closure 在 fork 后、exec 前只调用 async-signal-safe 的 dup2；FD 0 已由
    // Command 配成 /dev/null。先占据 3/4 是测试夹具，随后生产映射必须覆盖它们。
    unsafe {
        command.as_std_mut().pre_exec(|| {
            for target in [3, 4, 5] {
                if libc::dup2(libc::STDIN_FILENO, target) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    install_token_descriptors(&mut command, &descriptors).unwrap();

    let output = command.output().await.unwrap();
    assert!(
        output.status.success(),
        "child status={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"first-token|second-token|third-token");
}

/// 对应 Go：`TestWriteCCRUpstreamProxyToken`。
///
/// 验证 relay token 原子替换、权限收紧和临时文件清理。
#[test]
fn test_write_ccr_upstream_proxy_token() {
    let temp = TempDir::new().unwrap();
    let directory = temp.path().join("run/ccr");
    let path = directory.join("session_token");
    write_ccr_upstream_proxy_token(&path, "first-token").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    write_ccr_upstream_proxy_token(&path, "replacement-token").unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "replacement-token");
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(fs::read_dir(directory).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".session_token-")
    }));
}

/// 验证原子 rename 替换 symlink 本身，而不是跟随链接覆盖外部目标。
#[test]
fn ccr_token_atomic_replace_does_not_follow_destination_symlink() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let directory = temp.path().join("run/ccr");
    fs::create_dir_all(&directory).unwrap();
    let victim = temp.path().join("victim");
    fs::write(&victim, "victim-content").unwrap();
    let path = directory.join("session_token");
    symlink(&victim, &path).unwrap();

    write_ccr_upstream_proxy_token(&path, "replacement-token").unwrap();

    assert!(
        !fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), "replacement-token");
    assert_eq!(fs::read_to_string(victim).unwrap(), "victim-content");
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

/// 验证空 path/token 在任何文件副作用前失败。
#[test]
fn test_write_ccr_upstream_proxy_token_rejects_empty_inputs() {
    let temp = TempDir::new().unwrap();
    assert!(write_ccr_upstream_proxy_token(Path::new(""), "token").is_err());
    assert!(write_ccr_upstream_proxy_token(&temp.path().join("token"), "").is_err());
}

/// 对应 Go：`TestShouldPrepareCCRUpstreamProxyToken`。
///
/// 验证 startup 环境覆盖 config，并且仅 code-session remote proxy 同时开启才写 relay。
#[test]
fn test_should_prepare_ccr_upstream_proxy_token() {
    let cases = [
        ("true", "true", true, None, true),
        ("true", "true", false, None, false),
        ("false", "true", true, None, false),
        ("true", "false", true, None, false),
        ("true", "true", true, Some("false"), false),
    ];
    for (remote, proxy, code_session, startup_proxy, expected) in cases {
        let mut cfg = empty_config();
        cfg.environment
            .insert("CLAUDE_CODE_REMOTE".to_owned(), remote.to_owned());
        cfg.environment
            .insert("CCR_UPSTREAM_PROXY_ENABLED".to_owned(), proxy.to_owned());
        let mut startup = StartupContext {
            use_code_sessions: code_session,
            ..StartupContext::default()
        };
        if let Some(value) = startup_proxy {
            startup
                .environment_variables
                .insert("CCR_UPSTREAM_PROXY_ENABLED".to_owned(), value.to_owned());
        }
        let (executor, _) = make_executor(cfg, startup);
        assert_eq!(
            executor.should_prepare_ccr_upstream_proxy_token(),
            expected,
            "remote={remote} proxy={proxy} code_session={code_session} startup={startup_proxy:?}"
        );
    }
}

/// 对应 Go：`TestBuildArgsFromGatewayConfigMergesAppendSystemPrompt`。
///
/// 验证 gateway prompt 与本地 prompt 合并，同时跳过空 gateway 值。
#[tokio::test]
async fn test_build_args_from_gateway_config_merges_append_system_prompt() {
    let mut cfg = empty_config();
    cfg.local_append_system_prompt = "local prompt".to_owned();
    let startup = StartupContext {
        claude_code_args: [
            (
                "append-system-prompt".to_owned(),
                "gateway prompt".to_owned(),
            ),
            ("empty".to_owned(), String::new()),
            ("model".to_owned(), "claude-sonnet".to_owned()),
        ]
        .into_iter()
        .collect(),
        ..StartupContext::default()
    };
    let (executor, _) = make_executor(cfg, startup);
    let args = executor
        .build_args_from_gateway_config(vec!["--base".to_owned()])
        .await
        .unwrap();
    assert_flag_value(&args, "--model", "claude-sonnet");
    assert_flag_value(
        &args,
        "--append-system-prompt",
        "gateway prompt\n\nlocal prompt",
    );
    assert!(!has_flag(&args, "--empty"));
}

/// 对应 Go：`TestBuildArgsFromGatewayConfigAppendsLocalPrompt`。
///
/// 当 gateway 未提供 append prompt 时，验证本地 prompt 仍作为独立 flag 附加。
#[tokio::test]
async fn test_build_args_from_gateway_config_appends_local_prompt() {
    let mut cfg = empty_config();
    cfg.local_append_system_prompt = "local prompt".to_owned();
    let startup = StartupContext {
        claude_code_args: [("model".to_owned(), "claude-sonnet".to_owned())]
            .into_iter()
            .collect(),
        ..StartupContext::default()
    };
    let (executor, _) = make_executor(cfg, startup);
    let args = executor
        .build_args_from_gateway_config(vec!["--base".to_owned()])
        .await
        .unwrap();
    assert_flag_value(&args, "--model", "claude-sonnet");
    assert_flag_value(&args, "--append-system-prompt", "local prompt");
}

/// 返回 flag 后的单个值。
fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].as_str())
}

/// 对应 Go：`assertFlagValue`，断言一个双参数 flag 存在且值完全相同。
fn assert_flag_value(args: &[String], flag: &str, expected: &str) {
    assert_eq!(
        flag_value(args, flag),
        Some(expected),
        "参数 {flag} 的值不兼容：{args:?}"
    );
}

/// 对应 Go：`hasFlag`，判断参数列表中是否出现指定 flag。
fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|argument| argument == flag)
}

/// 验证本地模式参数、cwd、环境覆盖、stderr OTEL 行与原始 NDJSON 落盘。
#[tokio::test]
async fn test_execute_runs_claude_command_with_recovered_args_and_env() {
    let temp = TempDir::new().unwrap();
    let capture = temp.path().join("capture.txt");
    let fake = script(
        &temp,
        "fake-claude",
        "#!/bin/sh\n{\nprintf 'PWD=%s\\n' \"$PWD\"\nprintf 'ARGS=%s\\n' \"$*\"\nprintf 'SESSION=%s\\n' \"$CLAUDE_CODE_SESSION_ID\"\nprintf 'REMOTE_SESSION=%s\\n' \"$CLAUDE_CODE_REMOTE_SESSION_ID\"\nprintf 'BASE_URL=%s\\n' \"$ANTHROPIC_BASE_URL\"\nprintf 'CUSTOM_ENV=%s\\n' \"$CUSTOM_ENV\"\n} > \"$CAPTURE_FILE\"\nprintf '{\"type\":\"result\"}\\n'\nprintf 'stderr-marker\\n' >&2\n",
    );
    let mut cfg = empty_config();
    cfg.claude_path = fake.to_string_lossy().into_owned();
    cfg.local_testing_mode = true;
    cfg.working_dir = temp.path().to_string_lossy().into_owned();
    cfg.environment = [
        (
            "CAPTURE_FILE".to_owned(),
            capture.to_string_lossy().into_owned(),
        ),
        ("CUSTOM_ENV".to_owned(), "from-config".to_owned()),
    ]
    .into_iter()
    .collect();
    let startup = StartupContext {
        api_base_url: "https://api.example.test".to_owned(),
        claude_code_args: [("model".to_owned(), "claude-test".to_owned())]
            .into_iter()
            .collect(),
        ..StartupContext::default()
    };
    let (executor, otel) = make_executor(cfg, startup);
    executor.execute(&CancellationToken::new()).await.unwrap();

    let captured = fs::read_to_string(capture).unwrap();
    let canonical = fs::canonicalize(temp.path()).unwrap();
    for expected in [
        format!("PWD={}", canonical.display()),
        "ARGS=--output-format=stream-json --verbose --model claude-test".to_owned(),
        "SESSION=session-123".to_owned(),
        "REMOTE_SESSION=session-123".to_owned(),
        "BASE_URL=https://api.example.test".to_owned(),
        "CUSTOM_ENV=from-config".to_owned(),
    ] {
        assert!(
            captured.contains(&expected),
            "missing {expected:?}: {captured}"
        );
    }
    assert!(!captured.contains("--sdk-url"));
    assert!(has_record(
        &otel.records.lock().unwrap(),
        "[claude] stderr-marker",
        "process",
        "claude",
    ));
    assert!(
        fs::read(CLAUDE_LOG_PATH_FOR_TEST)
            .unwrap()
            .windows(br#"{"type":"result"}"#.len())
            .any(|window| window == br#"{"type":"result"}"#)
    );
}

const CLAUDE_LOG_PATH_FOR_TEST: &str = "/tmp/claude-code.log";

/// 对应 Go：`discardLogger`，创建没有外部输出副作用的测试 logger。
fn discard_logger() -> Logger {
    Logger::new(Arc::new(RecordingSlogHandler::default()))
}

/// 对应 Go：`hasRecord`，查找 message 与指定字符串属性同时匹配的记录。
fn has_record(records: &[LogRecord], message: &str, key: &str, value: &str) -> bool {
    records.iter().any(|record| {
        record.message == message
            && record
                .attributes
                .get(key)
                .and_then(|attribute| attribute.as_str())
                == Some(value)
    })
}

/// 验证取消只杀 shell direct child，已脱离 stdio 的孙进程仍存活。
#[tokio::test]
async fn cancellation_does_not_kill_process_group() {
    // fake Claude 通过 PATH 调用 sleep/mv；与全局 PATH 写入测试串行，确保 PID
    // marker 的时序只反映 direct-child 取消语义。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let pid_file = temp.path().join("grandchild.pid");
    let fake = script(
        &temp,
        "fake-claude",
        // 先写同目录临时文件再 rename，否则高并发全仓测试可在
        // open/truncate 与 PID 数字写完之间读到一个合法但截断的 PID。
        "#!/bin/sh\nsleep 30 </dev/null >/dev/null 2>&1 &\nprintf '%s' \"$!\" > \"$GRANDCHILD_PID_FILE.tmp\"\nmv \"$GRANDCHILD_PID_FILE.tmp\" \"$GRANDCHILD_PID_FILE\"\nwhile :; do :; done\n",
    );
    let mut cfg = empty_config();
    cfg.claude_path = fake.to_string_lossy().into_owned();
    cfg.local_testing_mode = true;
    cfg.working_dir = temp.path().to_string_lossy().into_owned();
    cfg.environment.insert(
        "GRANDCHILD_PID_FILE".to_owned(),
        pid_file.to_string_lossy().into_owned(),
    );
    let (executor, _) = make_executor(cfg, StartupContext::default());
    let cancellation = CancellationToken::new();
    let execution = tokio::spawn({
        let cancellation = cancellation.clone();
        async move { executor.execute(&cancellation).await }
    });
    for _ in 0..100 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pid: i32 = fs::read_to_string(&pid_file).unwrap().parse().unwrap();
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.to_string(), "claude code stopped: context canceled");
    // SAFETY: signal 0 只探测子进程 PID 是否存在，不会修改目标进程状态。
    assert_eq!(unsafe { libc::kill(pid, 0) }, 0);
    // SAFETY: PID 来自本测试子进程创建的文件，SIGKILL 只用于测试结束清理。
    let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
}

/// 验证 Claude direct child 被取消后，继承 stdio 的孙进程会延迟 Execute 返回。
///
/// Go `Cmd.Run` 会等待 stdout/stderr copy goroutine；只有显式结束孙进程、让两个
/// pipe 到达 EOF 后，Rust 才能返回 context canceled，不能擅自关闭 reader。
#[tokio::test]
async fn claude_cancellation_waits_for_descendant_pipe_eof() {
    // pipe-holder fixture 同样依赖 PATH 中的 sleep/mv，必须保护到孙进程清理完成。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let pid_file = temp.path().join("pipe-holder.pid");
    let fake = script(
        &temp,
        "fake-claude-pipe-holder",
        "#!/bin/sh\nsleep 30 &\nprintf '%s' \"$!\" > \"$GRANDCHILD_PID_FILE.tmp\"\nmv \"$GRANDCHILD_PID_FILE.tmp\" \"$GRANDCHILD_PID_FILE\"\nwhile :; do :; done\n",
    );
    let mut cfg = empty_config();
    cfg.claude_path = fake.to_string_lossy().into_owned();
    cfg.local_testing_mode = true;
    cfg.working_dir = temp.path().to_string_lossy().into_owned();
    cfg.environment.insert(
        "GRANDCHILD_PID_FILE".to_owned(),
        pid_file.to_string_lossy().into_owned(),
    );
    let (executor, _) = make_executor(cfg, StartupContext::default());
    let cancellation = CancellationToken::new();
    let mut execution = tokio::spawn({
        let cancellation = cancellation.clone();
        async move { executor.execute(&cancellation).await }
    });
    for _ in 0..100 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pid: i32 = fs::read_to_string(&pid_file).unwrap().parse().unwrap();
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut execution)
            .await
            .is_err(),
        "继承 pipe 的孙进程存活时 Claude Execute 不应返回"
    );
    // SAFETY: PID 来自本测试创建的 sleep；SIGKILL 只释放 fixture 继承的 pipe。
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    let error = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.to_string(), "claude code stopped: context canceled");
}

/// 验证 init-only 环境过滤仅删除精确的 `CCR_PLUGINS_JSON` 键。
#[test]
fn init_only_environment_filter_is_exact() {
    let filtered = filter_init_only_from_environ([
        ("CCR_PLUGINS_JSON", "secret"),
        ("CCR_PLUGINS_JSON_SUFFIX", "kept"),
        ("NORMAL", "value"),
    ]);
    assert_eq!(
        filtered,
        vec![
            ("CCR_PLUGINS_JSON_SUFFIX".into(), "kept".into()),
            ("NORMAL".into(), "value".into())
        ]
    );
}

/// 验证远端、本地、path/query 和 code-session URL 拼接。
#[test]
fn session_urls_match_go_string_semantics() {
    assert_eq!(
        build_session_urls("https://api.example.com/path?q=1", "s", "v2", false),
        (
            "wss://api.example.com/v2/session_ingress/ws/s".to_owned(),
            "https://api.example.com/v2/session_ingress/session/s".to_owned()
        )
    );
    assert_eq!(
        build_session_urls("https://localhost:8080", "s", "v1", false),
        (
            "ws://localhost:8080/v1/session_ingress/ws/s".to_owned(),
            "http://localhost:8080/v1/session_ingress/session/s".to_owned()
        )
    );
    assert_eq!(
        build_session_urls("http://api.example.com", "s", "v2", true),
        (
            "http://api.example.com/v1/code/sessions/s".to_owned(),
            "http://api.example.com/v1/code/sessions/s".to_owned()
        )
    );
}
