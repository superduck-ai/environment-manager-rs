//! Claude 安装/版本、init hook 与 outcome 验证测试。

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    os::unix::{ffi::OsStringExt as _, fs::PermissionsExt as _},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde_json::Value;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{LogCategory, LogLevel, SessionError},
    config::ClaudeConfig,
    logger::{
        LogAttributes, LogHandler, LogHandlerError, LogLevel as LoggerLevel, LogRecord, Logger,
    },
    session::{ActivityRecorder, ActivityRecorderError},
    util::Stopper,
};

use super::{
    Outcomes,
    install::{get_installed_version, is_specific_version},
    install_or_update_claude_code, run_init,
    types::run_combined_command,
};

/// 丢弃全部日志的 handler。
#[derive(Clone, Default)]
struct DiscardHandler;

impl LogHandler for DiscardHandler {
    /// 测试中允许 logger 构造记录。
    fn enabled(&self, _level: LoggerLevel) -> bool {
        true
    }

    /// 丢弃记录并成功返回。
    fn handle(&self, _record: &LogRecord) -> Result<(), LogHandlerError> {
        Ok(())
    }

    /// 派生 handler 仍丢弃属性与记录。
    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }

    /// 派生 group handler 仍丢弃记录。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 返回无输出 logger。
fn logger() -> Logger {
    Logger::new(Arc::new(DiscardHandler))
}

/// 保存 outcome 验证日志的 handler，用于同时断言消息、级别和有序属性。
#[derive(Clone, Default)]
struct RecordingOutcomeHandler {
    records: Arc<Mutex<Vec<LogRecord>>>,
}

impl LogHandler for RecordingOutcomeHandler {
    /// DEBUG 与 WARN 都必须进入记录槽，否则会漏掉空分支兼容日志。
    fn enabled(&self, _level: LoggerLevel) -> bool {
        true
    }

    /// 克隆完整记录；测试随后需要检查属性插入顺序。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        self.records.lock().unwrap().push(record.clone());
        Ok(())
    }

    /// outcome logger 不派生固定属性；仍返回共享记录槽以满足 handler 契约。
    fn with_attrs(&self, _attributes: LogAttributes) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }

    /// outcome logger 不使用 group；派生 handler 继续写入同一记录槽。
    fn with_group(&self, _name: &str) -> Arc<dyn LogHandler> {
        Arc::new(self.clone())
    }
}

/// 创建 outcome 专用收集 logger，并返回可在调用后读取的共享记录槽。
fn recording_outcome_logger() -> (Logger, Arc<Mutex<Vec<LogRecord>>>) {
    let handler = RecordingOutcomeHandler::default();
    let records = Arc::clone(&handler.records);
    (Logger::new(Arc::new(handler)), records)
}

/// 一条测试活动记录。
#[derive(Clone, Debug)]
struct Activity {
    level: LogLevel,
    category: LogCategory,
    message: String,
    extra: BTreeMap<String, Value>,
}

/// 收集同步活动，并忽略 failure result 网络发送。
#[derive(Default)]
struct RecordingActivityRecorder {
    activities: Mutex<Vec<Activity>>,
}

#[async_trait]
impl ActivityRecorder for RecordingActivityRecorder {
    /// 保存一条活动供断言。
    fn record_activity(
        &self,
        level: LogLevel,
        category: LogCategory,
        message: String,
        extra: BTreeMap<String, Value>,
    ) {
        self.activities.lock().unwrap().push(Activity {
            level,
            category,
            message,
            extra,
        });
    }

    /// 安装测试不需要周期任务，返回空 stopper。
    fn record_long_running_activity(
        &self,
        _category: LogCategory,
        _message: String,
    ) -> Arc<dyn Stopper> {
        Arc::new(NoopTestStopper)
    }

    /// 接受 failure result 并成功返回。
    async fn record_failure_result(
        &self,
        _session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        Ok(())
    }
}

/// 对应 Go：`noopTestStopper`，安装测试没有真实长任务需要停止。
struct NoopTestStopper;

#[async_trait]
impl Stopper for NoopTestStopper {
    /// 对应同步取消阶段；fixture 没有后台任务，因此保持空操作。
    fn request_stop(&self) {}

    /// 对应 Go：`noopTestStopper.Stop`；空实现是该 fixture 的真实契约。
    async fn stop(&self) {}
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

/// 对应 Go：`writeScript`。
///
/// 在给定临时目录写 mode 0700 的 fake 可执行脚本，并返回其完整路径。
fn write_script(temp: &TempDir, name: &str, body: &str) -> PathBuf {
    let path = temp.path().join(name);
    fs::write(&path, body).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}

/// 对应 Go：`writeVersionedClaudeScript`。
///
/// 写一个响应 `--version` 的 fake Claude，并把其余命令交给 `extra` 脚本片段。
fn write_versioned_claude_script(temp: &TempDir, version: &str, extra: &str) -> PathBuf {
    write_script(
        temp,
        "claude",
        &format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'Claude Code {version}\\n'; exit 0; fi\n{extra}\n"
        ),
    )
}

/// 在作用域结束时恢复 PATH。
struct PathRestore(Option<OsString>);

impl Drop for PathRestore {
    /// 恢复调用测试前的 PATH 值。
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            // SAFETY: crate 级 PROCESS_STATE_TEST_LOCK 覆盖跨模块 PATH 修改，且
            // restore guard 在锁 guard 之前析构。
            unsafe { std::env::set_var("PATH", path) };
        } else {
            // SAFETY: 同上；原环境没有 PATH，因此恢复为未设置。
            unsafe { std::env::remove_var("PATH") };
        }
    }
}

/// 临时把 PATH 替换为 fake npm 所在目录并返回恢复 guard。
fn replace_path(path: &std::path::Path) -> PathRestore {
    let original = std::env::var_os("PATH");
    // SAFETY: 调用者持有 crate 级 PROCESS_STATE_TEST_LOCK，测试等待所有受影响
    // child 完成后才释放 restore guard。
    unsafe { std::env::set_var("PATH", path) };
    PathRestore(original)
}

/// 验证具体版本的 substring 正则语义和保留关键字。
#[test]
fn test_is_specific_version() {
    for version in ["1.2.3", "v1.2.3", "1.2.3-beta"] {
        assert!(is_specific_version(version), "{version}");
    }
    for version in ["", "skip", "latest", "stable", "current", "abc"] {
        assert!(!is_specific_version(version), "{version}");
    }
}

/// 验证 `--version` 首个匹配与缺失可执行文件的空值回退。
#[tokio::test]
async fn test_get_installed_version() {
    let temp = TempDir::new().unwrap();
    let claude = write_script(
        &temp,
        "claude",
        "#!/bin/sh\nprintf 'Claude Code 1.2.3\\n'\nprintf '9.9.9\\n' >&2\n",
    );
    let cancellation = CancellationToken::new();
    assert_eq!(
        get_installed_version(&cancellation, claude.to_str().unwrap()).await,
        "1.2.3"
    );
    assert_eq!(
        get_installed_version(&cancellation, "/missing/claude").await,
        ""
    );
}

/// 验证 npm 安装目标恢复为官方 package，并仅在具体版本时追加 `@version`。
///
/// 生产 helper 是 `install` 子模块私有函数；测试通过完整公共安装流程观察 fake npm
/// 收到的 argv，同时让 fake Claude 提供安装后版本，因而覆盖真实调用而非复制算法。
#[tokio::test]
async fn test_install_via_npm_uses_recovered_package_target() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_install_via_npm_uses_recovered_package_target"
    )) {
        return;
    }
    let _path_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let state = temp.path().join("installed-version");
    let args_path = temp.path().join("npm-args");
    let claude = write_script(
        &temp,
        "claude",
        &format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then [ -f '{}' ] && /bin/cat '{}'; exit 0; fi\n",
            state.display(),
            state.display()
        ),
    );
    write_script(
        &temp,
        "npm",
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" > '{}'\ncase \"$*\" in\n  *'@1.2.3') printf 'Claude Code 1.2.3\\n' > '{}' ;;\n  *) printf 'Claude Code 2.0.0\\n' > '{}' ;;\nesac\n",
            args_path.display(),
            state.display(),
            state.display()
        ),
    );
    let _restore = replace_path(temp.path());
    let recorder = RecordingActivityRecorder::default();

    for (target, expected_args, expected_version) in [
        (
            "1.2.3",
            "install -g @anthropic-ai/claude-code@1.2.3",
            "1.2.3",
        ),
        ("latest", "install -g @anthropic-ai/claude-code", "2.0.0"),
    ] {
        // 每轮删除状态，使公共流程重新进入“未安装 → npm”分支。
        let _ = fs::remove_file(&state);
        let installed = install_or_update_claude_code(
            &CancellationToken::new(),
            &logger(),
            claude.to_str().unwrap(),
            target,
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(installed, expected_version);
        assert_eq!(
            fs::read_to_string(&args_path).unwrap().trim(),
            expected_args
        );
    }
}

/// 验证已有 Claude 的升级路径执行 `install <version>`，而不是回退到 npm。
#[tokio::test]
async fn test_upgrade_via_cli() {
    let temp = TempDir::new().unwrap();
    let state = temp.path().join("version");
    let args_path = temp.path().join("claude-args");
    fs::write(&state, "Claude Code 1.0.0\n").unwrap();
    let claude = write_script(
        &temp,
        "claude",
        &format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then /bin/cat '{}'; exit 0; fi\nprintf '%s\\n' \"$*\" > '{}'\nprintf 'Claude Code 2.0.0\\n' > '{}'\n",
            state.display(),
            args_path.display(),
            state.display()
        ),
    );

    let installed = install_or_update_claude_code(
        &CancellationToken::new(),
        &logger(),
        claude.to_str().unwrap(),
        "stable",
        &RecordingActivityRecorder::default(),
    )
    .await
    .unwrap();
    assert_eq!(installed, "2.0.0");
    assert_eq!(
        fs::read_to_string(args_path).unwrap().trim(),
        "install stable"
    );
}

/// 验证 skip/current/目标已安装三条不执行安装的路径。
#[tokio::test]
async fn test_install_or_update_claude_code_skip_current_and_already_installed() {
    let recorder = RecordingActivityRecorder::default();
    let cancellation = CancellationToken::new();
    assert_eq!(
        install_or_update_claude_code(
            &cancellation,
            &logger(),
            "/missing/claude",
            "skip",
            &recorder,
        )
        .await
        .unwrap(),
        ""
    );
    let error = install_or_update_claude_code(
        &cancellation,
        &logger(),
        "/missing/claude",
        "current",
        &recorder,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("version=current"));
    {
        let activities = recorder.activities.lock().unwrap();
        let missing_current = activities.last().unwrap();
        assert_eq!(missing_current.category, LogCategory::Init);
        assert_eq!(
            missing_current.message,
            "Claude Code required but not found (version=current)"
        );
        assert!(missing_current.extra.is_empty());
    }

    let temp = TempDir::new().unwrap();
    let claude = write_versioned_claude_script(&temp, "1.2.3", "");
    for target in ["current", "1.2.3"] {
        assert_eq!(
            install_or_update_claude_code(
                &cancellation,
                &logger(),
                claude.to_str().unwrap(),
                target,
                &recorder,
            )
            .await
            .unwrap(),
            "1.2.3"
        );
    }
}

/// 验证首次安装调用恢复出的 npm package target 并做安装后版本校验。
#[tokio::test]
async fn test_install_or_update_claude_code_installs_via_npm_and_verifies() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_install_or_update_claude_code_installs_via_npm_and_verifies"
    )) {
        return;
    }
    let _path_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let state = temp.path().join("installed-version");
    let args_path = temp.path().join("npm-args");
    let claude = write_script(
        &temp,
        "claude",
        &format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then [ -f '{}' ] && /bin/cat '{}'; exit 0; fi\n",
            state.display(),
            state.display()
        ),
    );
    let npm = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" > '{}'\nprintf 'Claude Code 1.2.3\\n' > '{}'\n",
        args_path.display(),
        state.display()
    );
    write_script(&temp, "npm", &npm);
    let _restore = replace_path(temp.path());
    let recorder = RecordingActivityRecorder::default();
    let installed = install_or_update_claude_code(
        &CancellationToken::new(),
        &logger(),
        claude.to_str().unwrap(),
        "1.2.3",
        &recorder,
    )
    .await
    .unwrap();
    assert_eq!(installed, "1.2.3");
    assert_eq!(
        fs::read_to_string(args_path).unwrap().trim(),
        "install -g @anthropic-ai/claude-code@1.2.3"
    );
}

/// 验证 CLI 升级成功，以及失败时继续使用旧版本并记录 warning。
#[tokio::test]
async fn test_install_or_update_claude_code_upgrades_via_cli_and_falls_back_on_failure() {
    let temp = TempDir::new().unwrap();
    let state = temp.path().join("version");
    let args_path = temp.path().join("claude-args");
    fs::write(&state, "Claude Code 1.0.0\n").unwrap();
    let claude = write_script(
        &temp,
        "claude",
        &format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then /bin/cat '{}'; exit 0; fi\nprintf '%s\\n' \"$*\" > '{}'\nprintf 'Claude Code 2.0.0\\n' > '{}'\n",
            state.display(),
            args_path.display(),
            state.display()
        ),
    );
    let recorder = RecordingActivityRecorder::default();
    assert_eq!(
        install_or_update_claude_code(
            &CancellationToken::new(),
            &logger(),
            claude.to_str().unwrap(),
            "latest",
            &recorder,
        )
        .await
        .unwrap(),
        "2.0.0"
    );
    assert_eq!(
        fs::read_to_string(args_path).unwrap().trim(),
        "install latest"
    );

    let failing = write_script(
        &temp,
        "claude-fail",
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf 'Claude Code 1.0.0\\n'; exit 0; fi\nprintf 'upgrade failed\\n' >&2\nexit 9\n",
    );
    assert_eq!(
        install_or_update_claude_code(
            &CancellationToken::new(),
            &logger(),
            failing.to_str().unwrap(),
            "latest",
            &recorder,
        )
        .await
        .unwrap(),
        "1.0.0"
    );
    assert_eq!(
        recorder.activities.lock().unwrap().last().unwrap().level,
        LogLevel::Warning
    );
}

/// 验证安装错误在有旧版本时降级为 warning，无旧版本时返回致命错误。
///
/// 两个分支都通过公共安装状态机触发私有 `handleInstallError`，并断言返回值与
/// activity 级别，从而保留 Go 调用方实际可观察的契约。
#[tokio::test]
async fn test_handle_install_error() {
    // fatal npm 分支会替换 PATH；整项测试隔离，避免前半段断言与后半段分属两个进程。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_handle_install_error"
    )) {
        return;
    }
    let temp = TempDir::new().unwrap();
    let failing_claude =
        write_versioned_claude_script(&temp, "1.0.0", "printf 'upgrade failed\\n' >&2\nexit 9");
    let nonfatal_recorder = RecordingActivityRecorder::default();
    let installed = install_or_update_claude_code(
        &CancellationToken::new(),
        &logger(),
        failing_claude.to_str().unwrap(),
        "latest",
        &nonfatal_recorder,
    )
    .await
    .unwrap();
    assert_eq!(installed, "1.0.0");
    assert_eq!(
        nonfatal_recorder
            .activities
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .level,
        LogLevel::Warning
    );

    let _path_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let npm_dir = TempDir::new().unwrap();
    write_script(
        &npm_dir,
        "npm",
        "#!/bin/sh\nprintf 'npm exploded\\n' >&2\nexit 5\n",
    );
    let _restore = replace_path(npm_dir.path());
    let fatal_recorder = RecordingActivityRecorder::default();
    let error = install_or_update_claude_code(
        &CancellationToken::new(),
        &logger(),
        "/missing/claude",
        "latest",
        &fatal_recorder,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("npm install"));
    assert_eq!(
        fatal_recorder
            .activities
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .level,
        LogLevel::Error
    );
}

/// 验证 npm CombinedOutput 的非 UTF-8 与 NUL 字节一直保留在 ClaudeError 中。
///
/// `Display` 只能产生有损 UTF-8 视图；兼容 CLI 必须使用 `ClaudeError::as_bytes()`。
#[tokio::test]
async fn npm_install_error_preserves_raw_combined_output_bytes() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::npm_install_error_preserves_raw_combined_output_bytes"
    )) {
        return;
    }
    let _path_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let npm_dir = TempDir::new().unwrap();
    write_script(
        &npm_dir,
        "npm",
        "#!/bin/sh\nprintf '\\377\\376\\000'\nexit 5\n",
    );
    let _restore = replace_path(npm_dir.path());
    let recorder = RecordingActivityRecorder::default();
    let error = install_or_update_claude_code(
        &CancellationToken::new(),
        &logger(),
        "/missing/claude",
        "latest",
        &recorder,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.as_bytes(),
        b"claude code installation failed during npm install: npm install failed: exit status 5, output: \xff\xfe\0"
    );
}

/// 验证 PATH 解析到非 UTF-8 目录后，spawn PathError 字节可穿过完整安装错误链。
///
/// fixture 具有 executable bit、但内容不是 Linux 可执行格式，使 LookPath 成功而
/// `execve` 返回 ENOEXEC；这样既覆盖 raw resolved path，也覆盖 `command_error` 和
/// fatal install 包装，不依赖直接调用内部格式化 helper。
#[tokio::test]
async fn npm_spawn_error_preserves_non_utf8_resolved_path_bytes() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::npm_spawn_error_preserves_non_utf8_resolved_path_bytes"
    )) {
        return;
    }
    let _path_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let root = TempDir::new().unwrap();
    let npm_dir = root
        .path()
        .join(OsString::from_vec(b"npm-bin-\xff".to_vec()));
    fs::create_dir(&npm_dir).unwrap();
    let npm = npm_dir.join("npm");
    fs::write(&npm, b"not an executable image\n").unwrap();
    fs::set_permissions(&npm, fs::Permissions::from_mode(0o700)).unwrap();
    let _restore = replace_path(&npm_dir);
    let recorder = RecordingActivityRecorder::default();
    let error = install_or_update_claude_code(
        &CancellationToken::new(),
        &logger(),
        "/missing/claude",
        "latest",
        &recorder,
    )
    .await
    .unwrap_err();
    let mut expected =
        b"claude code installation failed during npm install: npm install failed: fork/exec "
            .to_vec();
    expected.extend_from_slice(npm.as_os_str().as_encoded_bytes());
    expected.extend_from_slice(b": exec format error, output: ");
    assert_eq!(error.as_bytes(), expected);
}

/// 验证缺失 PATH executable 使用 Go `exec.LookPath` 文本而不是 Rust errno 文本。
#[tokio::test]
async fn npm_missing_from_path_uses_go_look_path_error() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::npm_missing_from_path_uses_go_look_path_error"
    )) {
        return;
    }
    let _path_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let empty_path = TempDir::new().unwrap();
    let _restore = replace_path(empty_path.path());
    let recorder = RecordingActivityRecorder::default();
    let error = install_or_update_claude_code(
        &CancellationToken::new(),
        &logger(),
        "/missing/claude",
        "latest",
        &recorder,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.as_bytes(),
        b"claude code installation failed during npm install: npm install failed: exec: \"npm\": executable file not found in $PATH, output: "
    );
}

/// 验证预取消的 CombinedOutput 不启动 child，也不产生脚本 marker 副作用。
#[tokio::test]
async fn combined_output_pre_cancel_does_not_spawn() {
    let temp = TempDir::new().unwrap();
    let marker = temp.path().join("spawned.marker");
    let command = write_script(
        &temp,
        "pre-cancel",
        "#!/bin/sh\nprintf spawned > \"$MARKER\"\n",
    );
    let environment = [(OsString::from("MARKER"), marker.as_os_str().to_os_string())];
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = run_combined_command(
        &cancellation,
        command.as_os_str(),
        &[],
        None,
        Some(&environment),
    )
    .await
    .unwrap_err();
    assert_eq!(error.error, b"context canceled");
    assert!(!marker.exists());
}

/// 验证 direct child 退出后，CombinedOutput 仍等待继承 writer 的孙进程关闭 pipe。
#[tokio::test]
async fn combined_output_waits_for_descendant_pipe_eof() {
    // 同模块 npm 测试会替换进程级 PATH；本 fixture 依赖 sleep，必须共用全局锁，
    // 否则 shell 的 “sleep: not found” 会进入 CombinedOutput 并造成伪失败。
    let _path_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let command = write_script(&temp, "pipe-holder", "#!/bin/sh\nsleep 0.25 &\nexit 0\n");
    let started = Instant::now();
    let output = run_combined_command(
        &CancellationToken::new(),
        command.as_os_str(),
        &[],
        None,
        None,
    )
    .await
    .unwrap();
    assert!(output.is_empty());
    assert!(
        started.elapsed() >= Duration::from_millis(180),
        "孙进程关闭继承 pipe 前 CombinedOutput 不应返回"
    );
}

/// 验证 signal 退出使用 Go Linux/ARM64 的完整信号名称。
#[tokio::test]
async fn combined_output_formats_linux_signal_name_like_go() {
    let temp = TempDir::new().unwrap();
    let command = write_script(&temp, "signal-exit", "#!/bin/sh\nkill -USR1 $$\n");
    let error = run_combined_command(
        &CancellationToken::new(),
        command.as_os_str(),
        &[],
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.exit_code, -1);
    assert_eq!(error.error, b"signal: user defined signal 1");
}

/// 验证 init hook 的 cwd/env/过滤和失败退出码文案。
#[tokio::test]
async fn init_hook_runs_with_filtered_environment_and_reports_exit_code() {
    let temp = TempDir::new().unwrap();
    let capture = temp.path().join("init-capture");
    let success = write_script(
        &temp,
        "claude-init",
        "#!/bin/sh\nprintf '%s|%s|%s' \"$1\" \"$CUSTOM_ENV\" \"${CCR_PLUGINS_JSON-unset}\" > \"$CAPTURE\"\n",
    );
    let mut cfg = empty_config();
    cfg.claude_path = success.to_string_lossy().into_owned();
    cfg.environment = [
        ("CUSTOM_ENV".to_owned(), "configured".to_owned()),
        ("CAPTURE".to_owned(), capture.to_string_lossy().into_owned()),
    ]
    .into_iter()
    .collect();
    run_init(
        &CancellationToken::new(),
        &logger(),
        Some(&cfg),
        temp.path(),
    )
    .await
    .unwrap();
    assert_eq!(
        fs::read_to_string(capture).unwrap(),
        "--init-only|configured|unset"
    );

    let failing = write_script(&temp, "claude-fail", "#!/bin/sh\nexit 7\n");
    cfg.claude_path = failing.to_string_lossy().into_owned();
    let error = run_init(
        &CancellationToken::new(),
        &logger(),
        Some(&cfg),
        temp.path(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "claude --init-only failed with exit code 7: exit status 7"
    );
}

/// 验证 outcome 拷贝、摘要、零值 map 与仓库枚举。
#[test]
fn test_outcomes_helpers() {
    let zero = Outcomes::default();
    assert!(zero.repositories().is_none());
    let mut outcomes = Outcomes::new();
    assert!(outcomes.is_empty());
    assert_eq!(outcomes.to_string(), "Outcomes{empty}");
    outcomes.add("anthropic/demo", vec!["main".to_owned(), "dev".to_owned()]);
    assert_eq!(outcomes.get_first("anthropic/demo"), "main");
    assert!(outcomes.get("missing").is_none());
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes.to_string(), "Outcomes{repositories: 1}");
    assert_eq!(outcomes.repositories().unwrap(), ["anthropic/demo"]);
    let mut copied = outcomes.as_map().unwrap();
    copied.get_mut("anthropic/demo").unwrap()[0] = "mutated".to_owned();
    assert_eq!(outcomes.get_first("anthropic/demo"), "main");
}

/// 验证空 branch 保留、多 branch 截断到第一项、单 branch 不变。
#[test]
fn test_outcomes_validate() {
    let mut outcomes = Outcomes::new();
    outcomes.add("empty/repo", Vec::new());
    outcomes.add("multi/repo", vec!["one".to_owned(), "two".to_owned()]);
    outcomes.add("single/repo", vec!["only".to_owned()]);
    let (outcome_logger, records) = recording_outcome_logger();
    let validated = outcomes.validate_with_logger(&outcome_logger);
    assert!(validated.get("empty/repo").is_none());
    assert_eq!(
        validated.get("multi/repo"),
        Some(["one".to_owned()].as_slice())
    );
    assert_eq!(
        validated.get("single/repo"),
        Some(["only".to_owned()].as_slice())
    );

    // HashMap 遍历顺序不稳定，因此按消息定位记录；各记录内部的属性顺序仍须与
    // Go `slog.Logger` 调用参数完全一致。
    let records = records.lock().unwrap();
    assert_eq!(records.len(), 2);
    let empty = records
        .iter()
        .find(|record| {
            record.message == "Repository has no branches specified, preserving empty list"
        })
        .unwrap();
    assert_eq!(empty.level, LoggerLevel::Debug);
    assert_eq!(
        empty
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["repository"]
    );
    assert_eq!(empty.attributes["repository"], "empty/repo");

    let multiple = records
        .iter()
        .find(|record| {
            record.message == "Repository has multiple branches, using only the first one"
        })
        .unwrap();
    assert_eq!(multiple.level, LoggerLevel::Warn);
    assert_eq!(
        multiple
            .attributes
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["repository", "branch_used", "branches_ignored"]
    );
    assert_eq!(multiple.attributes["repository"], "multi/repo");
    assert_eq!(multiple.attributes["branch_used"], "one");
    assert_eq!(
        multiple.attributes["branches_ignored"],
        serde_json::json!(["two"])
    );
}
