//! `internal/process/process_test.go` 对应的 Rust 行为测试。

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::{ffi::OsStringExt as _, fs::PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    logger::Logger,
    o11y::{DiscardO11yService, O11yService as _},
    util::{OutputStreamer, StreamType, output_streamer},
};

use super::process::stream_pipe;
use super::script::split_pattern;
use super::{ProcessError, ProcessExitError, ScriptError, execute, execute_script};

type RecordedData = Arc<Mutex<HashMap<StreamType, Vec<u8>>>>;

/// 在 PATH/GODEBUG/cwd 兼容测试结束时恢复进程全局状态。
struct ProcessEnvironmentRestore {
    path: Option<OsString>,
    godebug: Option<OsString>,
    cwd: PathBuf,
}

impl Drop for ProcessEnvironmentRestore {
    /// 在仍持有 crate 级进程状态锁时恢复环境和工作目录。
    fn drop(&mut self) {
        // SAFETY: 所有调用点都持有 `PROCESS_STATE_TEST_LOCK`，且 child 已 wait 完成；
        // 因而没有并发 Rust 线程在环境写入期间调用 libc getenv/setenv。
        unsafe {
            if let Some(path) = self.path.take() {
                std::env::set_var("PATH", path);
            } else {
                std::env::remove_var("PATH");
            }
            if let Some(godebug) = self.godebug.take() {
                std::env::set_var("GODEBUG", godebug);
            } else {
                std::env::remove_var("GODEBUG");
            }
        }
        std::env::set_current_dir(&self.cwd).expect("必须恢复测试前工作目录");
    }
}

/// 原子地替换 PATH、GODEBUG 和 cwd，并返回作用域恢复 guard。
///
/// 调用方必须在 guard 整个生命周期内持有 `PROCESS_STATE_TEST_LOCK`。
fn replace_process_environment(
    path: &OsStr,
    godebug: Option<&OsStr>,
    cwd: &Path,
) -> ProcessEnvironmentRestore {
    let restore = ProcessEnvironmentRestore {
        path: std::env::var_os("PATH"),
        godebug: std::env::var_os("GODEBUG"),
        cwd: std::env::current_dir().expect("测试进程必须有工作目录"),
    };
    std::env::set_current_dir(cwd).expect("必须切换到隔离测试目录");
    // SAFETY: 调用方持有 crate 级进程状态锁；设置后必须等所有受影响 child 退出。
    unsafe {
        std::env::set_var("PATH", path);
        if let Some(godebug) = godebug {
            std::env::set_var("GODEBUG", godebug);
        } else {
            std::env::remove_var("GODEBUG");
        }
    }
    restore
}

/// 写入权限为 0700 的 POSIX shell 测试脚本。
fn write_executable_script(temp: &TempDir, body: &str) -> PathBuf {
    let path = temp.path().join("script.sh");
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}

/// 对应 Go：`recordedStreams`，线程安全保存 stdout/stderr 字节。
#[derive(Clone)]
struct RecordedStreams {
    data: RecordedData,
}

/// 对应 Go：`newRecordedStreams`，创建两个流均为空的记录器。
fn new_recorded_streams() -> RecordedStreams {
    RecordedStreams {
        data: Arc::new(Mutex::new(HashMap::new())),
    }
}

impl RecordedStreams {
    /// 对应 Go：`recordedStreams.streamer`，返回共享同一缓冲区的输出回调。
    fn streamer(&self) -> OutputStreamer {
        let output = Arc::clone(&self.data);
        output_streamer(move |stream, bytes| {
            output
                .lock()
                .unwrap()
                .entry(stream)
                .or_default()
                .extend_from_slice(bytes);
            Ok::<_, io::Error>(())
        })
    }

    /// 对应 Go：`recordedStreams.output`，将指定流的当前快照转换为 UTF-8 文本。
    fn output(&self, stream: StreamType) -> String {
        String::from_utf8(
            self.data
                .lock()
                .unwrap()
                .get(&stream)
                .cloned()
                .unwrap_or_default(),
        )
        .expect("测试脚本输出必须是 UTF-8")
    }
}

/// 对应 Go：`processTestLogger`。
///
/// 返回使用显式 discard handler 的具体 Logger。生产 API 不保留 Go 的 nil logger
/// fallback，因此每个测试都必须明确注入 handler，防止错误日志污染测试 stderr。
fn process_test_logger() -> Logger {
    Logger::new(DiscardO11yService.log_handler())
}

/// 验证成功状态、时间字段以及 stdout/stderr 并发转发。
#[tokio::test]
async fn test_execute_streams_output_and_records_success() {
    let logger = process_test_logger();
    let temp = TempDir::new().unwrap();
    let script = write_executable_script(
        &temp,
        "#!/bin/sh\nprintf 'hello stdout'\nprintf 'hello stderr' >&2\n",
    );
    let streams = new_recorded_streams();
    let result = execute(
        &CancellationToken::new(),
        logger,
        &script,
        streams.streamer(),
    )
    .await
    .unwrap();
    assert_eq!(result.exit_code, 0);
    assert!(result.error.is_none());
    assert!(result.end_time >= result.start_time);
    assert_eq!(streams.output(StreamType::Stdout), "hello stdout");
    assert_eq!(streams.output(StreamType::Stderr), "hello stderr");
}

/// 验证非零退出仍返回执行结果并保留退出前输出。
#[tokio::test]
async fn test_execute_records_exit_error() {
    let logger = process_test_logger();
    let temp = TempDir::new().unwrap();
    let script = write_executable_script(&temp, "#!/bin/sh\nprintf 'before failure'\nexit 7\n");
    let streams = new_recorded_streams();
    let result = execute(
        &CancellationToken::new(),
        logger,
        &script,
        streams.streamer(),
    )
    .await
    .unwrap();
    assert_eq!(result.exit_code, 7);
    assert_eq!(result.error, Some(ProcessExitError::ExitCode(7)));
    assert_eq!(streams.output(StreamType::Stdout), "before failure");
}

/// 验证空 executable path 在创建子进程前被同步拒绝。
#[tokio::test]
async fn test_execute_validation_errors() {
    let logger = process_test_logger();
    let streams = new_recorded_streams();
    assert!(matches!(
        execute(
            &CancellationToken::new(),
            logger,
            Path::new(""),
            streams.streamer(),
        )
        .await,
        Err(ProcessError::EmptyPath)
    ));
}

/// 验证预取消与 Go `CommandContext.Start` 一样在触碰 `execve` 前失败。
///
/// fixture 故意没有 executable bit；若实现仍尝试 spawn，会得到 PermissionDenied
/// 启动错误而不是稳定的 context-canceled 错误，从而可靠暴露子进程副作用。
#[tokio::test]
async fn pre_cancelled_execute_never_attempts_spawn() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("not-executable.sh");
    std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = execute(
        &cancellation,
        process_test_logger(),
        &path,
        new_recorded_streams().streamer(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ProcessError::Cancelled));
    assert_eq!(
        error.to_string(),
        "failed to start command: context canceled"
    );
}

/// 验证 PATH 搜索失败与显式 slash 启动失败使用 Go 的两种不同错误前缀。
#[tokio::test]
async fn look_path_and_slash_permission_errors_match_go() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::look_path_and_slash_permission_errors_match_go"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let non_executable = temp.path().join("blocked-tool");
    std::fs::write(&non_executable, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&non_executable, std::fs::Permissions::from_mode(0o600)).unwrap();
    let _restore = replace_process_environment(temp.path().as_os_str(), None, temp.path());

    let lookup_error = execute(
        &CancellationToken::new(),
        process_test_logger(),
        Path::new("blocked-tool"),
        new_recorded_streams().streamer(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        lookup_error.to_string(),
        "failed to start command: exec: \"blocked-tool\": executable file not found in $PATH"
    );

    let slash_error = execute(
        &CancellationToken::new(),
        process_test_logger(),
        &non_executable,
        new_recorded_streams().streamer(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        slash_error.to_string(),
        format!(
            "failed to start command: fork/exec {}: permission denied",
            non_executable.display()
        )
    );
}

/// 验证显式 slash 路径中的非法 UTF-8 在 ENOENT/EACCES 启动错误里逐字节保留。
///
/// `Display` 只能返回替换后的合法 UTF-8；真正 CLI 兼容边界必须读取
/// `ProcessError::to_bytes`。这两个断言对应固定 Go 1.25.7 ARM64 probe 的
/// `*fs.PathError` 原始 `Error()` 字节。
#[tokio::test]
async fn slash_spawn_errors_preserve_non_utf8_path_bytes() {
    let temp = TempDir::new().unwrap();
    let missing = temp
        .path()
        .join(OsString::from_vec(b"missing-\xff".to_vec()));
    let missing_error = execute(
        &CancellationToken::new(),
        process_test_logger(),
        &missing,
        new_recorded_streams().streamer(),
    )
    .await
    .unwrap_err();
    let mut expected = b"failed to start command: fork/exec ".to_vec();
    expected.extend_from_slice(missing.as_os_str().as_encoded_bytes());
    expected.extend_from_slice(b": no such file or directory");
    assert_eq!(missing_error.to_bytes(), expected);

    let denied = temp
        .path()
        .join(OsString::from_vec(b"denied-\xfe".to_vec()));
    std::fs::write(&denied, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o600)).unwrap();
    let denied_error = execute(
        &CancellationToken::new(),
        process_test_logger(),
        &denied,
        new_recorded_streams().streamer(),
    )
    .await
    .unwrap_err();
    let mut expected = b"failed to start command: fork/exec ".to_vec();
    expected.extend_from_slice(denied.as_os_str().as_encoded_bytes());
    expected.extend_from_slice(b": permission denied");
    assert_eq!(denied_error.to_bytes(), expected);
}

/// 验证相对 PATH 默认触发 ErrDot，而最后一个 `execerrdot=0` 明确允许执行。
#[tokio::test]
async fn look_path_honors_errdot_and_godebug_override() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::look_path_honors_errdot_and_godebug_override"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let relative_directory = temp.path().join("relative-bin");
    std::fs::create_dir(&relative_directory).unwrap();
    let tool = relative_directory.join("relative-tool");
    std::fs::write(&tool, "#!/bin/sh\nprintf errdot-ok\n").unwrap();
    std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o700)).unwrap();
    let _restore = replace_process_environment(
        OsStr::new("relative-bin"),
        Some(OsStr::new("execerrdot=0,execerrdot=1")),
        temp.path(),
    );

    let error = execute(
        &CancellationToken::new(),
        process_test_logger(),
        Path::new("relative-tool"),
        new_recorded_streams().streamer(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "failed to start command: exec: \"relative-tool\": cannot run executable found relative to current directory"
    );

    // SAFETY: 仍持有 crate 级锁，且上一条 execute 已完成；最后一个同名设置应覆盖前者。
    unsafe { std::env::set_var("GODEBUG", "execerrdot=1,execerrdot=0") };
    let streams = new_recorded_streams();
    let result = execute(
        &CancellationToken::new(),
        process_test_logger(),
        Path::new("relative-tool"),
        streams.streamer(),
    )
    .await
    .unwrap();
    assert_eq!(result.exit_code, 0);
    assert_eq!(streams.output(StreamType::Stdout), "errdot-ok");
}

/// 验证 PATH 目录可包含任意 Unix 字节且不会被 UTF-8 转换破坏。
#[tokio::test]
async fn look_path_preserves_non_utf8_directory_bytes() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::look_path_preserves_non_utf8_directory_bytes"
    )) {
        return;
    }
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let directory = temp
        .path()
        .join(OsString::from_vec(b"non-utf8-\xff-bin".to_vec()));
    std::fs::create_dir(&directory).unwrap();
    let tool = directory.join("byte-path-tool");
    std::fs::write(&tool, "#!/bin/sh\nprintf non-utf8-ok\n").unwrap();
    std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o700)).unwrap();
    let _restore = replace_process_environment(directory.as_os_str(), None, temp.path());

    let streams = new_recorded_streams();
    let result = execute(
        &CancellationToken::new(),
        process_test_logger(),
        Path::new("byte-path-tool"),
        streams.streamer(),
    )
    .await
    .unwrap();
    assert_eq!(result.exit_code, 0);
    assert_eq!(streams.output(StreamType::Stdout), "non-utf8-ok");

    let missing_name = PathBuf::from(OsString::from_vec(b"missing-\xff-tool".to_vec()));
    let error = execute(
        &CancellationToken::new(),
        process_test_logger(),
        &missing_name,
        new_recorded_streams().streamer(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "failed to start command: exec: \"missing-\\xff-tool\": executable file not found in $PATH"
    );
}

/// 验证脚本执行输出自身路径，并且函数返回前临时文件已删除。
#[tokio::test]
async fn test_execute_script_writes_runs_and_cleans_temp_script() {
    // fixture 内的 shell 通过 PATH 查找 stat；与临时替换 PATH 的 LookPath 测试
    // 共享全局锁，避免默认并行执行时产生与被测逻辑无关的启动失败。
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let logger = process_test_logger();
    let output = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&output);
    let streamer = output_streamer(move |stream, bytes| {
        if stream == StreamType::Stdout {
            captured.lock().unwrap().extend_from_slice(bytes);
        }
        Ok::<_, io::Error>(())
    });
    let result = execute_script(
        &CancellationToken::new(),
        logger,
        "#!/bin/sh\nprintf 'SCRIPT_PATH:%s\\n' \"$0\"\nprintf 'SCRIPT_MODE:%s\\n' \"$(stat -c %a \"$0\")\"\n",
        "process-test-*.sh",
        streamer,
    )
    .await
    .unwrap();
    assert_eq!(result.exit_code, 0);
    let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    let mut lines = output.lines();
    let path = lines.next().unwrap().strip_prefix("SCRIPT_PATH:").unwrap();
    assert_eq!(lines.next(), Some("SCRIPT_MODE:700"));
    assert!(!Path::new(path).exists());
}

/// 验证空脚本在触碰文件系统前被拒绝。
#[tokio::test]
async fn test_execute_script_validation_errors() {
    let logger = process_test_logger();
    let streamer = output_streamer(|_, _| Ok::<_, io::Error>(()));
    assert!(matches!(
        execute_script(&CancellationToken::new(), logger, "", "test-*", streamer).await,
        Err(ScriptError::EmptyContent)
    ));
}

/// 验证输出回调报错后 reader 立即停止，而不是继续排空 pipe。
#[tokio::test]
async fn test_stream_pipe_stops_when_streamer_errors() {
    let logger = process_test_logger();
    let (mut writer, reader) = tokio::io::duplex(64);
    writer.write_all(b"hello").await.unwrap();
    drop(writer);

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let streamer = output_streamer(move |_, _| {
        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(io::Error::other("stop"))
    });
    stream_pipe(logger, reader, StreamType::Stdout, streamer).await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// 验证取消 token 只终止直接子进程，并让无孙进程的命令及时回收。
#[tokio::test]
async fn cancellation_kills_process() {
    let logger = process_test_logger();
    let temp = TempDir::new().unwrap();
    // 使用 shell 自身的 busy loop，避免产生孙进程。若使用 `sleep 60`，严格兼容
    // Go 的“只杀直接 child”会让 sleep 继续持有 pipe，测试也应观察到长期阻塞。
    let script = write_executable_script(&temp, "#!/bin/sh\nwhile :; do :; done\n");
    let streams = new_recorded_streams();
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        execute(&cancellation, logger, &script, streams.streamer()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_ne!(result.exit_code, 0);
}

/// 验证取消发送 SIGKILL 给 direct child，但不扩展到它创建的进程组。
///
/// 孙进程先脱离三个 stdio FD，避免它继续持有 pipe 影响 reader EOF；direct child
/// 为 SIGINT/SIGTERM 安装 marker trap。取消后 marker 不存在且孙进程仍存活，分别
/// 证明没有转发软信号、也没有杀整个 process group。
#[tokio::test]
async fn cancellation_kills_only_direct_child_with_sigkill() {
    // 同模块的 LookPath 测试会暂时替换进程级 PATH；本 fixture 需要 sleep/mv，
    // 因此也持有同一锁，避免并行测试令 shell 在写 PID marker 前失败。
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let pid_file = temp.path().join("grandchild.pid");
    let signal_marker = temp.path().join("soft-signal.marker");
    let trap_command = format!(
        "printf trapped > {}",
        shell_single_quote(&signal_marker.to_string_lossy())
    );
    let script = write_executable_script(
        &temp,
        &format!(
            "#!/bin/sh\ntrap {} INT TERM\nsleep 30 </dev/null >/dev/null 2>&1 &\nprintf '%s' \"$!\" > {}.tmp\nmv {}.tmp {}\nwhile :; do :; done\n",
            shell_single_quote(&trap_command),
            shell_single_quote(&pid_file.to_string_lossy()),
            shell_single_quote(&pid_file.to_string_lossy()),
            shell_single_quote(&pid_file.to_string_lossy()),
        ),
    );
    let cancellation = CancellationToken::new();
    let execution = tokio::spawn({
        let cancellation = cancellation.clone();
        let streams = new_recorded_streams();
        async move {
            execute(
                &cancellation,
                process_test_logger(),
                script,
                streams.streamer(),
            )
            .await
        }
    });
    for _ in 0..100 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let grandchild_pid: i32 = std::fs::read_to_string(&pid_file).unwrap().parse().unwrap();
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_code, -1);
    assert_eq!(result.error, Some(ProcessExitError::ExitCode(-1)));
    assert!(!signal_marker.exists());
    assert_eq!(kill(Pid::from_raw(grandchild_pid), None::<Signal>), Ok(()));
    let _cleanup_result = kill(Pid::from_raw(grandchild_pid), Signal::SIGKILL);
}

/// 验证 direct child 被取消后，只要孙进程仍持有 stdout/stderr，Execute 就等待 EOF。
///
/// 这覆盖 Go `StdoutPipe`/`StderrPipe` 的关键副作用：取消不清理进程组，reader 也不
/// 能提前丢弃已经打开的 pipe。测试显式杀掉孙进程后才允许执行任务完成。
#[tokio::test]
async fn cancellation_waits_for_descendant_that_holds_pipes() {
    // 保护 shell 使用的 sleep/mv 不受并行 LookPath 测试的全局 PATH 修改影响。
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let direct_pid_file = temp.path().join("direct.pid");
    let pid_file = temp.path().join("pipe-holder.pid");
    let script = write_executable_script(
        &temp,
        &format!(
            "#!/bin/sh\nprintf '%s' \"$$\" > {}\nsleep 30 &\nprintf '%s' \"$!\" > {}.tmp\nmv {}.tmp {}\nwhile :; do :; done\n",
            shell_single_quote(&direct_pid_file.to_string_lossy()),
            shell_single_quote(&pid_file.to_string_lossy()),
            shell_single_quote(&pid_file.to_string_lossy()),
            shell_single_quote(&pid_file.to_string_lossy()),
        ),
    );
    let cancellation = CancellationToken::new();
    let mut execution = tokio::spawn({
        let cancellation = cancellation.clone();
        let streams = new_recorded_streams();
        async move {
            execute(
                &cancellation,
                process_test_logger(),
                script,
                streams.streamer(),
            )
            .await
        }
    });
    for _ in 0..100 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let direct_pid: i32 = std::fs::read_to_string(&direct_pid_file)
        .unwrap()
        .parse()
        .unwrap();
    let descendant_pid: i32 = std::fs::read_to_string(&pid_file).unwrap().parse().unwrap();
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut execution)
            .await
            .is_err(),
        "孙进程持有 pipe 时 Execute 不应提前返回"
    );
    let mut direct_state = None;
    for _ in 0..100 {
        direct_state = linux_process_state(direct_pid);
        if direct_state.as_deref() == Some("Z") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        direct_state.as_deref(),
        Some("Z"),
        "Go Execute 在 pipe reader 完成前不调用 Wait，因此 direct child 应暂为 zombie"
    );
    assert_eq!(kill(Pid::from_raw(descendant_pid), Signal::SIGKILL), Ok(()));
    let result = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_code, -1);
    assert_eq!(result.error, Some(ProcessExitError::ExitCode(-1)));
    assert!(
        linux_process_state(direct_pid).is_none(),
        "reader EOF 后 Wait 必须回收 direct child"
    );
}

/// 从 Linux `/proc/PID/status` 读取单字母进程状态；进程不存在时返回 `None`。
///
/// 本迁移只支持 Linux ARM64，因此直接使用 `/proc` 比引入跨平台抽象更准确；该
/// helper 仅用于验证 Go 的 wait/reap 可观察顺序。
fn linux_process_state(pid: i32) -> Option<String> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("State:"))
        .and_then(|state| state.split_whitespace().next())
        .map(str::to_owned)
}

/// 将测试字符串安全地嵌入 POSIX shell 单引号字面量。
///
/// 临时路径通常不含引号，但仍完整转义，避免 fixture 因自定义 TMPDIR 而改变含义。
fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// 验证 tempfile pattern 按 Go `CreateTemp` 在最后一个星号处分割。
#[test]
fn splits_temp_pattern() {
    assert_eq!(split_pattern("prefix-*.sh"), ("prefix-", ".sh"));
    assert_eq!(
        split_pattern("prefix*middle*suffix"),
        ("prefix*middle", "suffix")
    );
    assert_eq!(split_pattern("plain"), ("plain", ""));
}
