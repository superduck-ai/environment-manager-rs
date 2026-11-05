//! 子进程启动、取消和输出捕获的行为测试。

use std::collections::HashMap;
use std::io;
use std::os::unix::fs::PermissionsExt;
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

use crate::internal::util::{OutputStreamer, StreamType, output_streamer};

use super::process::stream_pipe;
use super::script::split_pattern;
use super::{ProcessError, ProcessExitError, ScriptError, execute, execute_script};

type RecordedData = Arc<Mutex<HashMap<StreamType, Vec<u8>>>>;

/// 写入权限为 0700 的 POSIX shell 测试脚本。
fn write_executable_script(temp: &TempDir, body: &str) -> PathBuf {
    let path = temp.path().join("script.sh");
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}

/// 线程安全保存 stdout/stderr 字节。
#[derive(Clone)]
struct RecordedStreams {
    data: RecordedData,
}

/// 创建两个流均为空的记录器。
fn new_recorded_streams() -> RecordedStreams {
    RecordedStreams {
        data: Arc::new(Mutex::new(HashMap::new())),
    }
}

impl RecordedStreams {
    /// 返回共享同一缓冲区的输出回调。
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

    /// 将指定流的当前快照转换为 UTF-8 文本。
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

/// 验证成功状态、时间字段以及 stdout/stderr 并发转发。
#[tokio::test]
async fn test_execute_streams_output_and_records_success() {
    let temp = TempDir::new().unwrap();
    let script = write_executable_script(
        &temp,
        "#!/bin/sh\nprintf 'hello stdout'\nprintf 'hello stderr' >&2\n",
    );
    let streams = new_recorded_streams();
    let result = execute(&CancellationToken::new(), &script, streams.streamer())
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
    let temp = TempDir::new().unwrap();
    let script = write_executable_script(&temp, "#!/bin/sh\nprintf 'before failure'\nexit 7\n");
    let streams = new_recorded_streams();
    let result = execute(&CancellationToken::new(), &script, streams.streamer())
        .await
        .unwrap();
    assert_eq!(result.exit_code, 7);
    assert_eq!(result.error, Some(ProcessExitError::ExitCode(7)));
    assert_eq!(streams.output(StreamType::Stdout), "before failure");
}

/// 验证空 executable path 在创建子进程前被同步拒绝。
#[tokio::test]
async fn test_execute_validation_errors() {
    let streams = new_recorded_streams();
    assert!(matches!(
        execute(&CancellationToken::new(), Path::new(""), streams.streamer(),).await,
        Err(ProcessError::EmptyPath)
    ));
}

/// 验证预取消在触碰 `execve` 前失败。
///
/// fixture 故意没有 executable bit；若实现仍尝试 spawn，会得到 PermissionDenied
/// 启动错误而不是稳定的取消错误，从而可靠暴露子进程副作用。
#[tokio::test]
async fn pre_cancelled_execute_never_attempts_spawn() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("not-executable.sh");
    std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = execute(&cancellation, &path, new_recorded_streams().streamer())
        .await
        .unwrap_err();
    assert!(matches!(error, ProcessError::Cancelled));
    assert_eq!(error.to_string(), "command was cancelled before it started");
}

/// 验证启动失败保留 Rust 原生 I/O 错误类型。
#[tokio::test]
async fn spawn_errors_preserve_native_io_kind() {
    let temp = TempDir::new().unwrap();
    let non_executable = temp.path().join("blocked-tool");
    std::fs::write(&non_executable, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&non_executable, std::fs::Permissions::from_mode(0o600)).unwrap();
    let error = execute(
        &CancellationToken::new(),
        &non_executable,
        new_recorded_streams().streamer(),
    )
    .await
    .unwrap_err();
    match error {
        ProcessError::Start { source, .. } => {
            assert_eq!(source.kind(), io::ErrorKind::PermissionDenied);
        }
        other => panic!("expected a native spawn error, got {other}"),
    }
}

/// 验证脚本执行输出自身路径，并且函数返回前临时文件已删除。
#[tokio::test]
async fn test_execute_script_writes_runs_and_cleans_temp_script() {
    // fixture 内的 shell 通过 PATH 查找 stat，因此持有进程状态锁，避免其他测试
    // 修改全局环境时产生与被测逻辑无关的启动失败。
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let output = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&output);
    let streamer = output_streamer(move |stream, bytes| {
        if stream == StreamType::Stdout {
            captured.lock().unwrap().extend_from_slice(bytes);
        }
        Ok::<_, io::Error>(())
    });
    let result = execute_script(&CancellationToken::new(), "#!/bin/sh\nprintf 'SCRIPT_PATH:%s\\n' \"$0\"\nprintf 'SCRIPT_MODE:%s\\n' \"$(stat -c %a \"$0\")\"\n", "process-test-*.sh", streamer)
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
    let streamer = output_streamer(|_, _| Ok::<_, io::Error>(()));
    assert!(matches!(
        execute_script(&CancellationToken::new(), "", "test-*", streamer).await,
        Err(ScriptError::EmptyContent)
    ));
}

/// 验证输出回调报错后 reader 立即停止，而不是继续排空 pipe。
#[tokio::test]
async fn test_stream_pipe_stops_when_streamer_errors() {
    let (mut writer, reader) = tokio::io::duplex(64);
    writer.write_all(b"hello").await.unwrap();
    drop(writer);

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let streamer = output_streamer(move |_, _| {
        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(io::Error::other("stop"))
    });
    stream_pipe(reader, StreamType::Stdout, streamer).await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// 验证取消 token 只终止直接子进程，并让无孙进程的命令及时回收。
#[tokio::test]
async fn cancellation_kills_process() {
    let temp = TempDir::new().unwrap();
    // 使用 shell 自身的 busy loop，避免产生孙进程。若使用 `sleep 60`，
    // “只杀直接 child”会让 sleep 继续持有 pipe，测试也应观察到长期阻塞。
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
        execute(&cancellation, &script, streams.streamer()),
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
    // fixture 需要 sleep/mv，因此持有进程状态锁，避免并行测试修改 PATH。
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
        async move { execute(&cancellation, script, streams.streamer()).await }
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

/// 验证取消后先回收 direct child，再等待孙进程关闭 stdout/stderr。
#[tokio::test]
async fn cancellation_waits_for_descendant_that_holds_pipes() {
    // 保护 shell 使用的 sleep/mv 不受并行测试修改全局 PATH 的影响。
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
        async move { execute(&cancellation, script, streams.streamer()).await }
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
    let mut direct_state = linux_process_state(direct_pid);
    for _ in 0..100 {
        if direct_state.is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        direct_state = linux_process_state(direct_pid);
    }
    assert!(
        direct_state.is_none(),
        "等待孙进程 pipe EOF 时 direct child 应已经被回收"
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
        "reader EOF 后 direct child 仍应保持已回收状态"
    );
}

/// 从 Linux `/proc/PID/status` 读取单字母进程状态；进程不存在时返回 `None`。
///
/// 测试目标仅支持 Linux ARM64，因此直接使用 `/proc` 比引入跨平台抽象更准确；该
/// helper 仅用于验证 wait/reap 的可观察顺序。
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

/// 验证 tempfile pattern 按 临时文件创建 在最后一个星号处分割。
#[test]
fn splits_temp_pattern() {
    assert_eq!(split_pattern("prefix-*.sh"), ("prefix-", ".sh"));
    assert_eq!(
        split_pattern("prefix*middle*suffix"),
        ("prefix*middle", "suffix")
    );
    assert_eq!(split_pattern("plain"), ("plain", ""));
}
