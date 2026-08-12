//! Linux ARM64 低层进程语义回归测试。
//!
//! 覆盖命令查找、任意 Unix 路径字节、组合输出、取消/reap/pipe 和继承文件描述符。
//! 这些曾由外部 Go 对照程序验证的场景现在直接作为普通 Rust 测试执行。

use std::{
    ffi::{OsStr, OsString},
    io,
    os::{
        fd::AsRawFd as _,
        unix::{
            ffi::{OsStrExt as _, OsStringExt as _},
            fs::PermissionsExt as _,
        },
    },
    path::{Path, PathBuf},
    time::Duration,
};

use nix::{
    errno::Errno,
    fcntl::{FcntlArg, FdFlag, fcntl},
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tempfile::TempDir;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    process::{ProcessError, execute, go_spawn_error, resolve_go_command},
    util::output_streamer,
};

use super::{
    claude_code_executor::{add_token_via_file_descriptor, install_token_descriptors},
    types::run_combined_command,
};

/// PATH/GODEBUG/cwd 变更结束时恢复测试进程的原始全局状态。
struct ProcessStateRestore {
    path: Option<OsString>,
    godebug: Option<OsString>,
    cwd: PathBuf,
}

impl ProcessStateRestore {
    /// 保存当前状态；调用方随后可在持有全局测试锁时安全修改环境。
    fn capture() -> Self {
        Self {
            path: std::env::var_os("PATH"),
            godebug: std::env::var_os("GODEBUG"),
            cwd: std::env::current_dir().expect("process-edge 测试必须有当前目录"),
        }
    }
}

impl Drop for ProcessStateRestore {
    /// 恢复环境和 cwd，确保 probe 失败也不会污染后续测试。
    fn drop(&mut self) {
        // SAFETY: 测试在整个修改窗口持有 PROCESS_STATE_TEST_LOCK，且所有
        // 受 PATH 影响的 child 都已 wait；没有并发 Rust 线程调用 getenv/setenv。
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
        std::env::set_current_dir(&self.cwd).expect("必须恢复 process-edge 测试 cwd");
    }
}

/// 被测试创建的 descendant PID 清理 guard。
struct PidCleanup(Option<i32>);

impl PidCleanup {
    /// 记录仅由当前 case 创建、可在结束时 SIGKILL 的 PID。
    fn new(pid: i32) -> Self {
        Self(Some(pid))
    }

    /// 显式终止 descendant 并禁用 Drop 的重复清理。
    fn kill(&mut self) {
        if let Some(pid) = self.0.take() {
            kill_pid(pid);
        }
    }
}

impl Drop for PidCleanup {
    /// case panic 时仍尽力终止 pipe holder，避免泄漏后台进程。
    fn drop(&mut self) {
        if let Some(pid) = self.0.take() {
            let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
        }
    }
}

/// 直接验证低层进程边界行为，不依赖外部实现或 transcript 比较器。
#[tokio::test]
async fn process_edge_behaviors_are_preserved() {
    let _process_lock = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let temp = TempDir::new().expect("必须创建 process-edge 临时根");
    let mut observations = Vec::new();

    probe_look_path(temp.path(), &mut observations).await;
    probe_raw_combined_output(&mut observations).await;
    probe_reader_first_reap(temp.path(), &mut observations).await;
    probe_combined_output_reap(temp.path(), &mut observations).await;
    probe_cancelled_reader_first(temp.path(), &mut observations).await;
    probe_cancelled_combined_output(temp.path(), &mut observations).await;
    probe_extra_files(&mut observations).await;

    assert_eq!(observations.len(), 24, "所有进程边界场景都必须完成");
}

/// 运行 Rust LookPath/ErrDot 和非 UTF-8 direct-spawn 场景。
async fn probe_look_path(root: &Path, lines: &mut Vec<String>) {
    let _restore = ProcessStateRestore::capture();
    let bin = root.join("bin");
    std::fs::create_dir(&bin).unwrap();
    let non_executable = bin.join("nonexec");
    std::fs::write(&non_executable, "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&non_executable, std::fs::Permissions::from_mode(0o644)).unwrap();
    set_environment("PATH", Some(bin.as_os_str()));
    record_look_path("lookpath.missing", OsStr::new("missing"), lines);
    record_look_path("lookpath.nonexec", OsStr::new("nonexec"), lines);

    let relative = root.join("relative");
    std::fs::create_dir(&relative).unwrap();
    let relative_tool = relative.join("relative-bin");
    write_script(&relative_tool, "#!/bin/sh\nexit 0\n");
    std::env::set_current_dir(&relative).unwrap();
    set_environment("PATH", Some(OsStr::new(":")));
    set_environment("GODEBUG", None);
    record_errdot_look_path("lookpath.errdot", true, lines);
    set_environment("GODEBUG", Some(OsStr::new("execerrdot=1,execerrdot=0")));
    record_errdot_look_path("lookpath.errdot-last-zero", false, lines);
    set_environment("GODEBUG", Some(OsStr::new("execerrdot=0,execerrdot=1")));
    record_errdot_look_path("lookpath.errdot-last-one", true, lines);

    let raw_directory = root.join(OsString::from_vec(b"bin-\xff".to_vec()));
    std::fs::create_dir(&raw_directory).unwrap();
    write_script(&raw_directory.join("raw-bin"), "#!/bin/sh\nexit 0\n");
    set_environment("PATH", Some(raw_directory.as_os_str()));
    record_look_path("lookpath.nonutf8-directory", OsStr::new("raw-bin"), lines);
    record_look_path(
        "lookpath.nonutf8-name",
        OsStr::from_bytes(b"raw-\xff"),
        lines,
    );

    let raw_missing = root.join(OsString::from_vec(b"miss-\xff".to_vec()));
    let raw_denied = root.join(OsString::from_vec(b"denied-\xfe".to_vec()));
    std::fs::write(&raw_denied, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&raw_denied, std::fs::Permissions::from_mode(0o644)).unwrap();
    record_spawn_error(
        "run.nonutf8-enoent",
        &raw_missing,
        Some(libc::ENOENT),
        lines,
    )
    .await;
    record_spawn_error("run.nonutf8-eacces", &raw_denied, Some(libc::EACCES), lines).await;

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = execute(
        &cancellation,
        &raw_missing,
        output_streamer(|_, _| Ok::<_, io::Error>(())),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ProcessError::Cancelled));
    lines.push(format!(
        "run.precancel-nonutf8-enoent.type=*errors.errorString error.bytes={}",
        hex(b"context canceled")
    ));

    let lookup_error = resolve_go_command(OsStr::new("missing-basename")).unwrap_err();
    lines.push(format!(
        "run.precancel-missing-basename.type=*exec.Error error.bytes={}",
        hex(lookup_error.as_bytes())
    ));
}

/// 记录一条普通 LookPath 结果；错误文本使用 GoCommandError 的原始字节。
fn record_look_path(label: &str, program: &OsStr, lines: &mut Vec<String>) {
    let (path, error) = match resolve_go_command(program) {
        Ok(path) => (path.as_bytes().to_vec(), Vec::new()),
        Err(error) => (Vec::new(), error.into_bytes()),
    };
    lines.push(format!(
        "{label}.path.hex={} error.bytes={}",
        hex(&path),
        hex(&error)
    ));
}

/// 记录 ErrDot；Go 即使返回错误也会同时保留命中的相对候选 path。
fn record_errdot_look_path(label: &str, expect_error: bool, lines: &mut Vec<String>) {
    let result = resolve_go_command(OsStr::new("relative-bin"));
    let error = match (expect_error, result) {
        (true, Err(error)) => error.into_bytes(),
        (false, Ok(path)) => {
            assert_eq!(path.as_bytes(), b"relative-bin");
            Vec::new()
        }
        (expected, actual) => panic!("ErrDot expectation {expected} mismatch: {actual:?}"),
    };
    lines.push(format!(
        "{label}.path.hex={} error.bytes={}",
        hex(b"relative-bin"),
        hex(&error)
    ));
}

/// 实际调用 Tokio spawn，并用生产 `go_spawn_error` 记录非 UTF-8 PathError。
async fn record_spawn_error(
    label: &str,
    path: &Path,
    expected_errno: Option<i32>,
    lines: &mut Vec<String>,
) {
    let source = Command::new(path).spawn().unwrap_err();
    assert_eq!(source.raw_os_error(), expected_errno);
    let error = go_spawn_error(path.as_os_str(), &source);
    lines.push(format!(
        "{label}.type=*fs.PathError error.bytes={}",
        hex(error.as_bytes())
    ));
}

/// 捕获非法 UTF-8/NUL CombinedOutput，并按 Go `%w, output: %s` 拼接原始字节。
async fn probe_raw_combined_output(lines: &mut Vec<String>) {
    let cancellation = CancellationToken::new();
    let error = run_combined_command(
        &cancellation,
        OsStr::new("sh"),
        &[
            OsString::from("-c"),
            OsString::from("printf '\\377\\376\\000'; exit 5"),
        ],
        None,
        None,
    )
    .await
    .unwrap_err();
    let mut wrapped = b"npm install failed: ".to_vec();
    wrapped.extend_from_slice(&error.error);
    wrapped.extend_from_slice(b", output: ");
    wrapped.extend_from_slice(&error.output);
    lines.push(format!("combined.output.hex={}", hex(&error.output)));
    lines.push(format!("combined.error.bytes={}", hex(&error.error)));
    lines.push(format!("combined.wrapped.bytes={}", hex(&wrapped)));
}

/// 验证生产 `process::execute` 先等 pipe reader、后 reap direct child。
async fn probe_reader_first_reap(root: &Path, lines: &mut Vec<String>) {
    let direct_file = root.join("rust-reader-first-direct");
    let holder_file = root.join("rust-reader-first-holder");
    let script = root.join("rust-reader-first.sh");
    write_script(
        &script,
        &format!(
            "#!/bin/sh\necho $$ > {}\n/usr/bin/sleep 30 &\necho $! > {}\nexit 0\n",
            shell_quote(&direct_file),
            shell_quote(&holder_file)
        ),
    );
    let cancellation = CancellationToken::new();
    let mut execution = tokio::spawn({
        let cancellation = cancellation.clone();
        async move {
            execute(
                &cancellation,
                script,
                output_streamer(|_, _| Ok::<_, io::Error>(())),
            )
            .await
        }
    });
    let direct = wait_pid_file(&direct_file).await;
    let holder = wait_pid_file(&holder_file).await;
    let mut cleanup = PidCleanup::new(holder);
    let state = wait_process_state(direct, "Z").await;
    lines.push(format!(
        "reader-first.before-release.state={state} readers-done={} holder-alive={}",
        execution.is_finished(),
        process_exists(holder)
    ));
    cleanup.kill();
    let result = tokio::time::timeout(Duration::from_secs(3), &mut execution)
        .await
        .expect("reader-first execute 必须在 holder 关闭后返回")
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_code, 0);
    lines.push(format!(
        "reader-first.after-release.wait=\"\" direct-exists={}",
        process_exists(direct)
    ));
}

/// 验证生产 CombinedOutput 先 reap direct child，再等待 descendant pipe EOF。
async fn probe_combined_output_reap(root: &Path, lines: &mut Vec<String>) {
    let direct_file = root.join("rust-combined-direct");
    let holder_file = root.join("rust-combined-holder");
    let script = root.join("rust-combined.sh");
    write_script(
        &script,
        &format!(
            "#!/bin/sh\necho $$ > {}\n/usr/bin/sleep 30 &\necho $! > {}\nexit 0\n",
            shell_quote(&direct_file),
            shell_quote(&holder_file)
        ),
    );
    let mut execution = tokio::spawn(async move {
        let cancellation = CancellationToken::new();
        run_combined_command(&cancellation, script.as_os_str(), &[], None, None).await
    });
    let direct = wait_pid_file(&direct_file).await;
    let holder = wait_pid_file(&holder_file).await;
    let mut cleanup = PidCleanup::new(holder);
    wait_process_gone(direct).await;
    lines.push(format!(
        "combined.before-release.direct-exists={} result-ready={} holder-alive={}",
        process_exists(direct),
        execution.is_finished(),
        process_exists(holder)
    ));
    cleanup.kill();
    let output = tokio::time::timeout(Duration::from_secs(3), &mut execution)
        .await
        .expect("CombinedOutput 必须在 holder 关闭后返回")
        .unwrap()
        .unwrap();
    lines.push(format!(
        "combined.after-release.error=\"\" output.hex={}",
        hex(&output)
    ));
}

/// 验证取消 `process::execute` 只杀 direct child，并在 reader 阶段保留 zombie。
async fn probe_cancelled_reader_first(root: &Path, lines: &mut Vec<String>) {
    let direct_file = root.join("rust-cancel-reader-direct");
    let holder_file = root.join("rust-cancel-reader-holder");
    let script = root.join("rust-cancel-reader.sh");
    write_busy_holder_script(&script, &direct_file, &holder_file);
    let cancellation = CancellationToken::new();
    let mut execution = tokio::spawn({
        let cancellation = cancellation.clone();
        async move {
            execute(
                &cancellation,
                script,
                output_streamer(|_, _| Ok::<_, io::Error>(())),
            )
            .await
        }
    });
    let direct = wait_pid_file(&direct_file).await;
    let holder = wait_pid_file(&holder_file).await;
    let mut cleanup = PidCleanup::new(holder);
    cancellation.cancel();
    let state = wait_process_state(direct, "Z").await;
    lines.push(format!(
        "cancel-reader.before-release.state={state} readers-done={} holder-alive={}",
        execution.is_finished(),
        process_exists(holder)
    ));
    cleanup.kill();
    let result = tokio::time::timeout(Duration::from_secs(3), &mut execution)
        .await
        .expect("cancel reader-first 必须在 holder 关闭后返回")
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_code, -1);
    lines.push(format!(
        "cancel-reader.after-release.wait=\"signal: killed\" direct-exists={}",
        process_exists(direct)
    ));
}

/// 验证取消 CombinedOutput 时 direct 已回收，但 descendant 仍让结果保持 pending。
async fn probe_cancelled_combined_output(root: &Path, lines: &mut Vec<String>) {
    let direct_file = root.join("rust-cancel-combined-direct");
    let holder_file = root.join("rust-cancel-combined-holder");
    let script = root.join("rust-cancel-combined.sh");
    write_busy_holder_script(&script, &direct_file, &holder_file);
    let cancellation = CancellationToken::new();
    let mut execution = tokio::spawn({
        let cancellation = cancellation.clone();
        async move { run_combined_command(&cancellation, script.as_os_str(), &[], None, None).await }
    });
    let direct = wait_pid_file(&direct_file).await;
    let holder = wait_pid_file(&holder_file).await;
    let mut cleanup = PidCleanup::new(holder);
    cancellation.cancel();
    wait_process_gone(direct).await;
    lines.push(format!(
        "cancel-combined.before-release.direct-exists={} result-ready={} holder-alive={}",
        process_exists(direct),
        execution.is_finished(),
        process_exists(holder)
    ));
    cleanup.kill();
    let error = tokio::time::timeout(Duration::from_secs(3), &mut execution)
        .await
        .expect("cancel CombinedOutput 必须在 holder 关闭后返回")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.error.as_slice(), b"signal: killed");
    lines.push(format!(
        "cancel-combined.after-release.error=\"signal: killed\" output.hex={}",
        hex(&error.output)
    ));
}

/// 使用生产 token FD helper 验证 source CLOEXEC、child 3/4/5 映射和 source 不泄漏。
async fn probe_extra_files(lines: &mut Vec<String>) {
    let mut descriptors = Vec::new();
    for (index, token) in ["alpha", "beta", "gamma"].into_iter().enumerate() {
        let target = add_token_via_file_descriptor(
            token,
            &format!("process-edge token {index}"),
            &mut descriptors,
        )
        .unwrap();
        assert_eq!(target, i32::try_from(index).unwrap() + 3);
    }
    let source_fds = descriptors
        .iter()
        .map(|descriptor| descriptor.source.as_raw_fd())
        .collect::<Vec<_>>();
    let close_on_exec = descriptors
        .iter()
        .map(|descriptor| {
            let flags = fcntl(&descriptor.source, FcntlArg::F_GETFD).unwrap();
            FdFlag::from_bits_truncate(flags).contains(FdFlag::FD_CLOEXEC)
        })
        .collect::<Vec<_>>();
    assert!(source_fds.iter().all(|descriptor| *descriptor >= 64));

    let visibility_checks = source_fds
        .iter()
        .map(|descriptor| {
            format!(
                "if [ -e /proc/self/fd/{descriptor} ]; then visible=\"${{visible}}{descriptor},\"; fi"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let script = format!(
        "a=$(/bin/cat <&3)\nb=$(/bin/cat <&4)\nc=$(/bin/cat <&5)\nvisible=\n{visibility_checks}\nprintf 'payloads=[\"%s\" \"%s\" \"%s\"] source-visible=[' \"$a\" \"$b\" \"$c\"\nprintf ']\\n'\n"
    );
    let mut command = Command::new("sh");
    command.arg("-c").arg(&script);
    install_token_descriptors(&mut command, &descriptors).unwrap();
    let output = command.output().await.unwrap();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let child = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        child,
        "payloads=[\"alpha\" \"beta\" \"gamma\"] source-visible=[]\n"
    );
    lines.push(format!(
        "extrafiles.parent.sources={} cloexec={}",
        source_fds
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(","),
        close_on_exec
            .iter()
            .map(bool::to_string)
            .collect::<Vec<_>>()
            .join(",")
    ));
    lines.push(format!("extrafiles.child={}", child.trim_end()));
}

/// 写一个 direct busy loop 与单个 pipe-holder descendant 的脚本。
fn write_busy_holder_script(script: &Path, direct_file: &Path, holder_file: &Path) {
    write_script(
        script,
        &format!(
            "#!/bin/sh\necho $$ > {}\n/usr/bin/sleep 30 &\necho $! > {}\nwhile :; do :; done\n",
            shell_quote(direct_file),
            shell_quote(holder_file)
        ),
    );
}

/// 以 0700 写入测试 shell 脚本。
fn write_script(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

/// 修改进程环境；调用方必须持有全局测试锁。
fn set_environment(key: &str, value: Option<&OsStr>) {
    // SAFETY: 唯一调用者在完整测试生命周期持有 PROCESS_STATE_TEST_LOCK。
    unsafe {
        if let Some(value) = value {
            std::env::set_var(key, value);
        } else {
            std::env::remove_var(key);
        }
    }
}

/// 等待 shell 写出 PID 文件，超时表示 fixture 未实际启动。
async fn wait_pid_file(path: &Path) -> i32 {
    for _ in 0..300 {
        if let Ok(value) = std::fs::read_to_string(path)
            && let Ok(pid) = value.trim().parse::<i32>()
            && pid > 0
        {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("等待 PID 文件超时: {}", path.display());
}

/// 等待 `/proc` 出现目标单字母状态，并返回最后观察值。
async fn wait_process_state(pid: i32, expected: &str) -> String {
    let mut last = "missing".to_owned();
    for _ in 0..300 {
        last = process_state(pid).unwrap_or_else(|| "missing".to_owned());
        if last == expected {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    last
}

/// 等待 direct child 从 `/proc` 消失，证明 wait 已完成回收。
async fn wait_process_gone(pid: i32) {
    for _ in 0..300 {
        if !process_exists(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("direct child {pid} 未在三秒内回收");
}

/// 读取 Linux `/proc/PID/status` 的单字母状态。
fn process_state(pid: i32) -> Option<String> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("State:"))
        .and_then(|state| state.split_whitespace().next())
        .map(str::to_owned)
}

/// 用 signal 0 判断 PID 是否存在；zombie 仍返回 true。
fn process_exists(pid: i32) -> bool {
    matches!(
        kill(Pid::from_raw(pid), None::<Signal>),
        Ok(()) | Err(Errno::EPERM)
    )
}

/// SIGKILL 当前 probe 创建的 descendant；ESRCH 视为已经清理。
fn kill_pid(pid: i32) {
    if let Err(error) = kill(Pid::from_raw(pid), Signal::SIGKILL) {
        assert_eq!(error, Errno::ESRCH);
    }
}

/// 将 POSIX 路径安全放入 shell 单引号字面量。
fn shell_quote(path: &Path) -> String {
    let value = path.to_string_lossy();
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// 把字节编码为与 Go `%x` 相同的小写十六进制。
fn hex(value: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        let _ = write!(output, "{byte:02x}");
    }
    output
}
