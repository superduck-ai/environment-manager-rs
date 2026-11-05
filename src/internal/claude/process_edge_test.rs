//! Linux ARM64 低层进程语义回归测试。
//!
//! 覆盖原始输出、取消/reap/pipe 和继承文件描述符。
//! 这些曾由外部辅助程序验证的场景现在直接作为普通 Rust 测试执行。

use std::{
    ffi::{OsStr, OsString},
    io,
    os::{fd::AsRawFd as _, unix::fs::PermissionsExt as _},
    path::Path,
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

use crate::internal::{process::execute, util::output_streamer};

use super::{
    claude_code_executor::{add_token_via_file_descriptor, install_token_descriptors},
    types::run_combined_command,
};

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

    probe_raw_combined_output(&mut observations).await;
    probe_streaming_reap(temp.path(), &mut observations).await;
    probe_combined_output_reap(temp.path(), &mut observations).await;
    probe_cancelled_streaming_reap(temp.path(), &mut observations).await;
    probe_cancelled_combined_output(temp.path(), &mut observations).await;
    probe_extra_files(&mut observations).await;

    assert_eq!(observations.len(), 13, "所有进程边界场景都必须完成");
}

/// 捕获非法 UTF-8/NUL 输出，并验证错误包装不会改写原始字节。
async fn probe_raw_combined_output(lines: &mut Vec<String>) {
    let cancellation = CancellationToken::new();
    let output = run_combined_command(
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
    .unwrap();
    assert!(!output.status.success());
    let error = output.status.to_string().into_bytes();
    let output = output.output.expect_combined();
    let mut wrapped = b"npm install failed: ".to_vec();
    wrapped.extend_from_slice(&error);
    wrapped.extend_from_slice(b", output: ");
    wrapped.extend_from_slice(&output);
    lines.push(format!("combined.output.hex={}", hex(&output)));
    lines.push(format!("combined.error.bytes={}", hex(&error)));
    lines.push(format!("combined.wrapped.bytes={}", hex(&wrapped)));
}

/// 验证流式执行器先回收 direct child，再等待 descendant pipe EOF。
async fn probe_streaming_reap(root: &Path, lines: &mut Vec<String>) {
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
    wait_process_gone(direct).await;
    lines.push(format!(
        "streaming.before-release.direct-exists={} result-ready={} holder-alive={}",
        process_exists(direct),
        execution.is_finished(),
        process_exists(holder)
    ));
    cleanup.kill();
    let result = tokio::time::timeout(Duration::from_secs(3), &mut execution)
        .await
        .expect("streaming execute 必须在 holder 关闭后返回")
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_code, 0);
    lines.push(format!(
        "streaming.after-release.wait=\"\" direct-exists={}",
        process_exists(direct)
    ));
}

/// 验证合并捕获先回收 direct child，再等待 descendant pipe EOF。
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
        .expect("合并捕获必须在 holder 关闭后返回")
        .unwrap()
        .unwrap();
    let output = output.output.expect_combined();
    lines.push(format!(
        "combined.after-release.error=\"\" output.hex={}",
        hex(&output)
    ));
}

/// 验证取消流式执行时先回收 direct child，再等待 descendant pipe EOF。
async fn probe_cancelled_streaming_reap(root: &Path, lines: &mut Vec<String>) {
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
    wait_process_gone(direct).await;
    lines.push(format!(
        "cancel-streaming.before-release.direct-exists={} result-ready={} holder-alive={}",
        process_exists(direct),
        execution.is_finished(),
        process_exists(holder)
    ));
    cleanup.kill();
    let result = tokio::time::timeout(Duration::from_secs(3), &mut execution)
        .await
        .expect("取消后的流式执行必须在 holder 关闭后返回")
        .unwrap()
        .unwrap();
    assert_eq!(result.exit_code, -1);
    lines.push(format!(
        "cancel-streaming.after-release.direct-exists={}",
        process_exists(direct)
    ));
}

/// 验证取消合并捕获时 direct 已回收，但 descendant 仍让结果保持 pending。
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
        .expect("取消后的合并捕获必须在 holder 关闭后返回")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.to_string(), "command was cancelled");
    let output = error.output.expect_combined();
    lines.push(format!(
        "cancel-combined.after-release.error=\"command was cancelled\" output.hex={}",
        hex(&output)
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

/// 用 signal 0 判断 PID 是否存在。
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

/// 把字节编码为与 小写十六进制 相同的小写十六进制。
fn hex(value: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        let _ = write!(output, "{byte:02x}");
    }
    output
}
