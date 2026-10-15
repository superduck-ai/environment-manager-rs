//! 无终端命令执行、独立进程组、后台输出和超时清理。
use super::ToolResult;
use crate::execution::{ExecutionResult, ExecutionStatus};
#[path = "bash_output.rs"]
mod output;
use crate::{environment::EnvironmentProvider, util};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
pub(super) use output::not_started;
use regex::Regex;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::SeekFrom,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    process::Command,
    sync::Mutex,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
const NON_INTERACTIVE: &[(&str, &str)] = &[
    ("TERM", "xterm-256color"),
    ("LANG", "C"),
    ("LC_ALL", "C"),
    ("DEBIAN_FRONTEND", "noninteractive"),
    ("GIT_TERMINAL_PROMPT", "0"),
    ("GCM_INTERACTIVE", "never"),
    ("npm_config_yes", "true"),
    ("PIP_NO_INPUT", "1"),
    ("HOMEBREW_NO_AUTO_UPDATE", "1"),
    ("CI", "true"),
    ("COMPOSER_NO_INTERACTION", "1"),
    ("YARN_ENABLE_IMMUTABLE_INSTALLS", "false"),
    ("GIT_MERGE_AUTOEDIT", "no"),
    ("PYTHONUNBUFFERED", "1"),
];
static RULES: OnceLock<Vec<(Regex, String, String)>> = OnceLock::new();
/// 按参考工具的简单引号规则拆分分号和双与号命令链。
fn chains(command: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let (mut single, mut double) = (false, false);
    let mut chars = command.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' && !double {
            single = !single;
        } else if c == '"' && !single {
            double = !double;
        } else if !single && !double && (c == ';' || c == '&' && chars.peek() == Some(&'&')) {
            if c == '&' {
                chars.next();
            }
            out.push(std::mem::take(&mut current));
            continue;
        }
        current.push(c);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}
/// 保留交互命令检测顺序及建议文本。
fn interactive(command: &str) -> Option<String> {
    let rules = RULES.get_or_init(|| {
        let raw: Vec<(String, String, String)> =
            serde_json::from_str(include_str!("interactive.json")).unwrap();
        raw.into_iter()
            .map(|(p, d, s)| {
                (
                    regex::RegexBuilder::new(
                        &p.replace(r"\b", r"(?-u:\b)")
                            .replace(r"\s", r"[\t\n\r\x0b\x0c ]"),
                    )
                    .unicode(true)
                    .build()
                    .unwrap(),
                    d,
                    s,
                )
            })
            .collect()
    });
    for chain in chains(command.trim()) {
        let part = chain.split('|').next().unwrap_or("").trim();
        for (p, d, s) in rules {
            if p.is_match(part) {
                return Some(format!(
                    "Command blocked: '{d}' is an interactive command that cannot run in this environment. {s}\n[interactive_blocked] suggested_non_interactive_command: {s}"
                ));
            }
        }
    }
    None
}
/// 验证调用环境对象，不对值进行隐式类型转换。
fn call_env(a: &Value) -> Result<BTreeMap<String, String>, String> {
    let Some(raw) = a.get("env").filter(|v| !v.is_null()) else {
        return Ok(BTreeMap::new());
    };
    let Some(m) = raw.as_object() else {
        return Err("env must be an object of string values".into());
    };
    let mut out = BTreeMap::new();
    for (k, v) in m {
        if k.is_empty()
            || !k
                .bytes()
                .enumerate()
                .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || i > 0 && b.is_ascii_digit())
        {
            return Err(format!("invalid env var name {}", go_quote(k)));
        }
        let Some(s) = v.as_str() else {
            return Err(format!("env var {} must be a string", go_quote(k)));
        };
        out.insert(k.clone(), s.to_owned());
    }
    Ok(out)
}
/// 将字符串格式化为兼容的双引号提示。
fn go_quote(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}
/// 创建名称为十进制数字的私有临时文件。
async fn temp(prefix: &str, suffix: &str) -> std::io::Result<(PathBuf, fs::File)> {
    for _ in 0..100 {
        let path =
            Path::new(util::OUTPUT_DIR).join(format!("{prefix}{}{suffix}", rand::random::<u32>()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .await
        {
            Ok(f) => return Ok((path, f)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
    Err(std::io::Error::other("temporary file collision"))
}
/// 从输出文件末尾读取，并跳过开头被截断的 UTF-8 续字节。
async fn tail(path: &Path, max: usize) -> std::io::Result<(String, usize)> {
    let mut f = fs::File::open(path).await?;
    let len = f.metadata().await?.len() as usize;
    let start = len.saturating_sub(max);
    f.seek(SeekFrom::Start(start as u64)).await?;
    let mut bytes = Vec::new();
    f.take((len - start) as u64).read_to_end(&mut bytes).await?;
    let skip = if start > 0 {
        bytes.iter().take_while(|b| **b & 0xc0 == 0x80).count()
    } else {
        0
    };
    Ok((crate::util::lossy(&bytes[skip..]).into_owned(), len))
}
/// Unix 信号名称使用参考进程状态文本。
fn signal_name(n: i32) -> String {
    match n {
        1 => "hangup",
        2 => "interrupt",
        3 => "quit",
        4 => "illegal instruction",
        6 => "aborted",
        9 => "killed",
        11 => "segmentation fault",
        13 => "broken pipe",
        14 => "alarm clock",
        15 => "terminated",
        _ => return format!("signal {n}"),
    }
    .into()
}
/// 将进程退出状态编码成输出文件末尾的状态行。
fn status(s: ExitStatus) -> (i32, String) {
    if let Some(n) = s.signal() {
        (-1, format!("[killed by signal {}]", signal_name(n)))
    } else {
        let c = s.code().unwrap_or(-1);
        (c, format!("[exit code: {c}]"))
    }
}
/// 向整组进程发送信号，忽略已经退出的进程组。
fn signal_group(pid: u32, signal: Signal) {
    let _ = kill(Pid::from_raw(-(pid as i32)), signal);
}
/// Run Bash while draining both pipes into independent and combined logs.
pub async fn run(
    work: &Path,
    provider: Option<&dyn EnvironmentProvider>,
    cancellation: CancellationToken,
    a: &Value,
) -> ToolResult {
    let command = util::string(a, "command");
    if command.trim().is_empty() {
        return not_started("command is required");
    }
    if let Some(e) = interactive(command) {
        return not_started(e);
    }
    let env = match call_env(a) {
        Ok(e) => e,
        Err(e) => return not_started(e),
    };
    let timeout = a
        .get("timeout")
        .and_then(util::int)
        .filter(|n| *n > 0)
        .unwrap_or(120)
        .min(600) as u64;
    let on_timeout = if let Some(v) = a.get("on_timeout") {
        match v.as_str() {
            Some("kill") => "kill",
            Some("background") => "background",
            _ => return not_started("on_timeout must be \"kill\" or \"background\""),
        }
    } else {
        "kill"
    };
    let dir = util::string(a, "work_dir");
    let work = if dir.trim().is_empty() {
        work.to_owned()
    } else {
        util::resolve(work, dir)
    };
    if cancellation.is_cancelled() {
        return not_started("not started: context canceled");
    }
    if let Err(e) = util::create_dir_all(util::OUTPUT_DIR).await {
        return not_started(format!(
            "failed to create output dir: {}",
            util::io_error("mkdir", Path::new(util::OUTPUT_DIR), &e)
        ));
    }
    let (output, out) = match temp("bash_", ".log").await {
        Ok(v) => v,
        Err(e) => return not_started(format!("failed to create output file: {e}")),
    };
    let (script, mut script_file) = match temp("bash-", ".sh").await {
        Ok(v) => v,
        Err(e) => {
            let _ = fs::remove_file(&output).await;
            return not_started(format!("failed to write command script: {e}"));
        }
    };
    let script_content = format!(
        "source \"${{QODER_VAULT_ENV_FILE:-/etc/profile.d/vault-env.sh}}\" 2>/dev/null || true\n{command}"
    );
    let written = async {
        script_file.write_all(script_content.as_bytes()).await?;
        script_file.flush().await
    }
    .await;
    if let Err(e) = written {
        let _ = fs::remove_file(&script).await;
        let _ = fs::remove_file(&output).await;
        return not_started(format!("failed to write command script: {e}"));
    }
    drop(script_file);
    let mut merged: BTreeMap<_, _> = std::env::vars_os().collect();
    if let Some(provider) = provider {
        match provider.environ(cancellation.clone()).await {
            Ok(e) => {
                for (k, v) in e {
                    if !k.is_empty() {
                        merged.insert(k.into(), v.into());
                    }
                }
            }
            Err(e) => {
                let _ = fs::remove_file(&script).await;
                let _ = fs::remove_file(&output).await;
                return not_started(format!("failed to load runtime environment: {e}"));
            }
        }
    }
    for (k, v) in NON_INTERACTIVE {
        merged.insert((*k).into(), (*v).into());
    }
    for (k, v) in env {
        merged.insert(k.into(), v.into());
    }
    use std::os::unix::ffi::OsStrExt;
    if merged
        .iter()
        .any(|(k, v)| k.as_bytes().contains(&0) || v.as_bytes().contains(&0))
    {
        let _ = fs::remove_file(&script).await;
        let _ = fs::remove_file(&output).await;
        return not_started("failed to execute command: exec: environment variable contains NUL");
    }
    let executable = match util::executable("bash").await {
        Ok(p) => p,
        Err(e) => {
            let _ = fs::remove_file(&script).await;
            let _ = fs::remove_file(&output).await;
            return not_started(format!("failed to execute command: {e}"));
        }
    };
    let streams = output::Streams::new(&output);
    let (stdout, stderr) = match streams.open().await {
        Ok(files) => files,
        Err(error) => {
            let _ = fs::remove_file(&script).await;
            let _ = fs::remove_file(&output).await;
            return not_started(format!("failed to create stream files: {error}"));
        }
    };
    if cancellation.is_cancelled() {
        streams.remove().await;
        let _ = fs::remove_file(&script).await;
        let _ = fs::remove_file(&output).await;
        return not_started("not started: context canceled");
    }
    let mut cmd = Command::new(&executable);
    cmd.arg(&script)
        .current_dir(&work)
        .env_clear()
        .envs(merged.into_iter().map(|(key, value)| {
            use std::os::unix::ffi::{OsStrExt, OsStringExt};
            if let Some(i) = key.as_bytes().iter().position(|b| *b == b'=') {
                let mut bytes = key.as_bytes()[i + 1..].to_vec();
                bytes.push(b'=');
                bytes.extend_from_slice(value.as_bytes());
                (
                    std::ffi::OsString::from_vec(key.as_bytes()[..i].to_vec()),
                    std::ffi::OsString::from_vec(bytes),
                )
            } else {
                (key, value)
            }
        }))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: pre_exec 中只调用异步信号安全的 setsid，不分配内存或使用锁。
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let started = Instant::now();
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            streams.remove().await;
            let _ = fs::remove_file(&output).await;
            let _ = fs::remove_file(&script).await;
            return not_started(format!(
                "failed to execute command: fork/exec {}: {}",
                executable.display(),
                util::errno(&e)
            ));
        }
    };
    let pid = child.id().unwrap();
    let combined = Arc::new(Mutex::new(out));
    let mut captures = JoinSet::new();
    captures.spawn(output::capture(
        child.stdout.take().unwrap(),
        stdout,
        combined.clone(),
    ));
    captures.spawn(output::capture(
        child.stderr.take().unwrap(),
        stderr,
        combined.clone(),
    ));
    let output_copy = output.clone();
    let mut finished = tokio::spawn(async move {
        let waited = child.wait().await;
        let signal = waited
            .as_ref()
            .ok()
            .and_then(|s| s.signal())
            .map(signal_name)
            .unwrap_or_default();
        let mut errors = Vec::new();
        if let Err(error) = &waited {
            errors.push(format!("wait failed: {error}"));
        }
        let (code, text) = waited.map(status).unwrap_or((-1, "[exit code: -1]".into()));
        // Like Go's WaitDelay, bound capture when descendants retain the pipes.
        if tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(captured) = captures.join_next().await {
                match captured {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => errors.push(format!("output capture failed: {error}")),
                    Err(error) => errors.push(format!("output capture task failed: {error}")),
                }
            }
        })
        .await
        .is_err()
        {
            errors.push("output pipes remained open after process exit".into());
            captures.abort_all();
            while captures.join_next().await.is_some() {}
        }
        let duration_ms = started.elapsed().as_millis() as u64;
        drop(combined);
        let _ = fs::remove_file(script).await;
        let written = async {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&output_copy)
                .await
                .map_err(|e| util::io_error("open", &output_copy, &e))?;
            file.write_all(format!("\n{text}\n").as_bytes())
                .await
                .map_err(|e| util::io_error("write", &output_copy, &e))?;
            file.flush()
                .await
                .map_err(|e| util::io_error("write", &output_copy, &e))
        }
        .await;
        if let Err(error) = written {
            crate::logging::warn(
                "bash: failed to write exit status line",
                &[
                    ("pid", &pid.to_string()),
                    ("output_file", &output_copy.to_string_lossy()),
                    ("error", &error),
                ],
            );
        }
        (code, text, signal, duration_ms, errors.join("; "))
    });
    let mut reason = String::new();
    let mut result = None;
    let mut execution_status = ExecutionStatus::Completed;
    tokio::select! {
        v = &mut finished => { result = Some(v); },
        _ = cancellation.cancelled() => {
            reason = "[cancelled; process group terminated]".into();
            execution_status = ExecutionStatus::Cancelled;
        },
        _ = tokio::time::sleep(Duration::from_secs(timeout)) => {
            if on_timeout == "background" {
                crate::logging::info("bash: command continues in background", &[("pid", &pid.to_string()), ("output_file", &output.to_string_lossy()), ("waited_seconds", &timeout.to_string())]);
                let preview = tail(&output, 2000).await.map(|(text, _)| if text.is_empty() { "(no output yet)".into() } else { text }).unwrap_or_else(|e| format!("(output not readable: {e})"));
                let mut execution = ExecutionResult {
                    status: ExecutionStatus::Running, pid: Some(pid), duration_ms: started.elapsed().as_millis() as u64,
                    output_file: output.to_string_lossy().into_owned(), ..Default::default()
                };
                streams.snapshot(&mut execution, 2000).await;
                return output::result(format!("[backgrounded] Command did not finish within {timeout}s and continues running in the background.\npid: {pid}\noutput_file: {}\n\nOutput so far:\n{preview}", output.display()), !execution.error.is_empty(), execution);
            }
            reason = format!("[timed out after {timeout}s; process group terminated]");
            execution_status = ExecutionStatus::TimedOut;
        }
    }
    if result.is_none() {
        crate::logging::info(
            "bash: terminating command",
            &[
                (
                    "reason",
                    if cancellation.is_cancelled() {
                        "cancelled"
                    } else {
                        "timeout"
                    },
                ),
                ("pid", &pid.to_string()),
                ("output_file", &output.to_string_lossy()),
            ],
        );
        signal_group(pid, Signal::SIGTERM);
        let wait = tokio::time::timeout(Duration::from_secs(2), &mut finished).await;
        signal_group(pid, Signal::SIGKILL);
        result = Some(match wait {
            Ok(v) => v,
            Err(_) => finished.await,
        });
    }
    let (code, status, signal, duration_ms, wait_error) = result.unwrap().unwrap_or_else(|error| {
        (
            -1,
            "[exit code: -1]".into(),
            String::new(),
            started.elapsed().as_millis() as u64,
            error.to_string(),
        )
    });
    let mut execution = ExecutionResult {
        status: execution_status,
        pid: Some(pid),
        exit_code: Some(code),
        signal,
        duration_ms,
        timed_out: execution_status == ExecutionStatus::TimedOut,
        cancelled: execution_status == ExecutionStatus::Cancelled,
        error: wait_error,
        ..Default::default()
    };
    streams.snapshot(&mut execution, 30000).await;
    let (tail, total) = match tail(&output, 30000).await {
        Ok(v) => v,
        Err(e) => {
            crate::logging::warn(
                "bash: failed to read command output",
                &[
                    ("pid", &pid.to_string()),
                    ("output_file", &output.to_string_lossy()),
                    ("error", &util::io_error("open", &output, &e)),
                ],
            );
            let message = format!(
                "command finished ({status}) but its output could not be read from {}: {}",
                output.display(),
                util::io_error("open", &output, &e)
            );
            execution.error = message.clone();
            execution.output_file = output.to_string_lossy().into_owned();
            return output::result(message, true, execution);
        }
    };
    let tail_len = tail.len();
    let mut text = tail
        .strip_suffix(&format!("\n{status}\n"))
        .unwrap_or(&tail)
        .to_owned();
    if !reason.is_empty() {
        text.push_str(&format!("\n{reason}"));
    } else if code != 0 {
        text.push_str(&format!("\n{status}"));
    }
    if total > 30000 {
        execution.output_file = output.to_string_lossy().into_owned();
        text = format!(
            "[output truncated: showing the last {tail_len} of {total} bytes; full output in {}]\n{text}",
            output.display()
        );
    } else {
        let _ = fs::remove_file(&output).await;
    }
    if text.is_empty() {
        text = "(no output)".into();
    }
    output::result(
        text,
        code != 0 || !reason.is_empty() || !execution.error.is_empty(),
        execution,
    )
}
