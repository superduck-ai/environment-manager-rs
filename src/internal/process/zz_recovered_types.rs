//! 进程执行结果与错误类型。
//!
//! 本文件机械对应 Go `internal/process/zz_recovered_types.go`。Go 恢复类型名为
//! `Result`；Rust 同时保留该精确名称和迁移早期使用的 `ProcessResult` 别名，避免与
//! 标准库 `Result<T, E>` 混淆时仍可逐项对照。

use std::{
    ffi::{OsStr, OsString},
    io,
    os::unix::{
        ffi::{OsStrExt as _, OsStringExt as _},
        fs::PermissionsExt as _,
    },
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use rustix::fs::{Access, AtFlags, CWD, accessat};

use thiserror::Error;

/// 在创建或启动子进程阶段发生的错误。
#[derive(Debug, Error)]
pub enum ProcessError {
    /// 空路径无法被操作系统执行。
    #[error("path is empty")]
    EmptyPath,
    /// 调用开始前 context 已取消；与 Go `CommandContext.Start` 一样不创建进程。
    #[error("failed to start command: context canceled")]
    Cancelled,
    /// 创建进程或管道失败。
    #[error("failed to start command: {source}")]
    Start {
        /// 目标可执行文件；仅供诊断字段使用，不进入 Go 兼容错误文本。
        path: PathBuf,
        /// 已按 Go `exec.Error` 或 `PathError` 规则编码的原始错误字节。
        #[source]
        source: GoCommandError,
    },
}

impl ProcessError {
    /// 返回可直接写入外部 stderr 的 Go 兼容原始错误字节。
    ///
    /// `Display` 必须生成 UTF-8，无法保留显式 executable 路径中的非法字节；CLI
    /// 兼容层应调用本方法并使用 `write_all`。返回拥有值是因为 `Start` 需要在底层
    /// `fork/exec ...` 文本前补上 Go `process.Execute` 的固定上下文。
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::EmptyPath => b"path is empty".to_vec(),
            Self::Cancelled => b"failed to start command: context canceled".to_vec(),
            Self::Start { source, .. } => {
                let mut message = b"failed to start command: ".to_vec();
                message.extend_from_slice(source.as_bytes());
                message
            }
        }
    }
}

/// Go `exec.Error`/`os.PathError` 可携带任意字节的错误值。
///
/// Rust `io::Error` 的自定义消息只能是 UTF-8 `String`；若 executable 路径包含非法
/// UTF-8，先构造 `io::Error::other` 会永久丢失原始字节。因此兼容层使用专用类型，
/// 到日志/`Display` 时才生成逐坏字节替换的可读视图。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoCommandError {
    bytes: Vec<u8>,
}

impl GoCommandError {
    /// 从已经按 Go Linux 规则拼接好的字节创建命令错误。
    fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// 借用原始错误字节，供上层继续添加上下文而不经过 UTF-8。
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// 消费错误并无复制取得原始字节。
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::fmt::Display for GoCommandError {
    /// 为日志和通用 Error 接口提供 Go JSON 风格的 UTF-8 替换视图。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&go_utf8_error_view(&self.bytes))
    }
}

impl std::error::Error for GoCommandError {}

/// 子进程已经启动后得到的失败状态。
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProcessExitError {
    /// 子进程正常被 wait，但退出码非零；被 Unix 信号终止时退出码为 `-1`。
    #[error("process exited with code {0}")]
    ExitCode(i32),
    /// 操作系统无法完成 wait。
    #[error("process failed: {0}")]
    Wait(String),
}

/// 对应 Go `internal/process/zz_recovered_types.go` 中的 `Result`。
///
/// 保存一次进程执行的退出状态和时间信息。非零退出不会让 `execute` 本身返回错误，
/// 而是写入 `error`，以保留 Go 调用方区分“未能启动”和“已运行但失败”的方式。
#[derive(Debug, Clone)]
pub struct Result {
    /// 成功为 0，显式非零退出为对应码，Unix 信号终止或 wait 失败为 -1。
    pub exit_code: i32,
    /// 调用 spawn 前记录的墙钟时间。
    pub start_time: SystemTime,
    /// wait 完成后的墙钟时间。
    pub end_time: SystemTime,
    /// 单调时钟计算出的耗时，不受系统时间调整影响。
    pub duration: Duration,
    /// 非成功退出的详细原因。
    pub error: Option<ProcessExitError>,
}

/// 迁移期用于避免和 `std::result::Result` 混淆的兼容别名。
pub type ProcessResult = Result;

/// 按 Go 1.25 `exec.LookPath` 的 Linux 规则解析 executable。
///
/// # 参数
/// - `program`：调用方传给 `exec.Command` 的原始程序名，可包含非 UTF-8 字节。
///
/// # 返回
/// 名称包含 `/` 时原样返回，让 `execve` 在 child 中处理；纯文件名则使用父进程
/// 当前 `PATH` 搜索。未找到、命中相对 PATH（ErrDot）或名称无效时返回已经带
/// `exec: "...":` 前缀的 Go 兼容错误。
///
/// # 兼容性
/// `cmd.Env` 不参与查找：Go 在 `exec.Command` 构造时就使用父进程 PATH，之后才由
/// 调用方设置 `Cmd.Env`。`GODEBUG` 最后一个 `execerrdot=0` 可恢复旧的相对路径行为。
pub(crate) fn resolve_go_command(program: &OsStr) -> std::result::Result<OsString, GoCommandError> {
    let name = program.as_bytes();
    if name.contains(&b'/') {
        return Ok(program.to_os_string());
    }
    if name.is_empty() || name == b"." || name == b".." {
        return Err(go_look_path_error(
            name,
            "executable file not found in $PATH",
        ));
    }

    let search_path = std::env::var_os("PATH").unwrap_or_default();
    let search_path = search_path.as_bytes();
    // Go `filepath.SplitList("")` 返回空列表；只有 `:` 中实际存在的空元素才表示 `.`。
    if !search_path.is_empty() {
        for directory in search_path.split(|byte| *byte == b':') {
            let directory = if directory.is_empty() {
                b".".as_slice()
            } else {
                directory
            };
            let candidate = go_filepath_join(directory, name);
            if !is_go_executable(Path::new(OsStr::from_bytes(&candidate))) {
                continue;
            }
            if candidate.first() != Some(&b'/') && go_execerrdot_enabled() {
                return Err(go_look_path_error(
                    name,
                    "cannot run executable found relative to current directory",
                ));
            }
            return Ok(OsString::from_vec(candidate));
        }
    }

    Err(go_look_path_error(
        name,
        "executable file not found in $PATH",
    ))
}

/// 将 `spawn` 的 errno 包装成 Go `os.StartProcess` 的 `PathError` 文本。
///
/// `resolved_program` 必须是 `resolve_go_command` 的成功结果；因此无 slash 的程序名
/// 已经替换为 PATH 中的真实候选。Linux Go 使用 `fork/exec PATH: errno`，而 Rust
/// 标准库默认只返回 `Permission denied (os error 13)`，不能直接暴露给兼容边界。
pub(crate) fn go_spawn_error(resolved_program: &OsStr, source: &io::Error) -> GoCommandError {
    let mut message = b"fork/exec ".to_vec();
    // Go `PathError.Path` 是 string，允许包含任意非 NUL 字节；这里必须原样拼接，
    // 不能使用 Path::display 或 from_utf8_lossy，否则 CLI 差分会在路径处失真。
    message.extend_from_slice(resolved_program.as_bytes());
    message.extend_from_slice(b": ");
    message.extend_from_slice(go_errno_text(source).as_bytes());
    GoCommandError::new(message)
}

/// 判断候选是否是 Go `findExecutable` 接受的普通可执行路径。
///
/// 先 `stat` 并排除目录，再用 effective UID/GID 执行 X_OK 检查；这对 ACL、容器内
/// 身份切换以及 root 的执行权限都比只看 `mode & 0111` 更接近 Go `unix.Eaccess`。
fn is_go_executable(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if metadata.is_dir() {
        return false;
    }
    match accessat(CWD, path, Access::EXEC_OK, AtFlags::EACCESS) {
        Ok(()) => true,
        // Go 在旧内核或 seccomp 拒绝 eaccess 时同样回退到任一 executable mode bit。
        Err(error) if error == rustix::io::Errno::NOSYS || error == rustix::io::Errno::PERM => {
            metadata.permissions().mode() & 0o111 != 0
        }
        Err(_) => false,
    }
}

/// 复刻 Linux `filepath.Join(dir, file)` 在 PATH 搜索中的词法清理。
///
/// `file` 是无 slash 的 basename；仍实现完整的 `.`、`..` 和重复分隔符处理，因为
/// PATH directory 本身可以包含这些片段，且 Go 会在 `stat` 前调用 `filepath.Join`。
fn go_filepath_join(directory: &[u8], file: &[u8]) -> Vec<u8> {
    let mut joined = Vec::with_capacity(directory.len() + file.len() + 1);
    joined.extend_from_slice(directory);
    if !joined.ends_with(b"/") {
        joined.push(b'/');
    }
    joined.extend_from_slice(file);

    let absolute = joined.starts_with(b"/");
    let mut components: Vec<&[u8]> = Vec::new();
    for component in joined.split(|byte| *byte == b'/') {
        if component.is_empty() || component == b"." {
            continue;
        }
        if component == b".." {
            if components.last().is_some_and(|last| *last != b"..") {
                components.pop();
            } else if !absolute {
                components.push(component);
            }
            continue;
        }
        components.push(component);
    }

    let mut cleaned = Vec::new();
    if absolute {
        cleaned.push(b'/');
    }
    for (index, component) in components.iter().enumerate() {
        if index > 0 {
            cleaned.push(b'/');
        }
        cleaned.extend_from_slice(component);
    }
    if cleaned.is_empty() {
        cleaned.extend_from_slice(if absolute { b"/" } else { b"." });
    }
    cleaned
}

/// 读取 `GODEBUG` 中最后一个 `execerrdot` 设置。
///
/// Go 从右向左解析同名项，只有值严格为 `0` 才禁用 ErrDot。`#bisect-pattern` 是
/// Go 调试器专用能力，迁移程序没有 Go 调用栈可匹配，因此普通值部分仍按默认启用。
fn go_execerrdot_enabled() -> bool {
    let Some(settings) = std::env::var_os("GODEBUG") else {
        return true;
    };
    settings
        .as_bytes()
        .rsplit(|byte| *byte == b',')
        .find_map(|setting| setting.strip_prefix(b"execerrdot="))
        .is_none_or(|value| value.split(|byte| *byte == b'#').next() != Some(b"0"))
}

/// 生成 Go `exec.Error.Error()` 的带引号文本。
fn go_look_path_error(name: &[u8], reason: &str) -> GoCommandError {
    GoCommandError::new(format!("exec: {}: {reason}", go_quote(name)).into_bytes())
}

/// 以 `strconv.Quote` 的常见 Linux executable 名称规则转义任意字节。
///
/// 非 UTF-8 字节必须写成 `\\xNN`，否则错误自身会先发生替换，无法验证 Go 的
/// 原始名称诊断。有效、可打印的 Unicode 则保持原字符。
fn go_quote(value: &[u8]) -> String {
    let mut quoted = String::from("\"");
    let mut cursor = 0;
    while cursor < value.len() {
        match std::str::from_utf8(&value[cursor..]) {
            Ok(valid) => {
                for character in valid.chars() {
                    push_go_quoted_character(&mut quoted, character);
                }
                break;
            }
            Err(error) => {
                let valid_end = cursor + error.valid_up_to();
                // `valid_up_to` 指向 UTF-8 字符边界；该切片因此一定能安全解码。
                if let Ok(valid) = std::str::from_utf8(&value[cursor..valid_end]) {
                    for character in valid.chars() {
                        push_go_quoted_character(&mut quoted, character);
                    }
                }
                if valid_end < value.len() {
                    use std::fmt::Write as _;
                    let _ = write!(quoted, "\\x{:02x}", value[valid_end]);
                    cursor = valid_end + 1;
                } else {
                    break;
                }
            }
        }
    }
    quoted.push('"');
    quoted
}

/// 将单个有效 Unicode 字符追加为 Go 双引号字符串内容。
fn push_go_quoted_character(output: &mut String, character: char) {
    use std::fmt::Write as _;
    match character {
        '\\' => output.push_str("\\\\"),
        '"' => output.push_str("\\\""),
        '\u{0007}' => output.push_str("\\a"),
        '\u{0008}' => output.push_str("\\b"),
        '\u{000c}' => output.push_str("\\f"),
        '\n' => output.push_str("\\n"),
        '\r' => output.push_str("\\r"),
        '\t' => output.push_str("\\t"),
        '\u{000b}' => output.push_str("\\v"),
        character if character.is_ascii_control() => {
            let _ = write!(output, "\\x{:02x}", u32::from(character));
        }
        character if character.is_control() && u32::from(character) <= 0xffff => {
            let _ = write!(output, "\\u{:04x}", u32::from(character));
        }
        character if character.is_control() => {
            let _ = write!(output, "\\U{:08x}", u32::from(character));
        }
        character => output.push(character),
    }
}

/// 把 Rust I/O errno 转成 Go Linux `syscall.Errno.Error()` 文本。
///
/// exec、pipe close 和临时脚本清理实际只会产生这个集合中的少量 errno；保留完整
/// 常见表可避免 Rust `io::Error` 附加 `(os error N)` 或使用不同措辞。
pub(super) fn go_errno_text(error: &io::Error) -> String {
    match error.raw_os_error() {
        Some(libc::EPERM) => "operation not permitted".to_owned(),
        Some(libc::ENOENT) => "no such file or directory".to_owned(),
        Some(libc::EINTR) => "interrupted system call".to_owned(),
        Some(libc::EIO) => "input/output error".to_owned(),
        Some(libc::E2BIG) => "argument list too long".to_owned(),
        Some(libc::ENOEXEC) => "exec format error".to_owned(),
        Some(libc::EBADF) => "bad file descriptor".to_owned(),
        Some(libc::EAGAIN) => "resource temporarily unavailable".to_owned(),
        Some(libc::ENOMEM) => "cannot allocate memory".to_owned(),
        Some(libc::EACCES) => "permission denied".to_owned(),
        Some(libc::EFAULT) => "bad address".to_owned(),
        Some(libc::ENOTDIR) => "not a directory".to_owned(),
        Some(libc::EISDIR) => "is a directory".to_owned(),
        Some(libc::EINVAL) => "invalid argument".to_owned(),
        Some(libc::ENFILE) => "too many open files in system".to_owned(),
        Some(libc::EMFILE) => "too many open files".to_owned(),
        Some(libc::ETXTBSY) => "text file busy".to_owned(),
        Some(libc::ENAMETOOLONG) => "file name too long".to_owned(),
        Some(libc::ELOOP) => "too many levels of symbolic links".to_owned(),
        Some(number) => format!("errno {number}"),
        None => error.to_string(),
    }
}

/// 将每个非法 UTF-8 字节分别替换为 U+FFFD，仅供 `Display`/结构化日志使用。
///
/// Go `encoding/json` 以 `utf8.DecodeRune` 前进，坏序列中的每个 size=1 字节都会产生
/// 一个替换字符；Rust `from_utf8_lossy` 可能合并坏字节段，不能用于兼容日志。
pub(super) fn go_utf8_error_view(value: &[u8]) -> String {
    let mut output = String::new();
    let mut remaining = value;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                output.push_str(valid);
                break;
            }
            Err(error) => {
                let valid = &remaining[..error.valid_up_to()];
                output.push_str(std::str::from_utf8(valid).unwrap_or_default());
                output.push('\u{fffd}');
                remaining = &remaining[error.valid_up_to() + 1..];
            }
        }
    }
    output
}
