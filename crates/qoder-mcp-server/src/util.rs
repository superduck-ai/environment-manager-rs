//! 文件路径、系统错误和输出持久化的兼容辅助函数。
use serde_json::Value;
use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const OUTPUT_DIR: &str = "/tmp/tool-outputs";

/// 按 JSON 数字的浮点语义截断整数参数。
pub fn int(v: &Value) -> Option<i64> {
    v.as_f64().map(|n| {
        #[cfg(target_arch = "aarch64")]
        {
            n as i64
        }
        #[cfg(not(target_arch = "aarch64"))]
        {
            if n >= i64::MAX as f64 || n < i64::MIN as f64 {
                i64::MIN
            } else {
                n as i64
            }
        }
    })
}
/// 获取字符串；其他 JSON 类型保持空字符串回退。
pub fn string<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}
/// 按工具兼容规则解释布尔参数。
pub fn boolean(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::String(s) => s == "true" || s == "1",
        Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0),
        _ => false,
    }
}
/// 归一化路径中的点和父目录，不访问文件系统。
pub fn clean(p: impl AsRef<Path>) -> PathBuf {
    let p = p.as_ref();
    let absolute = p.is_absolute();
    let mut parts = Vec::new();
    for c in p.components() {
        use std::path::Component::*;
        match c {
            RootDir => {}
            CurDir => {}
            ParentDir => {
                if parts
                    .last()
                    .is_some_and(|s| *s != std::ffi::OsStr::new(".."))
                {
                    parts.pop();
                } else if !absolute {
                    parts.push(std::ffi::OsStr::new(".."));
                }
            }
            Normal(s) => parts.push(s),
            Prefix(_) => {}
        }
    }
    let mut out = if absolute {
        PathBuf::from("/")
    } else {
        PathBuf::new()
    };
    for c in parts {
        out.push(c);
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    out
}
/// 相对参数使用默认工作目录；绝对参数保留输入显示形式。
pub fn resolve(work: &Path, p: &str) -> PathBuf {
    if p.is_empty() {
        work.to_owned()
    } else if Path::new(p).is_absolute() {
        PathBuf::from(p)
    } else {
        clean(work.join(p))
    }
}
/// 求两个路径的词法相对路径，与文件是否存在无关。
pub fn relative(base: &Path, path: &Path) -> Option<String> {
    if base.is_absolute() != path.is_absolute() {
        return None;
    }
    let b = clean(base);
    let p = clean(path);
    let b: Vec<_> = b.components().collect();
    let p: Vec<_> = p.components().collect();
    let common = b.iter().zip(&p).take_while(|(a, b)| a == b).count();
    let mut out = PathBuf::new();
    for _ in common..b.len() {
        out.push("..");
    }
    for c in &p[common..] {
        out.push(c.as_os_str());
    }
    Some(if out.as_os_str().is_empty() {
        ".".into()
    } else {
        out.to_string_lossy().into_owned()
    })
}
/// 保留可观察的 Unix 错误用词，避免 Rust 自动追加 errno 编号。
pub fn errno(e: &io::Error) -> String {
    match e.raw_os_error() {
        Some(libc::ENOENT) => "no such file or directory".into(),
        Some(libc::EACCES) | Some(libc::EPERM) => "permission denied".into(),
        Some(libc::EISDIR) => "is a directory".into(),
        Some(libc::EEXIST) => "file exists".into(),
        Some(libc::ENOTDIR) => "not a directory".into(),
        Some(libc::E2BIG) => "argument list too long".into(),
        Some(libc::EINVAL) => "invalid argument".into(),
        Some(libc::ENAMETOOLONG) => "file name too long".into(),
        None if e.kind() == io::ErrorKind::InvalidInput => "invalid argument".into(),
        Some(libc::ENOSPC) => "no space left on device".into(),
        Some(libc::ELOOP) => "too many levels of symbolic links".into(),
        _ => e.to_string(),
    }
}
/// 构造文件操作错误的对外文本。
pub fn io_error(op: &str, p: &Path, e: &io::Error) -> String {
    format!("{op} {}: {}", p.display(), errno(e))
}
/// 输出文件名称使用纳秒时间戳。
pub fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
/// 用十进制一位小数格式化文件大小。
pub fn size(n: usize) -> String {
    if n < 1024 {
        return format!("{n} bytes");
    }
    let (d, u) = if n < 1024 * 1024 {
        (1024.0, "KB")
    } else if n < 1024 * 1024 * 1024 {
        (1048576.0, "MB")
    } else {
        (1073741824.0, "GB")
    };
    format!(
        "{} {u}",
        format!("{:.1}", n as f64 / d).trim_end_matches(".0")
    )
}
/// 超阈值时保留读取分页提示，或将完整输出写入文件。
pub fn truncate(text: String, tool: &str) -> String {
    let threshold = match tool {
        "bash" | "run_in_terminal" => 30000,
        "grep" | "grep_run" => 20000,
        _ => 50000,
    };
    if text.chars().count() <= threshold {
        return text;
    }
    if matches!(tool, "read" | "file_read") {
        return format!(
            "{}\n\n[File content truncated ({}). Use the read tool with start_line and end_line parameters to read the remaining content. If a single line is too long for the read tool to handle, use the bash tool to extract content as needed.]",
            text.chars().take(threshold).collect::<String>(),
            size(text.len())
        );
    }
    let path = format!("{OUTPUT_DIR}/{tool}_{}.txt", timestamp());
    if fs::create_dir_all(OUTPUT_DIR)
        .and_then(|_| fs::write(&path, text.as_bytes()))
        .is_err()
    {
        return text;
    }
    let mut preview: String = text.chars().take(2000).collect();
    if let Some(i) = preview.rfind('\n')
        && i > 1000
    {
        preview = text.chars().take(i).collect();
    }
    format!(
        "<persisted-output>\nOutput too large ({}). Full output saved to: {path}\n\nPreview (first {}):\n{preview}\n...\n</persisted-output>",
        size(text.len()),
        size(2000)
    )
}

/// 使用服务器进程的 PATH 定位可执行文件，不受工具调用环境覆盖影响。
pub fn executable(name: &str) -> Result<PathBuf, String> {
    use std::os::unix::fs::PermissionsExt;
    for dir in std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()) {
        let p = dir.join(name);
        if fs::metadata(&p).is_ok_and(|m| !m.is_dir() && m.permissions().mode() & 0o111 != 0) {
            if !p.is_absolute() {
                return Err(format!(
                    "exec: {name:?}: cannot run executable found relative to current directory"
                ));
            }
            return Ok(p);
        }
    }
    Err(format!(
        "exec: {name:?}: executable file not found in $PATH"
    ))
}

/// 创建目录树，现有普通文件使用参考接口的 ENOTDIR 文本。
pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(path).map_err(|e| {
        if e.raw_os_error() == Some(libc::EEXIST) {
            io::Error::from_raw_os_error(libc::ENOTDIR)
        } else {
            e
        }
    })
}
/// 写入文件并保留错误发生在打开或写入阶段的信息。
pub fn write_file(path: &Path, bytes: &[u8]) -> Result<(), (io::Error, &'static str)> {
    use io::Write;
    let mut file = fs::File::create(path).map_err(|e| (e, "open"))?;
    file.write_all(bytes).map_err(|e| (e, "write"))
}

/// 按参考运行时逐个无效字节替换 UTF-8，完整字符串无需分配。
pub fn lossy(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    if let Ok(text) = std::str::from_utf8(bytes) {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut output = String::with_capacity(bytes.len());
    let mut remaining = bytes;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(text) => {
                output.push_str(text);
                break;
            }
            Err(error) => {
                let valid = error.valid_up_to();
                output.push_str(
                    std::str::from_utf8(&remaining[..valid]).expect("valid UTF-8 prefix"),
                );
                output.push('\u{fffd}');
                remaining = &remaining[valid + 1..];
            }
        }
    }
    std::borrow::Cow::Owned(output)
}
