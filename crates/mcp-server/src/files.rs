//! 文件读取、原子写入和唯一文本替换。
use super::{ToolResult, diff, images};
use crate::util;
use serde_json::Value;
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

/// 读取文本范围或图片，并保留文件路径检查的顺序。
pub async fn read(a: &Value) -> ToolResult {
    let p = util::string(a, "file_path");
    if p.is_empty() {
        return ToolResult::error("file_path is required");
    }
    if !Path::new(p).is_absolute() {
        return ToolResult::error(format!(
            "Read file {p} failed, the path is not absolute path."
        ));
    }
    if p.starts_with("\\\\") || p.starts_with("//") {
        return ToolResult::error("Reading UNC/network paths is not allowed.");
    }
    if [
        "/dev/zero",
        "/dev/random",
        "/dev/urandom",
        "/dev/stdin",
        "/dev/null",
    ]
    .contains(&p)
    {
        return ToolResult::error(format!(
            "Reading {p} is not allowed: this is a dangerous special device."
        ));
    }
    let path = Path::new(p);
    if ["png", "jpg", "jpeg", "gif", "webp"].contains(
        &path
            .extension()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase()
            .as_str(),
    ) {
        return images::read(path).await;
    }
    let info = match fs::metadata(path).await {
        Ok(info) => info,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut message = format!("File not found: {p}");
            if let Some(s) = similar(path).await {
                message.push_str(&format!("\nDid you mean: {}?", s.display()));
            }
            return ToolResult::error(message);
        }
        Err(e) => {
            return ToolResult::error(format!(
                "failed to stat file: {}",
                util::io_error("stat", path, &e)
            ));
        }
    };
    if info.is_dir() {
        return ToolResult::error(format!(
            "{p} is a directory, not a file. Use Glob to list directory contents."
        ));
    }
    if info.len() > 10 << 20 {
        return ToolResult::error(format!(
            "File {p} is too large ({} bytes, limit {} bytes). Use start_line/end_line parameters to read a portion of the file.",
            info.len(),
            10 << 20
        ));
    }
    let mut file = match fs::File::open(path).await {
        Ok(f) => f,
        Err(e) => {
            return ToolResult::error(format!(
                "failed to detect file type: {}",
                util::io_error("open", path, &e)
            ));
        }
    };
    let mut sample = [0; 8192];
    let n = file.read(&mut sample).await.unwrap_or(0);
    if sample[..n].contains(&0) {
        return ToolResult::error(format!(
            "Cannot read {p}: binary file detected (contains null bytes). Use Bash to inspect binary files if needed (e.g., xxd, hexdump, strings)."
        ));
    }
    let data = match fs::read(path).await {
        Ok(d) => d,
        Err(e) => {
            return ToolResult::error(format!(
                "failed to read file: {}",
                util::io_error("open", path, &e)
            ));
        }
    };
    if data.is_empty() {
        return ToolResult::text("");
    }
    let content = crate::util::lossy(&data);
    let lines: Vec<_> = content.split_terminator('\n').collect();
    let start = a.get("start_line").and_then(util::int);
    let end = a.get("end_line").and_then(util::int);
    let mut s = start.unwrap_or(0).max(0);
    let mut e = end.unwrap_or_else(|| s.saturating_add(2000).min(lines.len() as i64));
    if start.is_some() || end.is_some() {
        s = s.max(0);
        if s >= lines.len() as i64 {
            return ToolResult::error(format!(
                "Read file {p} failed: start_line {s} is beyond file length ({} lines).",
                lines.len()
            ));
        }
        e = e.min(lines.len() as i64);
        if e <= s {
            return ToolResult::error(format!(
                "Read file {p} failed: end_line {e} must be greater than start_line {s}."
            ));
        }
    }
    let mut output = lines[s as usize..e as usize]
        .iter()
        .enumerate()
        .map(|(i, line)| format!("{}\t{line}", i + s as usize + 1))
        .collect::<Vec<_>>()
        .join("\n");
    if end.is_none() && (e as usize) < lines.len() {
        output.push_str("\n\n[Read output limited to 2000 lines. Use start_line/end_line to read the remaining content.]");
    }
    ToolResult::text(util::truncate(output, "read").await)
}
/// 根据文件名评分给出拼写建议，平分时保持目录排序。
async fn similar(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    let base = path.file_name()?.to_string_lossy().to_lowercase();
    let mut directory = fs::read_dir(parent).await.ok()?;
    let mut entries = Vec::new();
    while let Some(entry) = directory.next_entry().await.ok()? {
        entries.push(entry);
    }
    entries.sort_by_key(|e| e.file_name());
    let mut best = None;
    let mut best_score = 2;
    for e in entries {
        if e.file_type().await.ok()?.is_dir() {
            continue;
        }
        let name = e.file_name().to_string_lossy().into_owned();
        let lower = name.to_lowercase();
        let prefix = &base.as_bytes()[..3.min(base.len())];
        let mut score = 0;
        if lower.as_bytes().starts_with(prefix) {
            score += 3;
        }
        if Path::new(&lower).extension() == Path::new(&base).extension() {
            score += 2;
        }
        if base.len() >= 3
            && lower
                .as_bytes()
                .windows(lower.len().min(base.len()).max(1))
                .any(|v| v == &base.as_bytes()[..lower.len().min(base.len())])
        {
            score += 2;
        }
        if name.len().abs_diff(base.len()) <= 3 {
            score += 1;
        }
        if score > best_score {
            best_score = score;
            best = Some(parent.join(name));
        }
    }
    best
}
/// 使用相同的 CRLF 多数判定保留换行。
fn crlf(data: &[u8]) -> bool {
    let lf = data.iter().filter(|b| **b == b'\n').count();
    lf > 0 && data.windows(2).filter(|v| *v == b"\r\n").count() > lf / 2
}
/// 将写错误分类成工具对外提示。
fn write_error(e: &std::io::Error, p: &Path, op: &str, actual: &Path) -> ToolResult {
    let p = p.display();
    ToolResult::error(match e.raw_os_error() {
        Some(libc::EACCES) | Some(libc::EPERM) => {
            format!("permission denied writing {p}: check file/directory permissions")
        }
        Some(libc::ENOSPC) => format!("no space left on device writing {p}: disk is full"),
        Some(libc::EISDIR) => format!("cannot write {p}: path is a directory"),
        _ => format!(
            "failed to write file {p}: {}",
            util::io_error(op, actual, e)
        ),
    })
}
/// 创建同目录临时文件并替换目标；重命名失败时保留原工具的直接写回退。
async fn atomic(
    path: &Path,
    data: &[u8],
    mode: u32,
) -> Result<(), (std::io::Error, PathBuf, &'static str)> {
    let temp = path.parent().unwrap_or(Path::new(".")).join(format!(
        ".tmp.{}.{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        rand::random::<u64>() & i64::MAX as u64
    ));
    let result = async {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .open(&temp)
            .await?;
        file.write_all(data).await?;
        file.flush().await
    }
    .await;
    if let Err(e) = result {
        let _ = fs::remove_file(&temp).await;
        return Err((e, temp, "open"));
    }
    if fs::rename(&temp, path).await.is_err() {
        let _ = fs::remove_file(&temp).await;
        fs::write(path, data)
            .await
            .map_err(|e| (e, path.to_owned(), "open"))?;
    }
    Ok(())
}
/// 写文件并返回实际写入内容的统一差异。
pub async fn write(work: &Path, a: &Value) -> ToolResult {
    let p = util::string(a, "file_path");
    if p.is_empty() {
        return ToolResult::error("file_path is required");
    }
    let display = util::resolve(work, p);
    let path = fs::canonicalize(&display)
        .await
        .unwrap_or_else(|_| display.clone());
    let info = fs::metadata(&path).await.ok();
    if info.as_ref().is_some_and(|i| i.is_dir()) {
        return ToolResult::error(format!(
            "Cannot write to {}: path is an existing directory. Remove it first or choose a different path.",
            display.display()
        ));
    }
    let old = if info.is_some() {
        fs::read(&path).await.ok()
    } else {
        None
    };
    let mut data = util::string(a, "content").as_bytes().to_vec();
    let mode = info.as_ref().map_or(0o644, |i| i.permissions().mode());
    if let Some(old) = &old {
        if old.starts_with(b"\xef\xbb\xbf") && !data.starts_with(b"\xef\xbb\xbf") {
            data.splice(..0, b"\xef\xbb\xbf".iter().copied());
        }
        if crlf(old) && !crlf(&data) {
            let mut converted = Vec::new();
            for c in data {
                if c == b'\n' {
                    converted.push(b'\r');
                }
                converted.push(c);
            }
            data = converted;
        }
    }
    let parent = path.parent().unwrap_or(Path::new("."));
    if let Err(e) = util::create_dir_all(parent).await {
        return write_error(&e, &display, "mkdir", parent);
    }
    if let Err((e, p, op)) = atomic(&path, &data, mode).await {
        return write_error(&e, &display, op, &p);
    }
    let path_text = display.to_string_lossy();
    let text = if let Some(old) = old {
        format!(
            "Overwrite file {path_text} successfully\n\n{}",
            diff::unified(
                &crate::util::lossy(&old),
                &crate::util::lossy(&data),
                &path_text
            )
        )
    } else if !data.is_empty() {
        format!(
            "Write file {path_text} successfully\n\n{}",
            diff::unified("", &crate::util::lossy(&data), &path_text)
        )
    } else {
        format!("Write file {path_text} successfully (empty file)")
    };
    ToolResult::text(util::truncate(text, "write").await)
}
/// 唯一匹配替换，空旧字符串走直接创建分支。
pub async fn edit(work: &Path, a: &Value) -> ToolResult {
    let p = util::string(a, "file_path");
    if p.is_empty() {
        return ToolResult::error("file_path is required");
    }
    let path = util::resolve(work, p);
    let p = path.to_string_lossy();
    let old = util::string(a, "old_string");
    let new = util::string(a, "new_string");
    if old.is_empty() && new.is_empty() {
        return ToolResult::error("old_string and new_string cannot both be empty");
    }
    if fs::metadata(&path)
        .await
        .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound)
        && !old.is_empty()
    {
        return ToolResult::error(format!("file {p} does not exist"));
    }
    if old.is_empty() {
        let dir = path.parent().unwrap_or(Path::new("."));
        if let Err(e) = util::create_dir_all(dir).await {
            return ToolResult::error(format!(
                "edit: failed to create directory {}: {}",
                dir.display(),
                util::io_error("mkdir", dir, &e)
            ));
        }
        if let Err((e, op)) = util::write_file(&path, new.as_bytes()).await {
            return ToolResult::error(format!(
                "edit: failed to create file {p}: {}",
                util::io_error(op, &path, &e)
            ));
        }
        return ToolResult::text(
            util::truncate(
                format!("Created file {p}\n\n{}", diff::unified("", new, &p)),
                "edit",
            )
            .await,
        );
    }
    let bytes = match fs::read(&path).await {
        Ok(b) => b,
        Err(e) => {
            return ToolResult::error(format!(
                "failed to read file '{p}': {}",
                util::io_error(
                    if e.raw_os_error() == Some(libc::EISDIR) {
                        "read"
                    } else {
                        "open"
                    },
                    &path,
                    &e
                )
            ));
        }
    };
    let mut offsets = Vec::new();
    let mut cursor = 0;
    while cursor + old.len() <= bytes.len() {
        if let Some(offset) = bytes[cursor..]
            .windows(old.len())
            .position(|w| w == old.as_bytes())
        {
            let offset = cursor + offset;
            offsets.push(offset);
            cursor = offset + old.len();
        } else {
            break;
        }
    }
    let count = offsets.len();
    if count == 0 {
        return ToolResult::error(format!(
            "no occurrences of the provided multi-line string found in file '{p}'"
        ));
    }
    if count > 1 {
        return ToolResult::error(format!(
            "found multiple occurrences of the provided multi-line string in file '{p}', please ensure it's unique"
        ));
    }
    let at = offsets[0];
    let mut after = bytes[..at].to_vec();
    after.extend_from_slice(new.as_bytes());
    after.extend_from_slice(&bytes[at + old.len()..]);
    if let Err((e, op)) = util::write_file(&path, &after).await {
        return ToolResult::error(format!(
            "failed to write output to file '{p}': {}",
            util::io_error(op, &path, &e)
        ));
    }
    ToolResult::text(
        util::truncate(
            format!(
                "Replaced content in file {p}\n\n{}",
                diff::unified(old, new, &p)
            ),
            "edit",
        )
        .await,
    )
}
