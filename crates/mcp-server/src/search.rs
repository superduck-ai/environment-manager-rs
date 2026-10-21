//! ripgrep 参数转换、JSON 事件、分页和搜索排序。
use super::ToolResult;
use crate::util;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
use std::{path::Path, process::Output};
use tokio::fs;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// 运行可取消搜索子进程，必要时退回单线程。
async fn execute(
    work: &Path,
    arguments: &[String],
    path: &Path,
    cancel: CancellationToken,
) -> Result<Output, String> {
    let mut args = arguments.to_vec();
    loop {
        let executable = util::executable("rg").await?;
        let mut cmd = Command::new(&executable);
        cmd.args(&args)
            .arg(path)
            .current_dir(work)
            .kill_on_drop(true);
        let result = tokio::select! {_=cancel.cancelled()=>return Err("signal: killed".into()),v=cmd.output()=>v};
        let out = result
            .map_err(|e| format!("fork/exec {}: {}", executable.display(), util::errno(&e)))?;
        let stderr = crate::util::lossy(&out.stderr);
        if (stderr.contains("os error 11") || stderr.contains("Resource temporarily unavailable"))
            && !args.iter().any(|s| s == "-j")
        {
            args.extend(["-j".into(), "1".into()]);
            continue;
        }
        return Ok(out);
    }
}
/// 保留 include 的空格、逗号和花括号处理规则。
fn includes(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    for part in s.split_whitespace() {
        if part.contains('{') && part.contains('}') {
            out.push(part.into());
        } else {
            out.extend(
                part.split(',')
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| s.trim().into()),
            );
        }
    }
    out
}
/// 生成实际 rg 参数；内容模式才传递上下文参数。
fn arguments(a: &Value, mode: &str) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = [
        "--hidden",
        "--glob",
        "!.git",
        "--glob",
        "!.svn",
        "--glob",
        "!.hg",
        "--glob",
        "!.bzr",
        "--max-columns",
        "500",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    if util::boolean(&a["multiline"]) {
        out.extend(["-U".into(), "--multiline-dotall".into()]);
    }
    if util::boolean(&a["-i"]) {
        out.push("-i".into());
    }
    match mode {
        "files_with_matches" => out.push("-l".into()),
        "count" => out.push("-c".into()),
        "content" => {
            if a.get("-n").is_none_or(util::boolean) {
                out.push("-n".into());
            }
            if let Some(n) = a
                .get("context")
                .and_then(util::int)
                .or_else(|| a.get("-C").and_then(util::int))
            {
                out.extend(["-C".into(), n.to_string()]);
            } else {
                for k in ["-B", "-A"] {
                    if let Some(n) = a.get(k).and_then(util::int) {
                        out.extend([k.into(), n.to_string()]);
                    }
                }
            }
        }
        _ => return Err(format!("invalid output_mode: {mode}")),
    }
    let pattern = util::string(a, "pattern");
    if pattern.starts_with('-') {
        out.push("-e".into());
    }
    out.push(pattern.into());
    let kind = util::string(a, "type");
    if !kind.is_empty() {
        out.extend(["--type".into(), kind.into()]);
    }
    for p in includes(util::string(a, "include")) {
        out.extend(["--glob".into(), p]);
    }
    if mode == "content" {
        out.push("--json".into());
    }
    Ok(out)
}
/// 分页只在实际截断时标记 applied limit。
fn paginate<T>(mut values: Vec<T>, a: &Value) -> (Vec<T>, String) {
    let offset = a.get("offset").and_then(util::int).unwrap_or(0).max(0) as usize;
    if offset >= values.len() {
        values.clear();
    } else {
        values.drain(..offset);
    }
    let limit = a.get("head_limit").and_then(util::int);
    let max = limit.filter(|n| *n > 0).unwrap_or(250) as usize;
    let mut parts = Vec::new();
    if limit != Some(0) && values.len() > max {
        values.truncate(max);
        parts.push(format!("limit: {max}"));
    }
    if offset > 0 {
        parts.push(format!("offset: {offset}"));
    }
    (values, parts.join(", "))
}
/// 路径中第一个冒号后的内容保持原样。
fn relative_output(work: &Path, line: &str) -> String {
    if let Some(i) = line.find(':').filter(|i| *i > 0) {
        if let Some(rel) = util::relative(work, Path::new(&line[..i])) {
            return format!("{rel}{}", &line[i..]);
        }
        return line.into();
    }
    util::relative(work, Path::new(line)).unwrap_or_else(|| line.into())
}
/// 解码 rg 的字符串或 base64 字段。
fn field(v: &Value) -> Option<String> {
    if let Some(t) = v["text"].as_str().filter(|s| !s.is_empty()) {
        return Some(t.into());
    }
    if let Some(b) = v["bytes"].as_str().filter(|s| !s.is_empty()) {
        return STANDARD
            .decode(b)
            .ok()
            .map(|b| crate::util::lossy(&b).into_owned());
    }
    Some(String::new())
}
/// 内容模式先做 20000 字符内部截断，再经过持久化层。
fn truncate_content(text: String) -> String {
    if text.chars().count() <= 20000 {
        return text;
    }
    let end = text.char_indices().nth(20000).map(|(i, _)| i).unwrap();
    format!(
        "{}\n\n... [{} lines truncated] ...",
        &text[..end],
        text[end..].split('\n').count()
    )
}
/// 返回匹配数量对应的英文单复数。
fn plural(s: &str, n: i64) -> String {
    if n == 1 { s.into() } else { format!("{s}s") }
}
/// 将 ripgrep 输出转换为参考工具的完整响应文本。
pub async fn grep(work: &Path, cancel: CancellationToken, a: &Value) -> ToolResult {
    if util::string(a, "pattern").is_empty() {
        return ToolResult::error("pattern is required");
    }
    let mode = util::string(a, "output_mode");
    let mode = if mode.is_empty() {
        "files_with_matches"
    } else {
        mode
    };
    let path = util::resolve(work, util::string(a, "path"));
    if let Err(e) = fs::metadata(&path).await {
        return ToolResult::error(if e.kind() == std::io::ErrorKind::NotFound {
            format!("search path does not exist: {}", path.display())
        } else {
            format!(
                "failed to access search path {}: {}",
                path.display(),
                util::io_error("stat", &path, &e)
            )
        });
    }
    let args = match arguments(a, mode) {
        Ok(v) => v,
        Err(e) => return ToolResult::error(format!("failed to build ripgrep arguments: {e}")),
    };
    let out = match execute(work, &args, &path, cancel).await {
        Ok(v) => v,
        Err(e) => return ToolResult::error(format!("failed to run ripgrep: {e}")),
    };
    let code = out.status.code().unwrap_or(-1);
    if code != 0 && code != 1 {
        let stderr = crate::util::lossy(&out.stderr).trim().to_owned();
        return ToolResult::error(format!(
            "failed to run ripgrep: ripgrep failed with exit code {code}: {}",
            if stderr.is_empty() {
                format!("exit status {code}")
            } else {
                stderr
            }
        ));
    }
    let text = if code == 1 {
        String::new()
    } else {
        crate::util::lossy(&out.stdout).trim().to_owned()
    };
    let mut lines: Vec<String> = text
        .split('\n')
        .filter(|l| !l.trim().is_empty())
        .map(str::to_owned)
        .collect();
    let response = match mode {
        "content" => {
            let events: Vec<Value> = lines
                .iter()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .filter(|v| matches!(v["type"].as_str(), Some("match" | "context")))
                .collect();
            let (events, info) = paginate(events, a);
            let mut lines = Vec::new();
            for event in events {
                let d = &event["data"];
                if !d.is_null() && !d.is_object()
                    || ["path", "lines"].iter().any(|key| {
                        let v = &d[key];
                        !v.is_null()
                            && (!v.is_object()
                                || ["text", "bytes"].iter().any(|key| {
                                    v.get(key).is_some_and(|v| !v.is_null() && !v.is_string())
                                }))
                    })
                    || d.get("line_number")
                        .is_some_and(|v| !v.is_null() && v.as_i64().is_none())
                {
                    continue;
                }

                let (Some(path), Some(line)) = (field(&d["path"]), field(&d["lines"])) else {
                    continue;
                };
                let path = relative_output(work, &path).trim().to_owned();
                let line = line.strip_suffix('\n').unwrap_or(&line);
                let sep = if event["type"] == "context" { "-" } else { ":" };
                if a.get("-n").is_none_or(util::boolean) {
                    lines.push(format!(
                        "{path}:{}{sep}{line}",
                        d["line_number"].as_i64().unwrap_or(0)
                    ));
                } else {
                    lines.push(format!("{path}{sep}{line}"));
                }
            }
            let text = truncate_content(lines.join("\n"));
            let mut text = if text.is_empty() {
                "No matches found".into()
            } else {
                text
            };
            if !info.is_empty() {
                text.push_str(&format!("\n\n[Showing results with pagination = {info}]"));
            }
            text
        }
        "count" => {
            let (lines, info) = paginate(lines, a);
            let (mut count, mut files) = (0i64, 0i64);
            let mut final_lines = Vec::new();
            for mut line in lines {
                let c = if let Some((_, n)) = line.rsplit_once(':').filter(|(p, _)| !p.is_empty()) {
                    n.to_owned()
                } else {
                    let c = line.trim().to_owned();
                    if c.parse::<i64>().is_ok() {
                        line = format!("{}:{c}", path.display());
                    }
                    c
                };
                if let Ok(n) = c.parse::<i64>() {
                    count = count.wrapping_add(n);
                    files += 1;
                }
                final_lines.push(relative_output(work, &line));
            }
            let text = truncate_content(final_lines.join("\n"));
            let mut text = format!(
                "{}\n\nFound {count} total {} across {files} {}.",
                if text.is_empty() {
                    "No matches found"
                } else {
                    &text
                },
                plural("occurrence", count),
                plural("file", files)
            );
            if !info.is_empty() {
                text.push_str(&format!(" with pagination = {info}"));
            }
            text
        }
        _ => {
            let mut times = std::collections::HashMap::new();
            for path in &lines {
                let time = fs::metadata(path).await.and_then(|m| m.modified()).ok();
                times.insert(path.clone(), time);
            }
            lines.sort_by(|a, b| times[b].cmp(&times[a]).then_with(|| a.cmp(b)));
            let (lines, info) = paginate(lines, a);
            if lines.is_empty() {
                "No files found".into()
            } else {
                let mut header = format!(
                    "Found {} {}",
                    lines.len(),
                    plural("file", lines.len() as i64)
                );
                if !info.is_empty() {
                    header.push_str(&format!(" {info}"));
                }
                format!(
                    "{header}\n{}",
                    lines
                        .iter()
                        .map(|s| util::relative(work, Path::new(s)).unwrap_or_else(|| s.clone()))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            }
        }
    };
    ToolResult::text(util::truncate(response, "grep").await)
}
