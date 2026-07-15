//! Go `internal/manager/skill_symlink.go` 对应的 Claude skills 目录链接准备。
//!
//! manager 在技能 ZIP 解压后确保 `~/.claude/skills` 指向 `/workspace/skills`。已有正确
//! symlink 保持不变，错误 symlink 和空目录会被替换，非空目录绝不递归删除。

use std::fs;
use std::os::unix::fs::{DirBuilderExt as _, symlink};
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

use super::skill_extraction::SKILL_ZIP_DEST_DIR;
use crate::internal::logger::{LogAttributes, Logger};

/// 对应 Go `linkWorkspaceSkillsDirToClaudeDir`。
///
/// 从 `HOME` 计算 Claude 目录并调用可测试的 under helper。任一错误仅记录 warning，
/// 不阻止 manager 启动，因为 skills 链接属于可降级的启动准备步骤。
///
/// # 参数
/// - `logger`：manager 注入的 session logger；消息和属性顺序属于 Go 兼容输出。
///
/// # 副作用
/// 可能创建 `/workspace/skills`、`~/.claude` 和 `~/.claude/skills` symlink。
pub fn link_workspace_skills_dir_to_claude_dir(logger: &Logger) {
    let Some(home_dir) = std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    else {
        logger.warn(
            "skill_symlink_failed",
            log_attributes([
                ("source_dir", serde_json::json!(SKILL_ZIP_DEST_DIR)),
                ("error", serde_json::json!("$HOME is not defined")),
            ]),
        );
        return;
    };
    if let Err(error) = link_workspace_skills_dir_to_claude_dir_under(
        logger,
        Path::new(SKILL_ZIP_DEST_DIR),
        &home_dir,
    ) {
        logger.warn(
            "skill_symlink_failed",
            log_attributes([
                ("source_dir", serde_json::json!(SKILL_ZIP_DEST_DIR)),
                ("error", serde_json::json!(error.to_string())),
            ]),
        );
    }
}

/// 对应 Go `linkWorkspaceSkillsDirToClaudeDirUnder`。
///
/// 创建 source 与 `~/.claude` 后，通过 [`ensure_directory_symlink`] 幂等准备目标链接。
/// 返回错误时调用方可记录但不应清理 source 或用户已有 skills 内容。
///
/// # 参数
/// - `logger`：成功时接收 `skill_symlink_completed` 的具体 logger。
/// - `source_dir`：workspace skills 目录，函数负责按 0755 创建。
/// - `home_dir`：用户 home；目标固定为其 `.claude/skills`。
pub(super) fn link_workspace_skills_dir_to_claude_dir_under(
    logger: &Logger,
    source_dir: &Path,
    home_dir: &Path,
) -> Result<()> {
    create_dir_all_0755(source_dir).map_err(|error| {
        anyhow!(
            "creating skills directory {}: {error}",
            source_dir.display()
        )
    })?;
    let claude_dir = home_dir.join(".claude");
    create_dir_all_0755(&claude_dir).map_err(|error| {
        anyhow!(
            "creating Claude directory {}: {error}",
            claude_dir.display()
        )
    })?;
    let target_path = claude_dir.join("skills");
    let created = ensure_directory_symlink(source_dir, &target_path)?;
    logger.info(
        "skill_symlink_completed",
        log_attributes([
            (
                "source_dir",
                serde_json::json!(source_dir.to_string_lossy()),
            ),
            (
                "target_path",
                serde_json::json!(target_path.to_string_lossy()),
            ),
            ("created", serde_json::json!(created)),
        ]),
    );
    Ok(())
}

/// 创建保持 Go `slog` 参数顺序的 skills symlink 日志属性。
fn log_attributes(
    entries: impl IntoIterator<Item = (impl Into<String>, serde_json::Value)>,
) -> LogAttributes {
    entries
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect()
}

/// 使用 `0755` 创建目录树，并保留 Go `os.MkdirAll` 的 umask 与既有目录语义。
///
/// Rust 的便捷函数默认请求 `0777`；显式 mode 可确保 umask 为 `000` 时，skills
/// 和 `.claude` 目录仍不会获得 Go 版本未授予的组/其他用户写权限。
///
/// # 错误
/// 任一级目录无法创建时返回底层 I/O 错误。
fn create_dir_all_0755(path: &Path) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o755).create(path)
}

/// 对应 Go `ensureDirectorySymlink`。
///
/// 已有正确链接返回 `false`；错误链接、普通文件或空目录先用单次 `remove_file/remove_dir`
/// 删除后创建链接。非空目录的 `remove_dir` 必然失败，因此不会删除用户技能。若创建时
/// 发生并发竞态，但竞争方已放置相同链接，也按幂等成功返回 `false`。
pub(super) fn ensure_directory_symlink(source_path: &Path, target_path: &Path) -> Result<bool> {
    match fs::symlink_metadata(target_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                if fs::read_link(target_path).ok().as_deref() == Some(source_path) {
                    return Ok(false);
                }
                fs::remove_file(target_path).map_err(|error| {
                    anyhow!(
                        "removing existing Claude skills path {}: {error}",
                        target_path.display()
                    )
                })?;
            } else {
                if !metadata.is_dir() {
                    return Err(anyhow!(
                        "Claude skills path exists and is not a directory: {}",
                        target_path.display()
                    ));
                }
                // 只删除空目录；`remove_dir` 对非空目录报错，刻意不使用 remove_dir_all。
                fs::remove_dir(target_path).map_err(|error| {
                    anyhow!(
                        "removing existing Claude skills path {}: {error}",
                        target_path.display()
                    )
                })?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(anyhow!(
                "checking Claude skills link {}: {error}",
                target_path.display()
            ));
        }
    }

    if let Err(error) = symlink(source_path, target_path) {
        if fs::read_link(target_path).ok().as_deref() == Some(source_path) {
            return Ok(false);
        }
        return Err(anyhow!(
            "creating Claude skills link {} -> {}: {error}",
            target_path.display(),
            source_path.display()
        ));
    }
    Ok(true)
}
