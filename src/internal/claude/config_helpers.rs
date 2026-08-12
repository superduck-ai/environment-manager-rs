//! Claude 可执行文件路径的三层选择逻辑。
//!
//! 显式 CLI/config 值优先于 `CLAUDE_DEFAULT_PATH`，两者都为空时回退到 PATH 中的
//! `claude`。空字符串不会覆盖后续来源，与 Go `os.Getenv` 行为一致。

use crate::internal::{config::ClaudeConfig, logger::Logger};

use super::types::claude_attrs;

/// 选择 Claude 可执行文件路径并记录非默认来源。
#[must_use]
pub fn get_claude_path(logger: &Logger, cfg: Option<&ClaudeConfig>) -> String {
    if let Some(path) = cfg
        .map(|configuration| configuration.claude_path.as_str())
        .filter(|path| !path.is_empty())
    {
        logger.info(
            "Using custom Claude path from config (CLI flag)",
            claude_attrs!("path" => path),
        );
        return path.to_owned();
    }

    if let Some(path) = std::env::var_os("CLAUDE_DEFAULT_PATH").filter(|path| !path.is_empty()) {
        let path = path.to_string_lossy().into_owned();
        logger.info(
            "Using custom Claude path from CLAUDE_DEFAULT_PATH",
            claude_attrs!("path" => &path),
        );
        return path;
    }

    "claude".to_owned()
}
