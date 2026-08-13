//! `task-run` 会话模式的强类型解析。
//!
//! 空字符串保持 默认行为并解析为 `new`；未知值返回包含完整允许列表的错误。

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// `task-run --session-mode` 支持的稳定取值。
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SessionMode {
    /// 创建新会话。
    #[default]
    New,
    /// 只执行环境准备，不启动主要会话。
    SetupOnly,
    /// 恢复远端会话。
    Resume,
    /// 使用本地缓存恢复会话。
    ResumeCached,
}

impl SessionMode {
    /// 返回 CLI/wire 使用的 kebab-case 字符串。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::SetupOnly => "setup-only",
            Self::Resume => "resume",
            Self::ResumeCached => "resume-cached",
        }
    }

    /// 强类型枚举的所有实例都合法。
    #[must_use]
    pub const fn is_valid(self) -> bool {
        true
    }
}

impl fmt::Display for SessionMode {
    /// 将模式格式化为 CLI 使用的稳定字符串。
    ///
    /// 该标准 `Display` 实现承接；所有格式化入口因此共享
    /// 同一 kebab-case wire 值，而不再维护第二套字符串方法。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// 未知会话模式的解析错误。
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid session mode: {value:?} (must be new, setup-only, resume or resume-cached)")]
pub struct SessionModeParseError {
    /// 调用方提供的原始字符串。
    pub value: String,
}

impl FromStr for SessionMode {
    type Err = SessionModeParseError;

    /// 解析非空 CLI 字符串；空字符串的默认语义由 `parse_session_mode` 提供。
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "new" => Ok(Self::New),
            "setup-only" => Ok(Self::SetupOnly),
            "resume" => Ok(Self::Resume),
            "resume-cached" => Ok(Self::ResumeCached),
            unknown => Err(SessionModeParseError {
                value: unknown.to_owned(),
            }),
        }
    }
}

/// 解析会话模式，并将空字符串映射为 `SessionMode::New`。
pub fn parse_session_mode(value: &str) -> Result<SessionMode, SessionModeParseError> {
    if value.is_empty() {
        return Ok(SessionMode::New);
    }
    value.parse()
}
