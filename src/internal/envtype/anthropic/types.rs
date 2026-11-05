//! Anthropic 环境的数据类型与稳定错误载体。
//!
//! 集中保存 JSON DTO、环境生命周期字段及错误包装，供 `anthropic.rs` 的副作用实现
//! 使用；模块拆分不改变外部 JSON 或调用顺序。
//!
//! `Option<Config>` 允许测试构造 `None` 配置；正常工厂始终填充配置。JSON 使用正式的
//! snake_case 字段名，缺失字段采用类型默认值。

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::internal::{
    auth::AuthContext,
    config::{SessionMode, StartupContext},
    util::BoxError,
};

/// Anthropic 环境 JSON 配置。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct Config {
    /// 环境类型标签；只保存而不额外验证。
    pub environment_type: String,
    /// 配置版本；空值表示未指定。
    #[serde(skip_serializing_if = "String::is_empty")]
    pub version: String,
    /// session 工作目录；解码后必须非空。
    pub cwd: String,
    /// 新建或 setup-only session 执行的初始化脚本。
    #[serde(skip_serializing_if = "String::is_empty")]
    pub init_script: String,
    /// 注入 Claude Code 子进程的环境变量。
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    /// 需要并行准备的语言版本。
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub languages: Vec<Language>,
    /// 旧版逗号分隔允许工具配置。
    #[serde(skip_serializing_if = "String::is_empty")]
    pub allowed_tools: String,
    /// 旧版逗号分隔禁止工具配置。
    #[serde(skip_serializing_if = "String::is_empty")]
    pub disallowed_tools: String,
}

/// 单个语言及其期望版本。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct Language {
    /// 语言名称；识别值为 python、node/nodejs、go/golang。
    pub name: String,
    /// 原样替换安装脚本 `$1` 占位符的版本文本。
    pub version: String,
}

/// Anthropic 环境的可变 session 状态。
pub struct AnthropicEnvironmentType {
    /// 正常构造时存在；`None` 只用于保留 `None` 配置 错误路径。
    pub(crate) config: Option<Config>,
    /// stdin/work secret 解码出的启动上下文。
    pub(crate) startup_context: Option<StartupContext>,
    /// 当前 session 的认证上下文。
    pub(crate) auth_context: Option<AuthContext>,
    /// 决定一次性 setup 是否运行的 session 模式。
    pub(crate) session_mode: SessionMode,
}

impl fmt::Debug for AnthropicEnvironmentType {
    /// 输出配置和 session 元数据，但不展开认证凭据。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicEnvironmentType")
            .field("config", &self.config)
            .field("startup_context", &self.startup_context)
            .field("has_auth_context", &self.auth_context.is_some())
            .field("session_mode", &self.session_mode)
            .finish()
    }
}

/// 引导 Claude 设置时使用的一组内置文件。
#[derive(Clone, Debug)]
pub(crate) struct HookConfig {
    /// `.claude/settings.json` 的完整字节。
    pub(crate) settings_data: Vec<u8>,
    /// 放在 `.claude` 下的 stop hook 文件名。
    pub(crate) hook_name: String,
    /// stop hook 的完整字节。
    pub(crate) hook_data: Vec<u8>,
}

/// Anthropic 构造或初始化错误。
///
/// 既定接口大量使用 错误 source chain。统一错误载体既保留最终稳定文本，
/// 也保留可遍历的 source 链，不把 Tokio/平台错误枚举泄漏到公共 API。
#[derive(Debug)]
pub struct AnthropicError {
    message: String,
    source: Option<BoxError>,
}

impl AnthropicError {
    /// 创建没有下层 source 的稳定文本错误。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// 创建 `前缀: source` 形式的稳定错误链。
    pub fn with_source<E>(prefix: impl AsRef<str>, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            message: format!("{}: {source}", prefix.as_ref()),
            source: Some(Box::new(source)),
        }
    }

    /// 创建已完整格式化、同时保留下层 source 的错误。
    pub(crate) fn formatted_with_source<E>(message: String, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            message,
            source: Some(Box::new(source)),
        }
    }
}

impl fmt::Display for AnthropicError {
    /// 输出与 调用方比较的稳定错误文本。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for AnthropicError {
    /// 暴露 `source chain` 的下层错误。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

/// 初始化脚本失败，同时携带已捕获输出和可用的退出码。
#[derive(Debug)]
pub struct RunScriptError {
    /// stdout/stderr 合并流最后最多 4096 字节的有损 UTF-8 表示。
    pub output: String,
    /// 启动错误或非零退出错误。
    pub error: AnthropicError,
    /// 子进程正常退出时的退出码；启动失败时为空。
    pub exit_code: Option<i32>,
}

impl fmt::Display for RunScriptError {
    /// 错误正文完全转发内层稳定文本。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl Error for RunScriptError {
    /// 保留初始化脚本错误的 source 链。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

/// 将 boxed 环境错误转换为注册工厂要求的动态错误。
pub(crate) fn boxed(error: AnthropicError) -> BoxError {
    Box::new(error)
}

/// 让 sources manager 在当前 `initialize` 调用期间借用 recorder。
///
/// 该 wrapper 自身不决定生命周期擦除；擦除被限制在创建和销毁 manager 的同一
/// async 作用域内，避免 recorder 被保存到 session 之外。
pub(crate) struct ScopedActivityRecorder<'a> {
    /// 原始 recorder 借用。
    pub(crate) inner: &'a (dyn crate::internal::session::ActivityRecorder + Send + Sync),
}

/// 当前 initialize 调用内共享的 recorder wrapper。
///
/// `'recorder` 明确保留底层借用的有效期；Arc 只允许 manager 与 Git handler 共享
/// wrapper 所有权，不会也不能把引用提升为 `'static`。
pub(crate) type SharedScopedRecorder<'recorder> =
    Arc<dyn crate::internal::session::ActivityRecorder + 'recorder>;
