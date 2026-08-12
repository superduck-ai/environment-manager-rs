//! Anthropic 环境的数据类型与稳定错误载体。
//!
//! 对应 Go：`internal/envtype/anthropic/anthropic.go` 与同目录配置声明。该文件集中
//! 保存 JSON DTO、环境生命周期字段及错误包装，供 `anthropic.rs` 的副作用实现使用；
//! 拆分仅为控制 Rust 模块规模，不改变外部 JSON 或调用顺序。
//!
//! `Option<Config>` 保留 Go 测试可构造 nil config 的能力；正常工厂始终填充配置。
//! JSON 字段继续接受 `null` 并落为 Go 对应零值。

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{IgnoredAny, MapAccess, Visitor},
};

use crate::internal::{
    auth::AuthContext,
    config::{SessionMode, StartupContext},
    logger::Logger,
    util::BoxError,
};

/// Anthropic 环境 JSON 配置。
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Config {
    /// 环境类型标签；Go 实现只保存而不额外验证。
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
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Language {
    /// 语言名称；识别值为 python、node/nodejs、go/golang。
    pub name: String,
    /// 原样替换安装脚本 `$1` 占位符的版本文本。
    pub version: String,
}

impl<'de> Deserialize<'de> for Config {
    /// 逐键解码以保留 Go 的大小写不敏感字段名、重复字段和 null 零值语义。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ConfigVisitor)
    }
}

/// `Config` 的 Go `encoding/json` 兼容 visitor。
struct ConfigVisitor;

impl<'de> Visitor<'de> for ConfigVisitor {
    type Value = Config;

    /// 描述顶层必须为 JSON object。
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an anthropic configuration object")
    }

    /// 按 wire 顺序应用字段；重复 map 合并，其余非 null 字段后者覆盖前者。
    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut config = Config::default();
        while let Some(key) = map.next_key::<String>()? {
            if key.eq_ignore_ascii_case("environment_type") {
                if let Some(value) = map.next_value::<Option<String>>()? {
                    config.environment_type = value;
                }
            } else if key.eq_ignore_ascii_case("version") {
                if let Some(value) = map.next_value::<Option<String>>()? {
                    config.version = value;
                }
            } else if key.eq_ignore_ascii_case("cwd") {
                if let Some(value) = map.next_value::<Option<String>>()? {
                    config.cwd = value;
                }
            } else if key.eq_ignore_ascii_case("init_script") {
                if let Some(value) = map.next_value::<Option<String>>()? {
                    config.init_script = value;
                }
            } else if key.eq_ignore_ascii_case("environment") {
                match map.next_value::<Option<BTreeMap<String, Option<String>>>>()? {
                    Some(values) => config.environment.extend(
                        values
                            .into_iter()
                            .map(|(key, value)| (key, value.unwrap_or_default())),
                    ),
                    None => config.environment.clear(),
                }
            } else if key.eq_ignore_ascii_case("languages") {
                config.languages = map
                    .next_value::<Option<Vec<Option<Language>>>>()?
                    .unwrap_or_default()
                    .into_iter()
                    .map(Option::unwrap_or_default)
                    .collect();
            } else if key.eq_ignore_ascii_case("allowed_tools") {
                if let Some(value) = map.next_value::<Option<String>>()? {
                    config.allowed_tools = value;
                }
            } else if key.eq_ignore_ascii_case("disallowed_tools") {
                if let Some(value) = map.next_value::<Option<String>>()? {
                    config.disallowed_tools = value;
                }
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(config)
    }
}

impl<'de> Deserialize<'de> for Language {
    /// 逐键解码语言字段，接受大小写变体、重复键和 null 字符串。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(LanguageVisitor)
    }
}

/// `Language` 的 Go `encoding/json` 兼容 visitor。
struct LanguageVisitor;

impl<'de> Visitor<'de> for LanguageVisitor {
    type Value = Language;

    /// 描述单个语言必须为 JSON object。
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an anthropic language object")
    }

    /// 按顺序应用 name/version，null 不覆盖已有字符串。
    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut language = Language::default();
        while let Some(key) = map.next_key::<String>()? {
            if key.eq_ignore_ascii_case("name") {
                if let Some(value) = map.next_value::<Option<String>>()? {
                    language.name = value;
                }
            } else if key.eq_ignore_ascii_case("version") {
                if let Some(value) = map.next_value::<Option<String>>()? {
                    language.version = value;
                }
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(language)
    }
}

/// Anthropic 环境的可变 session 状态。
pub struct AnthropicEnvironmentType {
    /// 正常构造时存在；`None` 只用于保留 Go nil-config 错误路径。
    pub(crate) config: Option<Config>,
    /// manager 传入的结构化 logger；生产构造和测试 fixture 都必须显式提供。
    pub(crate) logger: Logger,
    /// stdin/work secret 解码出的启动上下文。
    pub(crate) startup_context: Option<StartupContext>,
    /// 当前 session 的认证上下文。
    pub(crate) auth_context: Option<AuthContext>,
    /// 决定一次性 setup 是否运行的 session 模式。
    pub(crate) session_mode: SessionMode,
}

impl fmt::Debug for AnthropicEnvironmentType {
    /// 输出配置和 session 元数据，但不展开认证凭据或 logger handler。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicEnvironmentType")
            .field("config", &self.config)
            .field("logger", &"configured")
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
/// 恢复源码大量使用 `fmt.Errorf("prefix: %w")`。统一错误载体既保留最终稳定文本，
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

    /// 创建 `前缀: source` 形式的 Go `%w` 兼容错误。
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
    /// 输出与 Go 调用方比较的稳定错误文本。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for AnthropicError {
    /// 暴露 `%w` 对应的下层错误链。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

/// 初始化脚本失败，同时携带 Go 双返回值中的已捕获输出。
#[derive(Debug)]
pub struct RunScriptError {
    /// stdout/stderr 合并流最后最多 4096 字节的有损 UTF-8 表示。
    pub output: String,
    /// 启动错误或非零退出错误。
    pub error: AnthropicError,
}

impl fmt::Display for RunScriptError {
    /// 错误正文完全转发内层兼容文本。
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
