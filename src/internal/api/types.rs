//! API 协议类型。
//!
//! 原 Go 文件由 DWARF 恢复；这里保留 JSON 字段和零值语义，并用 `Option` 明确表示
//! `omitempty`。动态事件内容使用 `serde_json::Value`，以免枚举封闭后丢弃后端新增字段。

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::ApiError;

/// 对应 Go `Client`，获取指定 session 的环境配置。
#[async_trait]
pub trait Client: Send + Sync {
    /// 按 session ID 查询环境配置；取消信号触发时应立即停止网络请求。
    async fn get_environment_for_session(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
    ) -> Result<GetEnvironmentConfigResponse, ApiError>;
}

/// 后端返回的环境配置 wire DTO。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct GetEnvironmentConfigResponse {
    /// 环境实现类型，例如 `anthropic` 或 `byoc`。
    pub environment_type: String,
    /// 环境类型专属配置，延迟到对应 envtype 模块解析。
    pub config: Value,
    /// Claude Code 允许使用的工具表达式。
    #[serde(default)]
    pub allowed_tools: String,
    /// Claude Code 禁止使用的工具表达式。
    #[serde(default)]
    pub disallowed_tools: String,
}

/// Claude 会话上下文，对应 `/v2/sessions/{id}` 响应主体。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SessionContext {
    /// 后端要求的工作目录。
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub cwd: String,
    /// 原始 outcomes 配置；`None` 表示字段缺失，`Some(Null)` 表示显式 JSON null。
    #[serde(default, deserialize_with = "deserialize_present_json_value")]
    pub outcomes: Option<Value>,
    /// 替换系统提示词。
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub custom_system_prompt: String,
    /// 追加系统提示词。
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub append_system_prompt: String,
    /// 指定模型名。
    #[serde(default, deserialize_with = "deserialize_string_or_null")]
    pub model: String,
    /// 原始 MCP 配置；保留未知字段以及 missing/null 的 Go RawMessage 差别。
    #[serde(default, deserialize_with = "deserialize_present_json_value")]
    pub mcp_config: Option<Value>,
}

/// `/v2/sessions/{id}` 的外层响应结构。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SessionContextResponse {
    /// 实际会话上下文。
    #[serde(default, deserialize_with = "deserialize_default_on_null")]
    pub session_context: SessionContext,
}

/// 反序列化 Go string 字段，并把显式 JSON null 映射为空字符串零值。
fn deserialize_string_or_null<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// 保留 `json.RawMessage` 的字段存在性，避免普通 `Option<Value>` 折叠显式 null。
fn deserialize_present_json_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

/// 把显式 null 的嵌套 struct 还原为 Go `encoding/json` 留下的结构体零值。
fn deserialize_default_on_null<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// 会话事件中的文本内容块。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ContentBlock {
    /// 内容块类型，当前合成事件固定为 `text`。
    #[serde(rename = "type")]
    pub block_type: String,
    /// 文本内容。
    pub text: String,
}

/// Claude assistant 消息的 wire 表示。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct AssistantMessage {
    /// 消息角色。
    pub role: String,
    /// 模型名称。
    pub model: String,
    /// 有序内容块。
    pub content: Vec<ContentBlock>,
    /// 停止原因。
    pub stop_reason: String,
    /// token 用量。
    #[serde(default)]
    pub usage: UsageMetrics,
}

impl AssistantMessage {
    /// 表明该强类型消息可以放入 session event 的 message 槽位。
    ///
    /// Go 通过无返回值的 marker `isMessageContent` 约束 interface 联合；Rust 已把
    /// wire 字段收紧为 `Option<AssistantMessage>`，因此类型本身就是证明。这个谓词
    /// 保留可查询的兼容表面，同时不会重新引入动态分派。
    #[must_use]
    pub const fn is_message_content(&self) -> bool {
        true
    }
}

/// Claude API token 用量。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct UsageMetrics {
    /// 普通输入 token 数。
    pub input_tokens: i64,
    /// 输出 token 数。
    pub output_tokens: i64,
    /// 创建缓存所使用的输入 token 数。
    pub cache_creation_input_tokens: i64,
    /// 从缓存读取的输入 token 数。
    pub cache_read_input_tokens: i64,
}

/// 按模型名索引的 token 用量。
pub type ModelUsageMap = BTreeMap<String, UsageMetrics>;

/// 工具权限拒绝记录。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PermissionDenial {
    /// 被拒绝的工具名。
    pub tool_name: String,
    /// 拒绝原因。
    pub reason: String,
    /// 请求发生时间。
    #[serde(serialize_with = "serialize_go_time_rfc3339_nano")]
    pub requested_at: DateTime<Utc>,
}

/// 发送到 SessionIngress 或 CCR 的统一事件。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SessionEvent {
    /// 事件类别。
    #[serde(rename = "type")]
    pub event_type: String,
    /// 事件唯一标识。
    pub uuid: String,
    /// 类型专属动态数据。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// assistant 消息。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<AssistantMessage>,
    /// 父 tool-use ID。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_tool_use_id: Option<String>,
    /// 是否为合成 API 错误消息。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_api_error_message: Option<bool>,
    /// result subtype。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype: Option<String>,
    /// result 是否表示失败。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    /// 总耗时，单位毫秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i64>,
    /// API 耗时，单位毫秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_api_ms: Option<i64>,
    /// Claude 对话轮数。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_turns: Option<i64>,
    /// 总费用，单位美元。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_cost_usd: Option<f64>,
    /// result 错误列表；空列表按 Go `omitempty` 省略。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    /// 每个模型的用量；空 map 省略。
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub model_usage: ModelUsageMap,
    /// 权限拒绝；空列表省略。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permission_denials: Vec<PermissionDenial>,
    /// 汇总 token 用量。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<UsageMetrics>,
}

/// 环境管理器日志事件数据。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EnvManagerLogEventData {
    /// 日志级别。
    pub level: LogLevel,
    /// 日志类别。
    pub category: LogCategory,
    /// 人类可读内容。
    pub content: String,
    /// UTC 时间戳。
    #[serde(serialize_with = "serialize_go_time_rfc3339_nano")]
    pub timestamp: DateTime<Utc>,
    /// 附加结构化字段；BTreeMap 保持 Go JSON 的排序行为。
    pub extra: BTreeMap<String, Value>,
}

impl EnvManagerLogEventData {
    /// 表明该结构可以序列化到 session event 的动态 data 槽位。
    ///
    /// Go 的空 marker `isEventData` 只承担编译期类型约束；Rust 构造函数会先把该
    /// 结构序列化为 `serde_json::Value`。返回 true 的强类型谓词表达同一事实，且
    /// 不伪造一个没有业务行为的空回调。
    #[must_use]
    pub const fn is_event_data(&self) -> bool {
        true
    }
}

/// 环境管理器日志类别。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogCategory {
    /// 环境初始化过程。
    Init,
    /// Claude Code 主执行过程。
    ///
    /// Go 的公开常量只声明 `init`，但 executor 通过开放字符串类型显式发送
    /// `api.LogCategory("execute")`；Rust 闭合枚举必须把该实际 wire 值列出。
    Execute,
}

/// 发送到后端的日志级别。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// 调试信息。
    Debug,
    /// 普通信息。
    Info,
    /// 警告；Go wire 值为 `warning`。
    #[serde(rename = "warning")]
    Warning,
    /// 错误。
    Error,
}

/// 转发的诊断日志条目。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DiagLogEntry {
    /// 原日志时间戳。
    #[serde(serialize_with = "serialize_go_time_rfc3339_nano")]
    pub timestamp: DateTime<Utc>,
    /// 日志自定义字段。
    pub fields: BTreeMap<String, Value>,
}

/// 按 Go `time.Time.MarshalJSON` 的 RFC3339Nano 规则序列化 UTC 时间。
///
/// Chrono 的默认 `AutoSi` 只会输出 0/3/6/9 位小数，而 Go 会删除任意数量的末尾
/// 零，例如 120ms 必须写成 `.12Z`。事件 payload 会被后端逐字节观察，因此不能
/// 直接依赖 Chrono 的默认 Serde 实现。Go 还会拒绝四位范围外的年份，这里同步在
/// 序列化边界返回错误。
fn serialize_go_time_rfc3339_nano<S>(
    value: &DateTime<Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if !(0..=9999).contains(&value.year()) {
        return Err(serde::ser::Error::custom(
            "Time.MarshalJSON: year outside of range [0,9999]",
        ));
    }

    let mut encoded = value.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanoseconds = value.timestamp_subsec_nanos();
    if nanoseconds != 0 {
        let fraction = format!("{nanoseconds:09}");
        encoded.push('.');
        encoded.push_str(fraction.trim_end_matches('0'));
    }
    encoded.push('Z');
    serializer.serialize_str(&encoded)
}

/// OpenTelemetry HTTP 导出端点和认证头。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OtlpEndpoints {
    /// metrics OTLP/HTTP 地址。
    pub metrics_endpoint: String,
    /// logs OTLP/HTTP 地址。
    pub logs_endpoint: String,
    /// 两类请求共享的 header。
    pub headers: BTreeMap<String, String>,
}

/// 对可展示给用户的 session 错误建立统一接口。
pub trait SessionError: std::error::Error + Send + Sync {
    /// 返回发送给最终用户的稳定英文错误文案。
    fn get_user_message(&self) -> String;

    /// 指示 manager 是否应终止整个 session。
    fn is_fatal(&self) -> bool;
}

/// Claude Code 执行失败。
#[derive(Clone, Debug, thiserror::Error)]
#[error("Claude Code execution failed")]
pub struct ClaudeCodeExecutionError {
    /// 是否为不可恢复失败。
    pub fatal: bool,
}

/// source 拉取或处理失败。
#[derive(Clone, Debug, thiserror::Error)]
#[error("source processing failed")]
pub struct SourceProcessingError {
    /// 是否为不可恢复失败。
    pub fatal: bool,
    /// source 类型，用于用户文案。
    pub source_type: String,
}

/// 用户 init/setup 脚本执行失败。
#[derive(Clone, Debug, thiserror::Error)]
#[error("setup script exited with code {exit_code}")]
pub struct InitScriptError {
    /// 子进程退出码。
    pub exit_code: i32,
    /// 合并后的脚本输出。
    pub output: String,
}

/// session 事件和诊断日志的发送抽象。
#[async_trait]
pub trait SessionBackend: Send + Sync {
    /// 发布单个 session 事件。
    async fn post_event(
        &self,
        cancellation: &CancellationToken,
        event: &SessionEvent,
    ) -> Result<(), ApiError>;

    /// 批量发送诊断日志；空批次必须直接成功。
    async fn flush_logs(
        &self,
        cancellation: &CancellationToken,
        logs: &[DiagLogEntry],
    ) -> Result<(), ApiError>;

    /// 返回当前后端对应的 OTLP/HTTP 端点。
    fn otlp_endpoints(&self) -> OtlpEndpoints;
}
