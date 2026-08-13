//! 配置子系统的数据声明。
//!
//! Session 配置与源码类型。源码类型使用封闭 enum 表达，因此未知 source 会在
//! 反序列化阶段被明确拒绝。

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::Value;

use crate::internal::auth::AuthConfig;

/// 把 JSON `null` 还原为字段类型的零值。
///
/// 配置协议允许 string、bool、int、slice、map 或 struct 字段为 `null`，并将其视为
/// 对应零值。Serde 的 `#[serde(default)]` 只处理字段缺失，
/// 因此所有可显式为 null 的配置字段都通过此入口统一补齐另一半语义。
fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// 解析值类型 slice，并把整个 null 或 null 元素映射为零值。
///
/// 对 `[]string`、`[]OutcomeField` 等切片中的 null 元素也会追加一个元素零值；
/// 使用 `Vec<T>` 的 Serde 默认实现则会在该元素处失败。Source 列表刻意不使用本
/// helper，因为 source 列表会把 null source 判为未知类型。
fn deserialize_null_default_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    let values = Option::<Vec<Option<T>>>::deserialize(deserializer)?.unwrap_or_default();
    Ok(values.into_iter().map(Option::unwrap_or_default).collect())
}

/// 解析 `map[string]T`，并把 null map/value 映射为零值。
///
/// JSON 会为 map 中的 primitive null 写入空字符串等零值；这与整个
/// map 为 null 时的 未初始化 map 在当前 owned DTO 中都安全归并为空集合/默认元素。
fn deserialize_null_default_map_values<'de, D, T>(
    deserializer: D,
) -> Result<HashMap<String, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    let values =
        Option::<HashMap<String, Option<T>>>::deserialize(deserializer)?.unwrap_or_default();
    Ok(values
        .into_iter()
        .map(|(key, value)| (key, value.unwrap_or_default()))
        .collect())
}

/// 所有 source 公共的 wire 字段。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BaseSource {
    /// source 的稳定类型标签，例如 `git_repository`。
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub source_type: String,
}

/// 启动 Claude 子进程所需的运行时选项。
#[derive(Clone, Debug)]
pub struct ClaudeConfig {
    /// 可执行文件名或绝对路径。
    pub command: String,
    /// 传给 Claude 的 `argv`，不含 `argv[0]`。
    pub args: Vec<String>,
    /// 额外的子进程环境变量。
    pub environment: HashMap<String, String>,
    /// 子进程工作目录。
    pub working_dir: String,
    /// 子进程执行超时；零值表示由上层决定。
    pub timeout: Duration,
    /// 旧版逗号分隔的允许工具配置。
    pub allowed_tools: String,
    /// 是否启用本地测试输出行为。
    pub local_testing_mode: bool,
    /// 旧版逗号分隔的禁止工具配置。
    pub disallowed_tools: String,
    /// 是否镜像 Claude 的详细日志。
    pub verbose_logging: bool,
    /// 仅本地调用追加的 system prompt。
    pub local_append_system_prompt: String,
    /// 用户通过 CLI 显式指定的 Claude 路径。
    pub claude_path: String,
    /// 是否打印捕获的代码执行日志。
    pub print_code_logs: bool,
    /// 是否跳过 Claude Code 执行。
    pub skip_claude_code: bool,
}

/// Git 仓库 source 的具体信息。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GitInfo {
    /// Git 提供方类型，例如 `github`。
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub git_type: String,
    /// 逻辑仓库名，通常为 `organization/repository`。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub repo: String,
    /// 可选的 ref、branch 或 commit。
    #[serde(rename = "ref", default, skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    /// 可选的显式 clone URL。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Git 主机标签。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub host: String,
    /// 可选的 source 专属认证配置。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthConfig>,
    /// 是否允许不受 outcome 约束的 git push。
    #[serde(
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub allow_unrestricted_git_push: bool,
}

/// Git 仓库 source，wire 上由顶层 `type` 和嵌套 `git_info` 组成。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GitRepositorySource {
    /// 顶层 source 类型。
    #[serde(flatten)]
    pub base: BaseSource,
    /// 仓库连接与 ref 信息。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub git_info: GitInfo,
}

/// 当前版本支持的 source 联合类型。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Source {
    /// Git 仓库 source。
    GitRepository(GitRepositorySource),
}

impl Serialize for Source {
    /// 按原始 untagged wire 形状序列化 source。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::GitRepository(source) => source.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Source {
    /// 先读取顶层 `type`，再分派到对应的具体 source 类型。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let source_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match source_type {
            "git_repository" => serde_json::from_value(value)
                .map(Self::GitRepository)
                .map_err(de::Error::custom),
            unknown => Err(de::Error::custom(format!("unknown source type: {unknown}"))),
        }
    }
}

/// 单个 MCP server 的启动或远端连接配置。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct McpServerConfig {
    /// 远端传输类型，通常为 `sse`、`http` 或 `ws`。
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub server_type: String,
    /// 远端 MCP URL。
    #[serde(
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub url: String,
    /// 本地 MCP server 命令。
    #[serde(
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub command: String,
    /// 本地命令参数。
    #[serde(
        default,
        deserialize_with = "deserialize_null_default_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub args: Vec<String>,
    /// 远端连接附加 headers。
    #[serde(
        default,
        deserialize_with = "deserialize_null_default_map_values",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub headers: HashMap<String, String>,
    /// server 暴露的工具策略。
    #[serde(
        default,
        deserialize_with = "deserialize_null_default_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub tools: Vec<McpServerConfigTool>,
    /// 未识别字段，必须在解析和再次序列化之间保持不变。
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// MCP server 中单个工具的启用与权限策略。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpServerConfigTool {
    /// MCP tool 名称。
    #[serde(
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "String::is_empty"
    )]
    pub name: String,
    /// 三态启用值；`None` 表示沿用 server 默认策略。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// 可选权限策略。
    #[serde(
        default,
        deserialize_with = "deserialize_null_default",
        skip_serializing_if = "PermissionPolicy::is_empty"
    )]
    pub permission_policy: PermissionPolicy,
}

/// MCP 工具权限策略的透明字符串类型。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PermissionPolicy(pub String);

impl PermissionPolicy {
    /// 判断策略是否为空，用于实现与 JSON 空值省略 一致的序列化。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// 顶层 MCP 配置。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct McpConfig {
    /// server 名称到配置的映射。
    #[serde(
        rename = "mcpServers",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub mcp_servers: HashMap<String, Option<McpServerConfig>>,
    /// 顶层未识别字段。
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// sandbox gateway 预计算出的 MCP 配置文件。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct McpConfigFile {
    /// 写入目标；空值由执行层替换为 `/tmp/mcp-config.json`。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub path: String,
    /// 标准 Base64 编码的文件内容。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub content: String,
    /// POSIX 文件 mode 的十进制表示。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub mode: i64,
}

/// outcome 中的 Git 仓库信息。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GitOutcomeInfo {
    /// Git 提供方类型。
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub git_type: String,
    /// outcome 对应的逻辑仓库名。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub repo: String,
    /// 允许推送的 branches；现有行为只接受恰好一个 branch。
    #[serde(default, deserialize_with = "deserialize_null_default_vec")]
    pub branches: Vec<String>,
}

/// startup/session 中的一条 outcome wire 配置。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct OutcomeField {
    /// outcome 类型，当前有效值为 `git_repository`。
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub outcome_type: String,
    /// Git 仓库 outcome 详情。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub git_info: GitOutcomeInfo,
}

/// 已验证会话的最小配置。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Session {
    /// 会话 ID。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub session_id: String,
    /// 可选 work ID。
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub work_id: String,
    /// 会话启动上下文。
    #[serde(default)]
    pub startup_context: Option<StartupContext>,
}

/// stdin 或 work secret 派生出的启动上下文。
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct StartupContext {
    /// 需要准备的源码列表。
    #[serde(default)]
    pub sources: Vec<Source>,
    /// Environment API base URL；输入允许省略 scheme，parser 会补 `https://`。
    #[serde(default)]
    pub api_base_url: String,
    /// 期望的 git outcomes。
    #[serde(default)]
    pub outcomes: Vec<OutcomeField>,
    /// 替换默认 system prompt 的文本。
    #[serde(default)]
    pub custom_system_prompt: String,
    /// 追加到默认 system prompt 的文本。
    #[serde(default)]
    pub append_system_prompt: String,
    /// Claude model 名称。
    #[serde(default)]
    pub model: String,
    /// 用户提供的 inline MCP 配置。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_config: Option<McpConfig>,
    /// 允许调用的工具。
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    /// 禁止调用的工具。
    #[serde(default)]
    pub disallowed_tools: Vec<String>,
    /// 需要显式启用的工具。
    #[serde(default)]
    pub enabled_tools: Vec<String>,
    /// sandbox gateway 预计算出的 Claude CLI 参数。
    #[serde(default)]
    pub claude_code_args: HashMap<String, String>,
    /// sandbox gateway 预计算出的 MCP 文件。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_config_file: Option<McpConfigFile>,
    /// 是否让 Claude 使用 sandbox gateway 配置。
    #[serde(default)]
    pub use_sandbox_gateway_config: bool,
    /// BYOC 环境入口命令。
    #[serde(default)]
    pub entrypoint: String,
    /// 注入子进程的环境变量。
    #[serde(default)]
    pub environment_variables: HashMap<String, String>,
    /// 环境子类型，例如 `baku`。
    #[serde(default)]
    pub environment_sub_type: String,
    /// 是否使用 code sessions v2 工作流。
    #[serde(default)]
    pub use_code_sessions: bool,
}

/// `StartupContext` 的 serde 输入形状。
///
/// 单独的 raw 类型让自定义反序列化边界保持清晰，同时所有字段仍由 serde 的
/// `default`、`flatten` 和封闭 source enum 完成结构化校验。
#[derive(Default, Deserialize)]
struct RawStartupContext {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    sources: Vec<Source>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    api_base_url: String,
    #[serde(default, deserialize_with = "deserialize_null_default_vec")]
    outcomes: Vec<OutcomeField>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    custom_system_prompt: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    append_system_prompt: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    model: String,
    #[serde(default)]
    mcp_config: Option<McpConfig>,
    #[serde(default, deserialize_with = "deserialize_null_default_vec")]
    allowed_tools: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_null_default_vec")]
    disallowed_tools: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_null_default_vec")]
    enabled_tools: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_null_default_map_values")]
    claude_code_args: HashMap<String, String>,
    #[serde(default)]
    mcp_config_file: Option<McpConfigFile>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    use_sandbox_gateway_config: bool,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    entrypoint: String,
    #[serde(default, deserialize_with = "deserialize_null_default_map_values")]
    environment_variables: HashMap<String, String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    environment_sub_type: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    use_code_sessions: bool,
}

impl<'de> Deserialize<'de> for StartupContext {
    /// 执行 startup context 的完整结构化解析。
    ///
    /// `McpConfig` 和 `McpServerConfig` 在同一次反序列化中保留 MCP 未知字段，
    /// 并由
    /// `McpServerConfig` 的 `#[serde(flatten)]` 完成该工作。source 则由封闭 enum
    /// 逐项判别，未知类型仍会立即返回带类型名的解析错误。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // 允许把顶层 JSON null 解进已分配的 raw struct，并继续使用全部零值。
        // `Option` 只在 wire 边界承接这一特殊输入，公开 StartupContext 不引入
        // “整个上下文可空”的额外状态。
        let raw = Option::<RawStartupContext>::deserialize(deserializer)?.unwrap_or_default();
        Ok(Self {
            sources: raw.sources,
            api_base_url: raw.api_base_url,
            outcomes: raw.outcomes,
            custom_system_prompt: raw.custom_system_prompt,
            append_system_prompt: raw.append_system_prompt,
            model: raw.model,
            mcp_config: raw.mcp_config,
            allowed_tools: raw.allowed_tools,
            disallowed_tools: raw.disallowed_tools,
            enabled_tools: raw.enabled_tools,
            claude_code_args: raw.claude_code_args,
            mcp_config_file: raw.mcp_config_file,
            use_sandbox_gateway_config: raw.use_sandbox_gateway_config,
            entrypoint: raw.entrypoint,
            environment_variables: raw.environment_variables,
            environment_sub_type: raw.environment_sub_type,
            use_code_sessions: raw.use_code_sessions,
        })
    }
}
