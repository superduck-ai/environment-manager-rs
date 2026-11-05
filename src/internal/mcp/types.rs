//! MCP 边界的数据类型与异步接口。
//!
//! MCP server、tool 与 JSON 请求类型。JSON 类型使用 `serde(flatten)` 保存未知
//! 字段，避免代理或新协议版本经过本服务时被旧结构无意裁剪；运行期资源使用 Arc、
//! Tokio cancellation token 与显式 async trait，替代 trait object、后台任务 和 channel。

use std::{
    fmt,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, RwLock},
};

#[cfg(not(test))]
use std::sync::OnceLock;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    auth::AuthContext,
    config::{ClaudeConfig, StartupContext},
    session::ActivityRecorder,
};

/// 进程首次使用 MCP 类型时只读取一次调试 token 环境变量。
#[cfg(not(test))]
static DEBUG_HARDCODED_TOKEN: OnceLock<bool> = OnceLock::new();

/// 返回进程启动配置中的 MCP 调试 token 开关。
///
/// 在 package init 时缓存 `MCP_DEBUG_TOKEN`；`OnceLock` 保持同一“运行期间不随
/// 环境变量修改而变化”的约束，并避免并行 server 启动互相影响。
fn debug_hardcoded_token() -> bool {
    #[cfg(test)]
    {
        // 会直接覆盖包变量；Rust 单元测试默认关闭，再通过实例 setter 定向开启，
        // 避免测试宿主偶然携带环境变量导致并行场景不确定。
        false
    }
    #[cfg(not(test))]
    {
        *DEBUG_HARDCODED_TOKEN.get_or_init(|| std::env::var_os("MCP_DEBUG_TOKEN").is_some())
    }
}

/// MCP 模块跨协议、配置和 I/O 边界使用的稳定文本错误。
#[derive(Debug, Error)]
#[error("{message}")]
pub struct McpError {
    message: String,
}

impl McpError {
    /// 从已经带有完整上下文的文本构造错误。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// MCP 操作的统一结果类型。
pub type McpResult<T> = Result<T, McpError>;

/// MCP `tools/list` 返回的一项工具声明。
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Tool {
    /// 工具在单个 MCP server 内的唯一名称。
    pub name: String,
    /// 给模型看的操作说明。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// JSON Schema；wire 字段按 MCP 规范命名为 `inputSchema`。
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
    /// 新版协议增加但本版本尚不解释的字段。
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Tool {
    /// 创建一个 object-input 工具，同时保留调用方给出的完整 schema。
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
            extra: Map::new(),
        }
    }
}

/// `tools/call` 的 params 对象。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct CallToolParams {
    /// 被调用工具名称。
    #[serde(default)]
    pub name: String,
    /// 原始参数对象；非 object 值被 getter 按 视为空参数集合。
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub arguments: Value,
    /// params 中未识别字段，例如后续协议的 `_meta` 扩展。
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// 完整的 MCP `tools/call` 请求。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct CallToolRequest {
    /// JSON-RPC 版本；业务 handler 通常不读取。
    #[serde(default)]
    pub jsonrpc: String,
    /// 请求 ID，数字和字符串都必须无损保存。
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub id: Value,
    /// 方法名。
    #[serde(default)]
    pub method: String,
    /// 工具调用参数。
    #[serde(default)]
    pub params: CallToolParams,
    /// 顶层未知字段。
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl CallToolRequest {
    /// 返回字符串参数；缺失、非 object 或非字符串时返回默认值。
    #[must_use]
    pub fn get_string(&self, key: &str, default_value: &str) -> String {
        self.params
            .arguments
            .as_object()
            .and_then(|arguments| arguments.get(key))
            .and_then(Value::as_str)
            .map_or_else(|| default_value.to_owned(), str::to_owned)
    }

    /// 返回必需字符串参数，错误文案与 MCP wire `RequireString` 一致。
    pub fn require_string(&self, key: &str) -> McpResult<String> {
        let Some(arguments) = self.params.arguments.as_object() else {
            return Err(McpError::message(format!(
                "required argument {key:?} not found"
            )));
        };
        match arguments.get(key) {
            Some(Value::String(value)) => Ok(value.clone()),
            Some(_) => Err(McpError::message(format!(
                "argument {key:?} is not a string"
            ))),
            None => Err(McpError::message(format!(
                "required argument {key:?} not found"
            ))),
        }
    }
}

/// MCP 文本内容块。
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TextContent {
    /// 内容类型，正常值固定为 `text`。
    #[serde(rename = "type")]
    pub content_type: String,
    /// 给模型展示的文本。
    pub text: String,
    /// annotations 等未来字段。
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// `tools/call` 的业务结果。
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct CallToolResult {
    /// 结果内容；当前业务服务器只产生文本块。
    pub content: Vec<TextContent>,
    /// 可选结构化结果。
    #[serde(
        rename = "structuredContent",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub structured_content: Option<Value>,
    /// true 表示可交给模型自我修正的工具级错误。
    #[serde(
        rename = "isError",
        default,
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub is_error: bool,
    /// 未识别结果字段。
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// 创建只包含一个文本块的成功工具结果。
#[must_use]
pub fn new_tool_result_text(text: impl Into<String>) -> CallToolResult {
    CallToolResult {
        content: vec![TextContent {
            content_type: "text".to_owned(),
            text: text.into(),
            extra: Map::new(),
        }],
        ..CallToolResult::default()
    }
}

/// 创建只包含一个文本块的工具错误结果。
#[must_use]
pub fn new_tool_result_error(text: impl Into<String>) -> CallToolResult {
    CallToolResult {
        is_error: true,
        ..new_tool_result_text(text)
    }
}

type ToolFuture = Pin<Box<dyn Future<Output = McpResult<CallToolResult>> + Send + 'static>>;
type ToolHandlerFn =
    dyn Fn(CancellationToken, CallToolRequest) -> ToolFuture + Send + Sync + 'static;

/// 可克隆的异步工具 handler。
#[derive(Clone)]
pub struct ToolHandler(Arc<ToolHandlerFn>);

impl ToolHandler {
    /// 从返回 future 的闭包构造 handler。
    #[must_use]
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(CancellationToken, CallToolRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = McpResult<CallToolResult>> + Send + 'static,
    {
        Self(Arc::new(move |cancellation, request| {
            Box::pin(handler(cancellation, request))
        }))
    }

    /// 执行 handler，并把 HTTP 请求取消状态传到网络和轮询操作。
    pub async fn call(
        &self,
        cancellation: CancellationToken,
        request: CallToolRequest,
    ) -> McpResult<CallToolResult> {
        (self.0)(cancellation, request).await
    }
}

impl fmt::Debug for ToolHandler {
    /// handler 内可能捕获凭据，因此只输出不透明标签。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ToolHandler(..)")
    }
}

/// 工具声明和实际 handler 的组合。
#[derive(Clone, Debug)]
pub struct ToolConfig {
    /// 公开给 MCP client 的工具元数据。
    pub tool: Tool,
    /// 收到 `tools/call` 时执行的函数。
    pub handler: ToolHandler,
}

/// 启动后的 loopback MCP 连接参数。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerConfig {
    /// 内核分配的随机 TCP 端口。
    pub port: u16,
    /// 客户端必须精确发送的 Bearer token。
    pub bearer_token: String,
}

/// 单个业务 MCP server 的运行接口。
#[async_trait]
pub trait MCPServer: Send + Sync {
    /// 返回注册和日志使用的稳定名称。
    fn get_name(&self) -> String;

    /// 生成工具列表；Arc receiver 让 handler 可以安全捕获 server 自身。
    fn get_tools(self: Arc<Self>) -> Vec<ToolConfig>;

    /// 启动 loopback HTTP server。
    async fn start(&self, tools: Vec<ToolConfig>) -> McpResult<ServerConfig>;

    /// 幂等停止 HTTP server。
    async fn stop(&self);

    /// 是否还需通过 `claude mcp add` 注册。
    fn should_register_with_claude(&self) -> bool;
}

/// 配置函数返回业务 server 的 boxed future。
pub type ConfiguratorFuture =
    Pin<Box<dyn Future<Output = McpResult<Arc<dyn MCPServer>>> + Send + 'static>>;

/// MCP server configurator 的异步边界。
pub type MCPServerConfigurator = Arc<
    dyn Fn(
            Arc<BaseServer>,
            Option<Arc<AuthContext>>,
            Option<Arc<StartupContext>>,
            PathBuf,
            Arc<dyn ActivityRecorder>,
        ) -> ConfiguratorFuture
        + Send
        + Sync,
>;

/// 工厂返回 server 的 boxed future。
pub type FactoryFuture =
    Pin<Box<dyn Future<Output = McpResult<Arc<dyn MCPServer>>> + Send + 'static>>;

/// MCP server factory 的异步边界。
pub type MCPServerFactory = Arc<
    dyn Fn(
            CancellationToken,
            Option<Arc<AuthContext>>,
            PathBuf,
            Option<Arc<StartupContext>>,
            Arc<dyn ActivityRecorder>,
        ) -> FactoryFuture
        + Send
        + Sync,
>;

/// 根据启动上下文和 Claude 配置判断 registration 是否启用的共享谓词。
///
/// 独立命名该边界既避免注册构造函数与元数据结构重复复杂闭包类型，也保留 `ShouldRegister` 可捕获运行期配置且能跨 Tokio 任务共享的语义。
pub type ShouldRegister =
    Arc<dyn Fn(Option<&StartupContext>, Option<&ClaudeConfig>) -> bool + Send + Sync>;

/// MCP server 注册元数据与运行工厂。
pub struct MCPServerRegistration {
    /// server 名称。
    pub name: String,
    /// 创建、启动并按需注册 server 的工厂。
    pub factory: MCPServerFactory,
    /// 面向调用者的功能说明。
    pub description: String,
    /// 按启动上下文筛选注册项。
    pub should_register: ShouldRegister,
    /// 追加给 Claude 的 system prompt。
    pub get_additional_system_prompt: Arc<dyn Fn() -> String + Send + Sync>,
    /// 返回第一次成功构建工具列表后缓存的 Claude tool 名称。
    pub get_tools: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
}

/// 让真实 MCP registration 可直接传入 Claude 执行器的适配边界。
///
/// Claude 当前只保存 registration 的生命周期引用，并不读取额外方法；因此这个
/// 空 marker 实现不会复制配置，也不会把 MCP 工厂降级成另一套中间 DTO。
impl crate::internal::claude::McpServerRegistration for MCPServerRegistration {}

impl fmt::Debug for MCPServerRegistration {
    /// 闭包不可打印，只展示非敏感注册元数据。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MCPServerRegistration")
            .field("name", &self.name)
            .field("description", &self.description)
            .finish_non_exhaustive()
    }
}

/// `BaseServer` 运行期任务句柄。
pub(crate) struct ServerRuntime {
    /// 通知 HTTP accept loop 开始优雅退出。
    pub server_cancellation: CancellationToken,
    /// HTTP accept loop。
    pub server_task: JoinHandle<()>,
}

/// 业务 server 共享的 loopback HTTP 实现。
pub struct BaseServer {
    /// server 稳定名称。
    pub name: String,
    /// MCP implementation 版本。
    pub version: String,
    /// Bearer token；debug 模式可在启动前预置。
    pub(crate) bearer_token: RwLock<String>,
    /// 构造时从进程级 debug 开关捕获，后续启动不再读取可变环境。
    pub(crate) debug_hardcoded_token: bool,
    /// 当前监听端口；未启动时为零。
    pub(crate) port: RwLock<u16>,
    /// HTTP server 生命周期状态。
    pub(crate) runtime: tokio::sync::Mutex<Option<ServerRuntime>>,
}

impl BaseServer {
    /// 创建尚未启动的基础 server。
    #[must_use]
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            bearer_token: RwLock::new(String::new()),
            debug_hardcoded_token: debug_hardcoded_token(),
            port: RwLock::new(0),
            runtime: tokio::sync::Mutex::new(None),
        }
    }

    /// 仅供 debug/test 在启动前预置 token。
    pub fn set_bearer_token(&self, token: impl Into<String>) {
        *self
            .bearer_token
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = token.into();
    }

    /// 测试中直接改写实例的 hardcoded-token 开关。
    ///
    /// 仅在构造后、共享前调用，不改变生产 API，也不会修改进程环境或影响并行测试。
    #[cfg(test)]
    pub(crate) fn set_debug_hardcoded_token(&mut self, enabled: bool) {
        self.debug_hardcoded_token = enabled;
    }

    /// 返回当前 bearer token 的副本，避免把锁 guard 带过 await。
    #[must_use]
    pub fn bearer_token(&self) -> String {
        self.bearer_token
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// 返回当前端口；未启动时为零。
    #[must_use]
    pub fn port(&self) -> u16 {
        *self
            .port
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl fmt::Debug for BaseServer {
    /// token 和捕获它的任务不会出现在 Debug 输出中。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BaseServer")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("port", &self.port())
            .field("has_bearer_token", &!self.bearer_token().is_empty())
            .finish_non_exhaustive()
    }
}

/// registry 进程级存储的具体容器类型。
pub(crate) type RegistrationStore = RwLock<Vec<Option<Arc<MCPServerRegistration>>>>;

/// registration 内 allowed-tools 缓存的具体类型。
pub(crate) type AllowedTools = Arc<RwLock<Vec<String>>>;
