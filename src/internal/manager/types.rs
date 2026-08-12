//! Manager 生命周期编排所需的类型与窄适配边界。
//!
//! Manager 的配置、错误和运行时依赖类型。Rust 版本把同步
//! function/interface 改为可共享闭包和 async trait；API 附加上下文、o11y 诊断及
//! environment lease 的动态能力通过局部 adapter 暴露，避免 manager 反向依赖具体实现。

use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    path::PathBuf,
    sync::Arc,
    time::SystemTime,
};

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::internal::{
    api::{self, ApiError, GetEnvironmentConfigResponse},
    auth::AuthContext,
    claude::{Executor, Outcomes},
    config::{ClaudeConfig, Session, SessionMode, StartupContext},
    envtype::{EnvTypes, EnvironmentType, LeaseManagerHandle},
    mcp::{MCPServer, MCPServerRegistration},
    session::{ActivityRecorder, EventPoster},
    util::BoxError,
};

/// Manager 对外返回的 Go 兼容错误字节。
///
/// Go 的 `error` 文本实际可以包含任意字节，特别是 Claude/npm `CombinedOutput` 会把
/// 子进程输出直接拼入 `%w` 错误链。这里不能用 `String`，否则还未到 CLI 就会把无效
/// UTF-8 永久替换。日志与 JSON 通过 [`Display`] 取得 Go `encoding/json` 风格的视图，
/// 最终 stderr 则通过 [`ManagerError::as_bytes`] 原样输出。
#[derive(Debug)]
pub struct ManagerError {
    message: Vec<u8>,
}

impl ManagerError {
    /// 从已带调用阶段上下文的 UTF-8 消息构造 manager 错误。
    ///
    /// 普通 API、配置和环境错误均走此入口；传入文本按 UTF-8 原样保存，不做额外
    /// 转义。可能携带子进程原始输出的 crate 内调用方必须改用
    /// `ManagerError::message_bytes`；它不公开给外部调用方。
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into().into_bytes(),
        }
    }

    /// 从可能包含无效 UTF-8 或 NUL 的完整字节构造 manager 错误。
    ///
    /// 该构造器只在 crate 内部错误适配边界使用，防止普通业务代码意外绕过文本
    /// 约束。字节不会被验证、转义或截断。
    #[must_use]
    pub(crate) fn message_bytes(message: Vec<u8>) -> Self {
        Self { message }
    }

    /// 给下层错误添加 ASCII 阶段前缀，同时保留下层的全部原始字节。
    ///
    /// `prefix` 应包含 Go `fmt.Errorf("...: %w")` 在 `%w` 前产生的完整静态文本；
    /// 返回值仅执行一次连续拼接，不经过 `Display`，因此无效 UTF-8 不会丢失。
    #[must_use]
    pub(crate) fn prefixed(prefix: &[u8], error: &[u8]) -> Self {
        let mut message = Vec::with_capacity(prefix.len() + error.len());
        message.extend_from_slice(prefix);
        message.extend_from_slice(error);
        Self::message_bytes(message)
    }

    /// 返回最终 CLI 应写入 stderr 的原始错误字节。
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.message
    }

    /// 消费错误并移交原始字节所有权，避免 manager→cmd 边界再次复制。
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.message
    }
}

impl Display for ManagerError {
    /// 为日志、指标和诊断系统生成 Go `encoding/json` 可接受的 UTF-8 文本视图。
    ///
    /// 每个单独的无效输入字节都替换为一个 U+FFFD；这与 Go `utf8.DecodeRune`
    /// 的 size=1 行为一致，也避免 `String::from_utf8_lossy` 合并连续坏字节。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&go_utf8_error_view(&self.message))
    }
}

impl std::error::Error for ManagerError {}

/// 将任意错误字节转换成 Go `encoding/json` 采用的逐坏字节替换文本。
fn go_utf8_error_view(value: &[u8]) -> String {
    let mut output = String::new();
    let mut remaining = value;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                output.push_str(valid);
                break;
            }
            Err(error) => {
                let valid_end = error.valid_up_to();
                // `valid_up_to` 定义了合法 UTF-8 前缀，因此这里不会失败；避免 unwrap
                // 也让错误展示路径永远不会因为另一个格式化错误 panic。
                output.push_str(std::str::from_utf8(&remaining[..valid_end]).unwrap_or_default());
                output.push('\u{fffd}');
                remaining = &remaining[valid_end + 1..];
            }
        }
    }
    output
}

/// Manager 使用的 API client 能力集合。
///
/// `api::Client` 只声明环境配置请求；Go 运行时还通过可选 interface 取得 auth context
/// 和 outcomes。Rust 在 manager 边界显式声明这两项，避免 `Any` downcast。
#[async_trait]
pub trait ManagerApiClient: Send + Sync {
    /// 读取当前 session 的 environment 类型、专属配置和工具策略。
    async fn get_environment_for_session(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
    ) -> Result<GetEnvironmentConfigResponse, ApiError>;

    /// 返回已解析的认证上下文；`None` 保留 Go 缺失时 panic 的配置错误语义。
    fn get_auth_context(&self) -> Option<AuthContext>;

    /// 返回 API/work 层预先计算的 repository outcomes；空值使用新建空集合。
    fn get_outcomes(&self) -> Option<Outcomes>;
}

/// 把基础 `api::Client` 与调用方持有的 auth/outcomes 组合成 manager client。
#[derive(Clone)]
pub struct ManagerApiClientAdapter {
    client: Arc<dyn api::Client>,
    auth_context: Option<AuthContext>,
    outcomes: Option<Outcomes>,
}

impl ManagerApiClientAdapter {
    /// 创建显式上下文 adapter；所有返回值均 clone，调用方后续修改不会改变模板。
    #[must_use]
    pub fn new(
        client: Arc<dyn api::Client>,
        auth_context: Option<AuthContext>,
        outcomes: Option<Outcomes>,
    ) -> Self {
        Self {
            client,
            auth_context,
            outcomes,
        }
    }
}

#[async_trait]
impl ManagerApiClient for ManagerApiClientAdapter {
    /// 将环境配置请求原样转发给基础 API client。
    async fn get_environment_for_session(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
    ) -> Result<GetEnvironmentConfigResponse, ApiError> {
        self.client
            .get_environment_for_session(cancellation, session_id)
            .await
    }

    /// 返回 adapter 保存的 auth context 副本。
    fn get_auth_context(&self) -> Option<AuthContext> {
        self.auth_context.clone()
    }

    /// 返回 adapter 保存的 outcomes 深拷贝。
    fn get_outcomes(&self) -> Option<Outcomes> {
        self.outcomes.clone()
    }
}

/// Claude executor 的同步构造闭包。
///
/// 参数顺序与 Go `ClaudeExecutorFactory` 一致；Rust 使用 owned clone 让闭包无需跨 await
/// 借用 manager。Lease 采用最小 handle，因为具体 podmonitor 类型属于 envtype adapter。
pub type ClaudeExecutorFactory = Arc<
    dyn Fn(
            ClaudeConfig,
            String,
            Option<StartupContext>,
            AuthContext,
            Vec<Arc<MCPServerRegistration>>,
            Outcomes,
            Arc<dyn ActivityRecorder>,
            Option<Arc<dyn LeaseManagerHandle>>,
        ) -> Result<Arc<dyn Executor>, BoxError>
        + Send
        + Sync,
>;

/// Tunnel 长连接的 manager 侧最小生命周期接口。
#[async_trait]
pub trait TunnelClient: Send + Sync {
    /// 建立连接并持续处理请求，直到收到取消或遇到不可恢复错误。
    async fn connect_and_run(
        self: Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<(), ManagerError>;

    /// 关闭主连接和仍存活的本地 WebSocket tunnel；必须可重复调用。
    async fn close(&self) -> Result<(), ManagerError>;
}

#[async_trait]
impl TunnelClient for crate::internal::tunnel::Client {
    /// 把 manager token 转发给真实 tunnel client 的长期连接循环。
    async fn connect_and_run(
        self: Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<(), ManagerError> {
        crate::internal::tunnel::Client::connect_and_run(&self, cancellation)
            .await
            .map_err(|error| ManagerError::message(error.to_string()))
    }

    /// 转发真实 tunnel client 的异步关闭流程。
    async fn close(&self) -> Result<(), ManagerError> {
        crate::internal::tunnel::Client::close(self)
            .await
            .map_err(|error| ManagerError::message(error.to_string()))
    }
}

/// 创建 tunnel client 的可替换闭包，对应 Go `createTunnelClientFuncType`。
pub type CreateTunnelClientFunc = Arc<
    dyn Fn(
            &Manager,
            &str,
            &str,
            Option<&AuthContext>,
        ) -> Result<Arc<dyn TunnelClient>, ManagerError>
        + Send
        + Sync,
>;

/// 一次 environment-specific lease 构造请求。
#[derive(Clone)]
pub struct ManagerLeaseRequest {
    /// Manager 的父取消令牌；新 lease 必须从它派生自己的执行 token。
    pub cancellation: CancellationToken,
    /// 当前 session ID。
    pub session_id: String,
    /// 后端 environment ID。
    pub environment_id: String,
    /// startup context 中的 API base URL。
    pub api_base_url: String,
    /// Session ingress bearer token；不得日志输出。
    pub session_ingress_token: String,
    /// organization UUID，同时作为部分部署/lease 请求的 tenant 标识。
    pub organization_uuid: String,
}

/// 已构造 lease 的运行句柄与执行取消上下文。
#[derive(Clone)]
pub struct ManagerLeaseRuntime {
    /// start/stop 生命周期句柄。
    pub(super) handle: Arc<dyn LeaseManagerHandle>,
    /// Lease 过期、服务端终止或父取消时触发的 Claude 执行 token。
    pub(super) cancellation: CancellationToken,
}

impl ManagerLeaseRuntime {
    /// 从 lifecycle handle 读取其真实 cancellation token。
    ///
    /// token 不由 adapter 或 manager 另行创建，因而 lease 过期、服务端终止和父级
    /// 取消都能沿 podmonitor 的同一棵取消树传到 executor。
    #[must_use]
    pub fn new(handle: Arc<dyn LeaseManagerHandle>) -> Self {
        let cancellation = handle.cancellation_token();
        Self {
            handle,
            cancellation,
        }
    }
}

/// 测试或特殊组合层覆盖动态 environment lease 发现行为的窄 adapter。
///
/// 生产默认路径直接调用 `EnvironmentType::lease_manager_provider`；该 adapter 不再是
/// 创建 BYOC podmonitor 的必要接线，仅保留现有可替换测试边界。
pub trait EnvironmentLeaseAdapter: Send + Sync {
    /// 若 environment 支持 lease 则创建 runtime，否则返回 `Ok(None)`。
    fn create_lease_manager(
        &self,
        environment: &dyn EnvironmentType,
        request: ManagerLeaseRequest,
    ) -> Result<Option<ManagerLeaseRuntime>, BoxError>;
}

/// Manager 内部使用的无 PII 诊断出口。
///
/// 名称和字段与 Go `diag.LogEnvManagerNoPII` 一一对应；没有注入实现时仅省略诊断，
/// 不把同一事件伪装成普通业务日志。
pub trait ManagerObservability: Send + Sync {
    /// 记录一个稳定事件名及不含用户数据的结构化字段。
    fn log_env_manager_no_pii(&self, event: &str, fields: BTreeMap<String, Value>);
}

/// Environment manager 的全部运行依赖和会话状态。
///
/// 字段顺序机械对应 Go `Manager`；末尾三个 adapter 是 Rust 为缺失的动态 interface
/// 查询增加的显式能力。所有可跨任务对象都由 `Arc` 共享。
#[derive(Clone)]
pub struct Manager {
    pub(super) api_client: Arc<dyn ManagerApiClient>,
    pub(super) session_config: Option<Session>,
    pub(super) env_types: Arc<EnvTypes>,
    pub(super) executor_factory: ClaudeExecutorFactory,
    pub(super) session_mode: SessionMode,
    pub(super) session_mode_explicitly_set: bool,
    pub(super) claude_config: Option<ClaudeConfig>,
    pub(super) environment_id: String,
    pub(super) organization_uuid: String,
    pub(super) process_start_time: SystemTime,
    pub(super) event_poster: Option<Arc<dyn EventPoster>>,
    pub(super) create_tunnel_client_func: Option<CreateTunnelClientFunc>,
    pub(super) environment_lease_adapter: Option<Arc<dyn EnvironmentLeaseAdapter>>,
    pub(super) observability: Option<Arc<dyn ManagerObservability>>,
}

/// MCP 并行初始化任务的完整结果。
pub(super) struct McpRegistrationBatch {
    /// 已成功启动、清理阶段需要停止的 servers。
    pub(super) servers: Vec<Arc<dyn MCPServer>>,
    /// 尝试过的全部 registrations，包括非致命失败项。
    pub(super) registrations: Vec<Arc<MCPServerRegistration>>,
}

/// 可取消 direct-child 命令的 combined output 失败信息。
pub(super) struct CommandFailure {
    /// spawn、wait、取消或非零退出状态的兼容文本。
    pub(super) error: String,
    /// 失败前 stdout/stderr 共用 pipe 中已经读取的字节。
    pub(super) output: Vec<u8>,
}

/// 把工作目录字符串转换成 MCP factory 使用的 owned path。
#[must_use]
pub(super) fn working_directory_path(working_directory: &str) -> PathBuf {
    PathBuf::from(working_directory)
}
