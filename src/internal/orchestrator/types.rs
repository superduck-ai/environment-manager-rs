//! Orchestrator 的共享类型声明。
//!
//! 本模块集中协议 DTO、轮询 trait 和 orchestrator 配置，并用 `Arc<dyn Trait>` 表达
//! 可共享接口、用 `CancellationToken` 表达取消能力。

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::internal::process::{CommandFailure, CommandOutput};

/// 后端 work envelope 的原始 JSON 字节。
pub type RawMessage = Vec<u8>;

/// orchestrator 子系统的稳定文本错误。
///
/// 网络、进程和 JSON 库的具体错误类型不会泄漏进公共 trait；完整原因仍保留在
/// `Display` 文本中，便于命令行入口按 的文案向上包装。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct OrchestratorError {
    message: String,
}

impl OrchestratorError {
    /// 由稳定文案构造错误。
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// orchestrator 公共结果别名。
pub type OrchestratorResult<T> = Result<T, OrchestratorError>;

/// Hook 子进程的共享执行结果；非零退出仍是带 `ExitStatus` 的完成结果。
pub type CommandOutcome = Result<CommandOutput, CommandFailure>;

/// 跨模块的 sandbox 包与 Hook 之间的目录内适配请求。
///
/// adapter 必须在 `execute` 返回前完成临时 sandbox 的 cleanup；这样未来接入真实
/// `internal::sandbox` 时，Hook 的“初始化 → 包装 → 执行 → cleanup”时序不会改变。
#[derive(Clone)]
pub struct SandboxRequest {
    /// 上层取消令牌。
    pub cancellation: CancellationToken,
    /// 原始 work JSON，既是 sandbox 配置来源也是命令 stdin。
    pub stdin_data: Vec<u8>,
    /// sandbox 配置文件路径。
    pub sandbox_config_path: String,
    /// sandbox 工作目录。
    pub working_dir: String,
    /// 尚未包装的真实命令。
    pub command: String,
    /// 尚未包装的参数（已包含注入的 session ID）。
    pub args: Vec<String>,
    /// `None` 表示没有 Hook 级超时。
    pub timeout: Option<Duration>,
}

/// Hook 与 sandbox runtime 之间的显式适配接口。
///
/// 启用 sandbox 却没有提供 adapter 会返回错误，而不会静默退化为宿主机执行。
#[async_trait]
pub trait SandboxAdapter: Send + Sync {
    /// 包装并执行命令，且在返回前清理 sandbox 资源。
    async fn execute(&self, request: SandboxRequest) -> OrchestratorResult<CommandOutcome>;
}

/// 外部 timeout/execute Hook 配置。
#[derive(Clone)]
pub struct Hook {
    /// 日志和错误中使用的 Hook 名称。
    pub name: String,
    /// 可执行文件路径或命令名；空值表示 no-op。
    pub command: String,
    /// 固定参数。
    pub args: Vec<String>,
    /// 单次执行超时；零值表示不额外设置超时。
    pub timeout: Duration,
    /// 是否要求 sandbox 执行。
    pub sandbox_enabled: bool,
    /// sandbox 设置文件路径。
    pub sandbox_config_path: String,
    /// sandbox 工作目录。
    pub working_dir: String,
    /// 可选 sandbox 适配器。
    pub sandbox_adapter: Option<Arc<dyn SandboxAdapter>>,
}

impl Default for Hook {
    /// 返回默认 Hook；空命令是 no-op。
    fn default() -> Self {
        Self {
            name: String::new(),
            command: String::new(),
            args: Vec::new(),
            timeout: Duration::ZERO,
            sandbox_enabled: false,
            sandbox_config_path: String::new(),
            working_dir: String::new(),
            sandbox_adapter: None,
        }
    }
}

/// HTTP Poller 与命令 PollHook 的统一异步接口。
#[async_trait]
pub trait PollerInterface: Send + Sync {
    /// 非阻塞查询一次队列；`None` 表示队列为空。
    async fn poll(
        &self,
        cancellation: &CancellationToken,
    ) -> OrchestratorResult<Option<RawMessage>>;

    /// 空队列后的 1–3 秒随机等待；取消应立即中断。
    async fn sleep_with_jitter(&self, cancellation: &CancellationToken) -> OrchestratorResult<()>;
}

/// orchestrator 可观测事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrchestratorEvent {
    /// loop timeout 到达。
    Timeout,
    /// 开始一次 poll。
    PollAttempt,
    /// poll 返回错误。
    PollError,
    /// poll 成功但队列为空。
    QueueEmpty,
    /// 已收到 session，准备输出或执行。
    SessionStart,
    /// session 已成功交付。
    SessionEnd,
    /// session 交付失败。
    SessionEndError(String),
}

/// orchestrator 与 o11y 计数/耗时记录之间的窄接口。
pub trait OrchestratorObserver: Send + Sync {
    /// 记录计数事件。
    fn record_event(&self, _event: OrchestratorEvent) {}

    /// 记录单次 poll 耗时。
    fn record_poll_duration(&self, _duration: Duration) {}
}

/// 默认无操作 observer；不会改变 orchestrator 控制流。
#[derive(Debug, Default)]
pub struct NoopOrchestratorObserver;

impl OrchestratorObserver for NoopOrchestratorObserver {}

/// session 原始 JSON 的输出目标，默认实现写 stdout，测试可注入内存实现。
#[async_trait]
pub trait SessionOutput: Send + Sync {
    /// 写入一条 session JSON，并负责末尾换行。
    async fn write_session(&self, session_json: &[u8]) -> OrchestratorResult<()>;
}

/// 默认 stdout 输出目标。
#[derive(Debug, Default)]
pub struct StdoutSessionOutput;

/// orchestrator 状态机配置。
#[derive(Clone)]
pub struct OrchestratorConfig {
    /// 必填 poller；保留 `Option` 以便构造函数校验缺失值。
    pub poller: Option<Arc<dyn PollerInterface>>,
    /// loop timeout 到达时执行；错误只记录，不终止状态机。
    pub timeout_hook: Option<Arc<Hook>>,
    /// 收到 work 后执行；配置后不再把 JSON 写 stdout。
    pub execute_hook: Option<Arc<Hook>>,
    /// 单轮 poll 窗口，零值默认 5 分钟。
    pub poll_timeout: Duration,
    /// timeout Hook 周期，零值默认 5 分钟。
    pub loop_timeout: Duration,
    /// 连续 poll 失败上限；0 表示无限重试。
    pub max_poll_failures: i64,
    /// 可选 o11y adapter。
    pub observer: Option<Arc<dyn OrchestratorObserver>>,
    /// 可选 session 输出 adapter。
    pub session_output: Option<Arc<dyn SessionOutput>>,
}

impl Default for OrchestratorConfig {
    /// 返回默认配置；构造函数负责校验 poller 并填充超时。
    fn default() -> Self {
        Self {
            poller: None,
            timeout_hook: None,
            execute_hook: None,
            poll_timeout: Duration::ZERO,
            loop_timeout: Duration::ZERO,
            max_poll_failures: 0,

            observer: None,
            session_output: None,
        }
    }
}

/// 持有已经验证配置的 orchestrator 状态机。
pub struct Orchestrator {
    pub(crate) config: OrchestratorConfig,
}

/// execute Hook 从 work JSON 中读取的最小 session DTO。
#[derive(Debug, Default, Deserialize)]
pub(crate) struct SessionData {
    #[serde(default)]
    pub(crate) id: String,
}

/// 命令型 poller。
#[derive(Clone)]
pub struct PollHook {
    /// 可执行命令。
    pub command: String,
    /// 固定参数。
    pub args: Vec<String>,
    /// 每次执行超时。
    pub timeout: Duration,
    /// 序列化到 stdin 的环境身份。
    pub stdin_context: PollHookContext,
}

/// PollHook stdin 协议。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PollHookContext {
    /// organization UUID。
    pub organization_uuid: String,
    /// environment ID。
    pub environment_id: String,
    /// runner worker ID。
    pub worker_id: String,
}

/// 后端 HTTP work poller。
#[derive(Clone)]
pub struct Poller {
    pub(crate) api_base_url: String,
    pub(crate) environment_id: String,
    pub(crate) auth_token: String,
    pub(crate) worker_id: String,
    pub(crate) reclaim_older_than_ms: i64,
    pub(crate) http_client: reqwest::Client,
}

/// work 的类型专属数据。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct WorkData {
    /// 当前通常为 `session`。
    #[serde(rename = "type")]
    pub r#type: String,
    /// session ID。
    pub id: String,
}

/// `/work/poll` 返回的 work envelope。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct WorkResponse {
    /// work ID。
    pub id: String,
    /// 必须为 `work`。
    #[serde(rename = "type")]
    pub r#type: String,
    /// environment ID。
    pub environment_id: String,
    /// work 状态。
    pub state: String,
    /// work 数据。
    pub data: WorkData,
    /// 打包 secret。
    pub secret: String,
    /// 后端创建时间原文。
    pub created_at: String,
}

/// `/whoami` HTTP 客户端。
#[derive(Clone)]
pub struct WhoamiClient {
    pub(crate) api_base_url: String,
    pub(crate) auth_token: String,
    pub(crate) http_client: reqwest::Client,
}

/// `/whoami` 响应。
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct WhoamiResponse {
    /// 必须为 `environment_whoami`。
    #[serde(rename = "type")]
    pub r#type: String,
    /// organization UUID。
    pub organization_uuid: String,
    /// environment ID。
    pub environment_id: String,
}
