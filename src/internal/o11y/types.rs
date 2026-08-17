//! 可观测性配置、服务 trait、上下文和静态指标定义。
//!
//! 配置用 `Option` 表达可缺省参数，用 `Arc<dyn O11yService>` 支持跨任务共享，
//! 并以 `Cow<'static, str>` 同时容纳静态
//! 指标和 `record_duration` 动态派生的指标名。

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    error::Error,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use opentelemetry::metrics::{Counter, Gauge, Meter};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tokio_util::sync::CancellationToken;

/// 指标标签集合；有序 map 让 OTel attributes 与单元测试输出保持确定性。
pub type MetricTags = BTreeMap<String, String>;

/// 可在线程间共享并在函数结束前最后写入的错误。
pub type SharedMetricError = Arc<Mutex<Option<Arc<dyn Error + Send + Sync + 'static>>>>;

/// OTel 初始化、关闭和内部 instrument cache 的统一错误。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct O11yError {
    message: String,
}

impl O11yError {
    /// 从已经带完整上下文的稳定文案构造错误。
    pub(crate) fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// OpenTelemetry 配置。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct O11yConfig {
    /// OTLP/HTTP metrics 完整 endpoint，包括 `/v1/metrics`。
    pub metrics_endpoint: String,
    /// 预留的 OTLP/HTTP logs endpoint；应用日志由 tracing subscriber 处理。
    pub logs_endpoint: String,
    /// metrics 与 logs 共享的请求头。
    pub headers: BTreeMap<String, String>,
    /// 预留的日志导出开关。
    pub enable_logging: bool,
    /// 是否用 stdout exporter 取代 OTLP exporter；该选项优先级更高。
    pub use_stdout_metrics: bool,
}

/// 一次函数调用会派生 `.count` 与 `.latency_ms` 两个指标。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct O11yFunctionMetric {
    /// 不包含派生后缀的基础名称。
    pub name: Cow<'static, str>,
    /// 面向运维人员的基础描述。
    pub description: Cow<'static, str>,
}

impl O11yFunctionMetric {
    /// 构造可使用静态字符串或 owned 字符串的函数指标。
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
        }
    }
}

/// 单个 OTel counter 或 gauge 的定义。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct O11yMetric {
    /// OTel instrument 名称。
    pub name: Cow<'static, str>,
    /// OTel instrument 描述。
    pub description: Cow<'static, str>,
    /// UCUM 单位；计数器使用 `1`，耗时使用 `ms`。
    pub unit: Cow<'static, str>,
}

impl O11yMetric {
    /// 构造一个完整指标定义。
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        description: impl Into<Cow<'static, str>>,
        unit: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            unit: unit.into(),
        }
    }
}

/// 提供一组可覆盖合并的指标标签。
pub trait TagProvider: Send + Sync {
    /// 返回 provider 当前标签的 owned 快照。
    fn tags(&self) -> MetricTags;
}

/// O11y 服务抽象；所有记录方法都保持无返回值，instrument 故障只写日志。
#[async_trait]
pub trait O11yService: Send + Sync {
    /// 刷新并关闭所有 exporter；取消 token 只约束等待，不改变关闭顺序。
    async fn shutdown(&self, cancellation: &CancellationToken) -> Result<(), O11yError>;

    /// 将 counter 增加 1；缺少指标定义时静默 no-op。
    fn increment(
        &self,
        cancellation: &CancellationToken,
        metric: Option<&O11yMetric>,
        tags: Option<&MetricTags>,
    );

    /// 写入同步 gauge；缺少指标定义时静默 no-op。
    fn record_gauge(
        &self,
        cancellation: &CancellationToken,
        metric: Option<&O11yMetric>,
        value: f64,
        tags: Option<&MetricTags>,
    );
}

/// 可克隆的 O11y 上下文。
#[derive(Clone)]
pub struct O11yContext {
    pub(crate) cancellation: CancellationToken,
    pub(crate) service: Arc<dyn O11yService>,
}

impl O11yContext {
    /// 创建仅含 discard 服务的后台上下文。
    pub fn background() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            service: Arc::new(super::DiscardO11yService),
        }
    }

    /// 用指定取消树和服务构造上下文，主要供应用装配与测试替身使用。
    pub fn with_service(cancellation: CancellationToken, service: Arc<dyn O11yService>) -> Self {
        Self {
            cancellation,
            service,
        }
    }

    /// 返回上下文携带的 session 取消 token。
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// 实际 O11y 服务。
pub struct O11yServiceImpl {
    pub(crate) metrics_provider: Option<Arc<O11yMetricsProvider>>,
}

/// 已创建 instrument 的强类型缓存值。
#[derive(Clone)]
pub(crate) enum CachedInstrument {
    /// 单调递增的 `u64` counter。
    Counter(Counter<u64>),
    /// 同步 `f64` gauge。
    Gauge(Gauge<f64>),
}

/// OTel meter provider 及其 instrument 缓存。
pub(crate) struct O11yMetricsProvider {
    pub(crate) meter_provider: Option<SdkMeterProvider>,
    pub(crate) instrument_cache: Mutex<HashMap<String, CachedInstrument>>,
    pub(crate) meter: Mutex<Option<Meter>>,
}

/// exporter 的强类型联合，避免初始化路径中的错误 downcast。
pub(crate) enum MetricsExporter {
    /// OTLP/HTTP protobuf exporter。
    Otlp(opentelemetry_otlp::MetricExporter),
    /// 人类可读 stdout exporter。
    Stdout(opentelemetry_stdout::MetricExporter),
}

/// 环境管理器启动次数。
pub static ENV_MANAGER_START_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("env_manager.start.count"),
    description: Cow::Borrowed("Number of times the environment manager started"),
    unit: Cow::Borrowed("1"),
};

/// 环境管理器结束次数（无论成功或失败）。
pub static ENV_MANAGER_END_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("env_manager.end.count"),
    description: Cow::Borrowed(
        "Number of times the environment manager exited (successfully or not)",
    ),
    unit: Cow::Borrowed("1"),
};

/// Claude Code 启动次数。
pub static CLAUDE_CODE_START_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("claude_code.start.count"),
    description: Cow::Borrowed("Number of times Claude Code started"),
    unit: Cow::Borrowed("1"),
};

/// Claude Code 结束次数（无论成功或失败）。
pub static CLAUDE_CODE_END_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("claude_code.end.count"),
    description: Cow::Borrowed("Number of times Claude Code exited (successfully or not)"),
    unit: Cow::Borrowed("1"),
};

/// Git checkout 操作的 count/latency 基础定义。
pub static GIT_CHECKOUT_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("git.checkout"),
    description: Cow::Borrowed("Git checkout operation"),
};

/// orchestrator poll 尝试次数。
pub static ORCHESTRATOR_POLL_ATTEMPT_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("orchestrator.poll.attempt.count"),
    description: Cow::Borrowed("Number of orchestrator poll attempts"),
    unit: Cow::Borrowed("1"),
};

/// orchestrator poll 错误次数。
pub static ORCHESTRATOR_POLL_ERROR_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("orchestrator.poll.error.count"),
    description: Cow::Borrowed("Number of orchestrator poll errors"),
    unit: Cow::Borrowed("1"),
};

/// orchestrator poll 请求耗时，单位毫秒。
pub static ORCHESTRATOR_POLL_DURATION_METRIC: O11yMetric = O11yMetric {
    name: Cow::Borrowed("orchestrator.poll.duration_ms"),
    description: Cow::Borrowed("Duration of orchestrator poll requests"),
    unit: Cow::Borrowed("ms"),
};

/// orchestrator 空队列结果次数。
pub static ORCHESTRATOR_QUEUE_EMPTY_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("orchestrator.queue.empty.count"),
    description: Cow::Borrowed("Number of empty queue poll results"),
    unit: Cow::Borrowed("1"),
};

/// orchestrator session 启动次数。
pub static ORCHESTRATOR_SESSION_START_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("orchestrator.session.start.count"),
    description: Cow::Borrowed("Number of orchestrator sessions started"),
    unit: Cow::Borrowed("1"),
};

/// orchestrator session 结束次数。
pub static ORCHESTRATOR_SESSION_END_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("orchestrator.session.end.count"),
    description: Cow::Borrowed("Number of orchestrator sessions ended"),
    unit: Cow::Borrowed("1"),
};

/// orchestrator 主循环超时次数。
pub static ORCHESTRATOR_TIMEOUT_COUNTER: O11yMetric = O11yMetric {
    name: Cow::Borrowed("orchestrator.timeout.count"),
    description: Cow::Borrowed("Number of orchestrator loop timeouts"),
    unit: Cow::Borrowed("1"),
};

/// 环境管理器整体启动的 count/latency 基础定义。
pub static STARTUP_TOTAL_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.startup.total"),
    description: Cow::Borrowed("Total environment manager startup"),
};

/// Claude Code 安装或升级的 count/latency 基础定义。
pub static CLAUDE_INSTALL_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.claude_install"),
    description: Cow::Borrowed("Claude Code installation/upgrade"),
};

/// 环境初始化的 count/latency 基础定义。
pub static ENV_INIT_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.env_init"),
    description: Cow::Borrowed("Overall environment initialization"),
};

/// MCP 注册的 count/latency 基础定义。
pub static MCP_REGISTRATION_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.mcp_registration"),
    description: Cow::Borrowed("MCP server registration"),
};

/// plugin marketplace 安装的 count/latency 基础定义。
pub static PLUGIN_MARKETPLACE_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.plugin_marketplace"),
    description: Cow::Borrowed("Plugin marketplace installation"),
};

/// source 处理的 count/latency 基础定义。
pub static SOURCES_PROCESSING_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.sources_processing"),
    description: Cow::Borrowed("Source (git repo) processing"),
};

/// Git proxy 配置的 count/latency 基础定义。
pub static GIT_PROXY_SETUP_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.git_proxy_setup"),
    description: Cow::Borrowed("Git proxy setup"),
};

/// 语言环境配置的 count/latency 基础定义。
pub static LANGUAGE_SETUP_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.language_setup"),
    description: Cow::Borrowed("Language setup"),
};

/// 用户 init script 的 count/latency 基础定义。
pub static INIT_SCRIPT_METRIC: O11yFunctionMetric = O11yFunctionMetric {
    name: Cow::Borrowed("env_manager.init_script"),
    description: Cow::Borrowed("User-provided init script execution"),
};
