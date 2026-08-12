//! 环境类型注册、生命周期和 lease provider 的公共类型。
//!
//! 对应 Go：`internal/envtype/envtype.go` 中的 `EnvironmentType`、`Factory`、
//! `Registration` 和 `EnvTypes` 声明，以及 manager 对 BYOC lease provider 的接口断言。
//! Rust 使用 async trait 表达初始化与 lease 生命周期。

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::internal::auth::AuthContext;
use crate::internal::config::{SessionMode, StartupContext};
use crate::internal::session::ActivityRecorder;
use crate::internal::util::BoxError;

/// 单个具体环境在 session 启动阶段提供的统一生命周期接口。
#[async_trait]
pub trait EnvironmentType: Send + Sync {
    /// 更新当前 session 模式；实现据此决定是否执行一次性 setup 脚本。
    fn set_session_mode(&mut self, mode: SessionMode);

    /// 保存解析后的启动上下文；`None` 表示调用方没有提供上下文。
    fn set_startup_context(&mut self, context: Option<StartupContext>);

    /// 保存认证上下文；实现不得把其中的 token 输出到日志或 Debug。
    fn set_auth_context(&mut self, context: Option<AuthContext>);

    /// 初始化环境；取消后不得启动新的外部命令或网络操作。
    async fn initialize(
        &mut self,
        cancellation: &CancellationToken,
        activity_recorder: Option<&(dyn ActivityRecorder + Send + Sync)>,
    ) -> Result<(), BoxError>;

    /// 返回环境声明的工作目录；空字符串表示沿用当前目录。
    fn get_cwd(&self) -> &str;

    /// 返回启动 Claude Code 时追加的环境变量。
    fn get_claude_environment_variables(&self) -> BTreeMap<String, String>;

    /// 动态暴露当前环境可选的 lease 能力。
    ///
    /// Go 可以直接对 interface 执行 `env.(LeaseManagerProvider)`；Rust 的
    /// `dyn EnvironmentType` 不会因为具体类型还实现了另一个 trait 而自动支持横向
    /// 转换，因此在基础 trait 上提供显式查询入口。普通环境沿用默认 `None`，BYOC
    /// 返回自身的 provider，manager 无需依赖具体环境类型或启动层手工 adapter。
    fn lease_manager_provider(&self) -> Option<&dyn LeaseManagerProvider> {
        None
    }
}

/// 对应 Go：`internal/envtype/envtype.go` 中的 `Factory`。
///
/// 环境工厂接收后端返回的原始 JSON 配置。
///
/// # 返回
/// 成功时返回可动态分发的环境；配置解析或校验失败时原样返回工厂错误。
pub type Factory =
    Arc<dyn Fn(&[u8]) -> Result<Box<dyn EnvironmentType>, BoxError> + Send + Sync + 'static>;

/// 单个环境类型的注册信息。
#[derive(Clone)]
pub struct Registration {
    /// 配置中使用的稳定环境类型名称。
    pub type_name: String,
    /// 从原始 JSON 创建实现；`None` 只用于保留注册校验语义。
    pub factory: Option<Factory>,
    /// 面向用户的环境类型说明。
    pub description: String,
}

impl fmt::Debug for Registration {
    /// 输出元数据和工厂是否存在，不展开闭包捕获内容。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registration")
            .field("type_name", &self.type_name)
            .field("has_factory", &self.factory.is_some())
            .field("description", &self.description)
            .finish()
    }
}

/// 按稳定名称保存环境工厂的注册表。
#[derive(Debug, Default)]
pub struct EnvTypes {
    pub(crate) registry: HashMap<String, Registration>,
}

/// manager 与具体 podmonitor 实现之间的最小异步边界。
#[async_trait]
pub trait LeaseManagerHandle: Send + Sync {
    /// 发送初始 heartbeat 并启动后台监控。
    async fn start(&self) -> Result<(), BoxError>;

    /// 发出取消并等待后台监控停止。
    async fn stop(&self);

    /// 返回 lease 自己的执行取消 token。
    ///
    /// 该 token 必须来自实际 lease 状态机，而不是调用层临时创建的无关联 token；
    /// manager 会从它继续派生 executor/tunnel 的 direct child。
    fn cancellation_token(&self) -> CancellationToken;
}

/// 能为当前环境构造 lease manager 的可替换接口。
pub trait LeaseManagerProvider: Send + Sync {
    /// 构造 lease manager；具体 heartbeat 状态机由 podmonitor 模块负责。
    fn create_lease_manager(
        &self,
        cancellation: &CancellationToken,
        session_id: &str,
        environment_id: &str,
        api_base_url: &str,
        session_ingress_token: &str,
        organization_uuid: &str,
    ) -> Result<Arc<dyn LeaseManagerHandle>, BoxError>;
}
