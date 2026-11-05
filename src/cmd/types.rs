//! 命令层共享类型。
//!
//! TaskContext 和 stdin client 承载 task-run 已接线的 v0/v1 统一上下文。

use crate::internal::{
    api::GetEnvironmentConfigResponse, auth::AuthContext, claude::Outcomes, config::StartupContext,
};

/// stdin v0/v1 解析后交给 `task-run` 主流程的统一上下文。
///
/// 指针字段使用 `Option` 保留 healthcheck 和错误注入测试中的
/// `None` 语义；普通 session 在进入 manager 前会再次完成必需字段校验。
#[derive(Clone, Debug)]
pub(super) struct TaskContext {
    /// 会话启动配置；healthcheck 也至少携带仅含 API base URL 的上下文。
    pub(super) startup_context: Option<StartupContext>,
    /// stdin 提供的环境配置；healthcheck 为 `None`。
    pub(super) environment: Option<GetEnvironmentConfigResponse>,
    /// 已解析且脱敏 Debug 的认证上下文。
    pub(super) auth_context: Option<AuthContext>,
    /// repository 到允许分支的映射。
    pub(super) outcomes: Option<Outcomes>,
    /// stdin 覆盖后的实际 session ID。
    pub(super) session_id: String,
    /// v1 work ID；v0 为空。
    pub(super) work_id: String,
    /// v1 environment ID；v0 为空。
    pub(super) environment_id: String,
    /// 是否只执行 ACK 和健康检查。
    pub(super) is_healthcheck: bool,
}

/// 把 stdin 中已经取得的配置适配成 manager 的 API client。
///
/// 它不再次访问网络，所有 getter 返回深拷贝，以免
/// manager 对 session ID 或环境配置的修改反向污染原始 stdin 上下文。
pub(super) struct StdinConfigClient {
    /// stdin 环境配置；普通 session 必须存在。
    pub(super) config: Option<GetEnvironmentConfigResponse>,
    /// stdin/work secret 的认证上下文。
    pub(super) auth_context: Option<AuthContext>,
    /// 已校验 outcomes。
    pub(super) outcomes: Option<Outcomes>,
}
