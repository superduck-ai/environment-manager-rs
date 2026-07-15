//! Manager 的默认 Tunnel client 与内置 control action 注册。
//!
//! 本文件机械对应 Go `internal/manager/tunnel_register.go`。HTTP(S) API URL 只替换
//! scheme，status/snapshot 始终注册；Baku 环境仅在 Vercel 或 Antspace 凭据完整时
//! 注册 deploy action，并按可用凭据选择默认部署目标。

use std::sync::Arc;

use url::Url;

use crate::internal::{
    auth::AuthContext,
    tunnel::{
        self,
        actions::{
            deploy::{new_antspace_client, new_deploy_action},
            new_registry,
            snapshot::new_snapshot_action,
            status::new_status_action,
        },
    },
};

use super::zz_recovered_types::{CreateTunnelClientFunc, Manager, ManagerError, TunnelClient};

/// 创建 Manager 构造函数安装的默认 tunnel factory。
///
/// Go 依赖包级 `init` 修改可变函数指针；Rust 在 `Manager` 构造期间显式取得该
/// factory，从而让初始化顺序可见、可测试，并避免全局可变状态。
#[must_use]
pub(super) fn default_create_tunnel_client_func() -> CreateTunnelClientFunc {
    Arc::new(create_tunnel_client)
}

/// 解析 API URL、注册 control actions，并构造真实 WebSocket tunnel client。
pub(super) fn create_tunnel_client(
    manager: &Manager,
    api_base_url: &str,
    auth_token: &str,
    auth_context: Option<&AuthContext>,
) -> Result<Arc<dyn TunnelClient>, ManagerError> {
    let mut parsed = Url::parse(api_base_url)
        .map_err(|error| ManagerError::message(format!("parse tunnel API base URL: {error}")))?;
    let scheme = parsed.scheme().to_owned();
    match scheme.as_str() {
        "https" => parsed
            .set_scheme("wss")
            .map_err(|()| ManagerError::message("parse tunnel API base URL: invalid scheme"))?,
        "http" => parsed
            .set_scheme("ws")
            .map_err(|()| ManagerError::message("parse tunnel API base URL: invalid scheme"))?,
        _ => {}
    }

    let logger = manager.logger.clone();
    let registry = Arc::new(new_registry(Some(logger.clone())));
    registry.register(Arc::new(new_status_action(None)));
    registry.register(Arc::new(new_snapshot_action(Some(logger.clone()))));

    let environment_sub_type = manager
        .session_config
        .as_ref()
        .and_then(|session| session.startup_context.as_ref())
        .map_or("", |context| context.environment_sub_type.as_str());
    if environment_sub_type == "baku" {
        let vercel_token = auth_context
            .map(AuthContext::get_vercel_deploy_token)
            .unwrap_or_default()
            .to_owned();
        let antspace_client = auth_context.and_then(|context| {
            let control_plane_url = context.get_antspace_control_plane_url();
            let antspace_auth_token = context.get_antspace_auth_token();
            if control_plane_url.is_empty() || antspace_auth_token.is_empty() {
                return None;
            }
            Some(new_antspace_client(
                control_plane_url,
                antspace_auth_token,
                Some(logger.clone()),
            ))
        });
        let default_target = if vercel_token.is_empty() && antspace_client.is_some() {
            "antspace"
        } else {
            "vercel"
        };

        if !vercel_token.is_empty() || antspace_client.is_some() {
            registry.register(Arc::new(new_deploy_action(
                Some(logger.clone()),
                vercel_token,
                &manager.organization_uuid,
                antspace_client,
                default_target,
            )));
        } else {
            manager.logger.info(
                "deploy action disabled",
                [(
                    "environment_sub_type".to_owned(),
                    serde_json::Value::String(environment_sub_type.to_owned()),
                )]
                .into_iter()
                .collect(),
            );
        }
    }

    let session_id = manager
        .session_config
        .as_ref()
        .map_or("", |session| session.session_id.as_str());
    let client = tunnel::new_client(
        Some(logger),
        session_id,
        parsed.to_string(),
        auth_token,
        vec![tunnel::with_action_registry(registry)],
    )
    .map_err(|error| ManagerError::message(error.to_string()))?;
    let client: Arc<dyn TunnelClient> = client;
    Ok(client)
}
