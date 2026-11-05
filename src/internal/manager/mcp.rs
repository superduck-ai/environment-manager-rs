//! Manager 启动阶段的 MCP registration 选择、server 构造和非致命降级。
//!
//! Registrations 按全局注册顺序串行启动，单个 server 失败只记录诊断并继续；返回值仍保留全部 registrations，供
//! Claude executor 生成工具策略和附加 system prompt。

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use tokio_util::sync::CancellationToken;

use crate::internal::{
    auth::AuthContext,
    mcp::{self, MCPServer, MCPServerRegistration},
    session::ActivityRecorder,
};

use super::types::{Manager, ManagerError, McpRegistrationBatch, working_directory_path};

impl Manager {
    /// 选择当前会话启用的 registrations，并依次创建、启动和注册 MCP servers。
    ///
    /// 任一 factory 失败都是非致命错误：记录
    /// `mcp_server_setup_failed_nonfatal` 后继续下一项，最终 batch 本身仍成功返回。
    pub(super) async fn register_mcp_servers(
        &self,
        cancellation: &CancellationToken,
        auth_context: &AuthContext,
        working_directory: &str,
        activity_recorder: Arc<dyn ActivityRecorder>,
    ) -> McpRegistrationBatch {
        let started = Instant::now();
        let startup_context = self
            .session_config
            .as_ref()
            .and_then(|session| session.startup_context.as_ref());
        let registrations =
            mcp::get_mcp_registrations(startup_context, self.claude_config.as_ref());

        tracing::info!("server_count" = ?(registrations.len()), "{}", "Starting MCP server registrations");

        let mut servers = Vec::new();
        for registration in &registrations {
            match self
                .setup_mcp_server_with_registration(
                    cancellation,
                    registration,
                    auth_context,
                    working_directory,
                    Arc::clone(&activity_recorder),
                )
                .await
            {
                Ok(server) => servers.push(server),
                Err(error) => {
                    self.log_diag("mcp_server_setup_failed_nonfatal", BTreeMap::new());
                    tracing::warn!("server_name" = ?(&registration.name), "error" = ?(error.to_string()), "{}", "Failed to setup MCP server");
                }
            }
        }

        tracing::info!("duration_ms" = ?(duration_milliseconds(started)), "registered_count" = ?(servers.len()), "attempted_count" = ?(registrations.len()), "{}", "Completed MCP server registrations");
        McpRegistrationBatch {
            servers,
            registrations,
        }
    }

    /// 调用单个 registration factory，并保留 阶段日志和错误包装文本。
    pub(super) async fn setup_mcp_server_with_registration(
        &self,
        cancellation: &CancellationToken,
        registration: &Arc<MCPServerRegistration>,
        auth_context: &AuthContext,
        working_directory: &str,
        activity_recorder: Arc<dyn ActivityRecorder>,
    ) -> Result<Arc<dyn MCPServer>, ManagerError> {
        let started = Instant::now();
        tracing::info!("server_name" = ?(&registration.name), "description" = ?(&registration.description), "{}", "Starting MCP server setup");

        let startup_context = self
            .session_config
            .as_ref()
            .and_then(|session| session.startup_context.clone())
            .map(Arc::new);
        let result = (registration.factory)(
            cancellation.clone(),
            Some(Arc::new(auth_context.clone())),
            working_directory_path(working_directory),
            startup_context,
            activity_recorder,
        )
        .await;
        let duration_ms = duration_milliseconds(started);
        let server = match result {
            Ok(server) => server,
            Err(error) => {
                tracing::warn!("server_name" = ?(&registration.name), "duration_ms" = ?(duration_ms), "error" = ?(error.to_string()), "{}", "Failed to create and start MCP server");
                return Err(ManagerError::message(format!(
                    "failed to create and start {} MCP server: {error}",
                    registration.name
                )));
            }
        };

        tracing::info!("server_name" = ?(&registration.name), "description" = ?(&registration.description), "duration_ms" = ?(duration_ms), "{}", "Successfully setup MCP server");
        Ok(server)
    }
}

/// 将单调时钟耗时饱和转换为日志协议使用的有符号毫秒数。
fn duration_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}
