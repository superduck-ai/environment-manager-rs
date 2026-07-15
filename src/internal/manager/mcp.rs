//! Manager 启动阶段的 MCP registration 选择、server 构造和非致命降级。
//!
//! 本文件机械对应 Go `internal/manager/mcp.go`。Registrations 按全局注册顺序串行
//! 启动，单个 server 失败只记录诊断并继续；返回值仍保留全部 registrations，供
//! Claude executor 生成工具策略和附加 system prompt。

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    auth::AuthContext,
    logger::LogAttributes,
    mcp::{self, MCPServer, MCPServerRegistration},
    session::ActivityRecorder,
};

use super::zz_recovered_types::{
    Manager, ManagerError, McpRegistrationBatch, working_directory_path,
};

impl Manager {
    /// 选择当前会话启用的 registrations，并依次创建、启动和注册 MCP servers。
    ///
    /// 任一 factory 失败都与 Go 一样是非致命错误：记录
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

        self.logger.info(
            "Starting MCP server registrations",
            attributes([("server_count", json!(registrations.len()))]),
        );

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
                    self.logger.warn(
                        "Failed to setup MCP server",
                        attributes([
                            ("server_name", json!(&registration.name)),
                            ("error", json!(error.to_string())),
                        ]),
                    );
                }
            }
        }

        self.logger.info(
            "Completed MCP server registrations",
            attributes([
                ("duration_ms", json!(duration_milliseconds(started))),
                ("registered_count", json!(servers.len())),
                ("attempted_count", json!(registrations.len())),
            ]),
        );
        McpRegistrationBatch {
            servers,
            registrations,
        }
    }

    /// 调用单个 registration factory，并保留 Go 的阶段日志和错误包装文本。
    pub(super) async fn setup_mcp_server_with_registration(
        &self,
        cancellation: &CancellationToken,
        registration: &Arc<MCPServerRegistration>,
        auth_context: &AuthContext,
        working_directory: &str,
        activity_recorder: Arc<dyn ActivityRecorder>,
    ) -> Result<Arc<dyn MCPServer>, ManagerError> {
        let started = Instant::now();
        self.logger.info(
            "Starting MCP server setup",
            attributes([
                ("server_name", json!(&registration.name)),
                ("description", json!(&registration.description)),
            ]),
        );

        let startup_context = self
            .session_config
            .as_ref()
            .and_then(|session| session.startup_context.clone())
            .map(Arc::new);
        let result = (registration.factory)(
            cancellation.clone(),
            self.logger.clone(),
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
                self.logger.warn(
                    "Failed to create and start MCP server",
                    attributes([
                        ("server_name", json!(&registration.name)),
                        ("duration_ms", json!(duration_ms)),
                        ("error", json!(error.to_string())),
                    ]),
                );
                return Err(ManagerError::message(format!(
                    "failed to create and start {} MCP server: {error}",
                    registration.name
                )));
            }
        };

        self.logger.info(
            "Successfully setup MCP server",
            attributes([
                ("server_name", json!(&registration.name)),
                ("description", json!(&registration.description)),
                ("duration_ms", json!(duration_ms)),
            ]),
        );
        Ok(server)
    }
}

/// 把成对字段收集为 logger 使用的稳定排序属性。
fn attributes<const N: usize>(entries: [(&str, Value); N]) -> LogAttributes {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// 将单调时钟耗时饱和转换为日志协议使用的有符号毫秒数。
fn duration_milliseconds(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}
