//! code-sign registration 与启动配置解析。
//!
//! `SIGNING_SERVER_URL` 非空时原样优先；否则使用 startup API base URL，缺少 scheme
//! 时按 Go 行为补 `http://`，并移除所有尾随 `/`。

use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use crate::internal::{
    auth::AuthContext,
    config::{ClaudeConfig, StartupContext},
    mcp::{
        BaseServer, MCPServer, MCPServerConfigurator, MCPServerRegistration, McpError, McpResult,
        new_registration,
    },
    session::ActivityRecorder,
};

use super::CodeSignMCPServer;

static REGISTRATION: OnceLock<Arc<MCPServerRegistration>> = OnceLock::new();

/// 返回 code-sign registration，并在首次调用时加入全局 MCP 注册表。
#[must_use]
pub fn registration() -> Arc<MCPServerRegistration> {
    Arc::clone(REGISTRATION.get_or_init(|| {
        let configurator: MCPServerConfigurator = Arc::new(
            |base_server, auth_context, startup_context, working_dir, activity_recorder| {
                Box::pin(async move {
                    configure_server(
                        base_server,
                        auth_context,
                        startup_context,
                        working_dir,
                        activity_recorder,
                    )
                    .await
                    .map(|server| server as Arc<dyn MCPServer>)
                })
            },
        );
        new_registration(
            "codesign",
            "Code signing MCP server for SSH-style Ed25519 signatures used in git commits and authentication",
            Some(configurator),
            Arc::new(should_register),
            Arc::new(String::new),
        )
    }))
}

/// 根据环境变量/startup context 创建 code-sign server。
pub async fn configure_server(
    base_server: Arc<BaseServer>,
    auth_context: Option<Arc<AuthContext>>,
    startup_context: Option<Arc<StartupContext>>,
    working_dir: PathBuf,
    _activity_recorder: Arc<dyn ActivityRecorder>,
) -> McpResult<Arc<CodeSignMCPServer>> {
    let signing_server_url = match std::env::var("SIGNING_SERVER_URL") {
        Ok(value) if !value.is_empty() => {
            tracing::info!("url" = ?(&value), "{}", "Using signing server URL from environment");
            value
        }
        _ => {
            let api_base_url = startup_context
                .as_deref()
                .map(|context| context.api_base_url.as_str())
                .unwrap_or_default();
            if api_base_url.is_empty() {
                return Err(McpError::message(
                    "API base URL is required for code signing server",
                ));
            }
            let value =
                if api_base_url.starts_with("http://") || api_base_url.starts_with("https://") {
                    api_base_url.to_owned()
                } else {
                    format!("http://{api_base_url}")
                };
            let value = value.trim_end_matches('/').to_owned();
            tracing::info!("url" = ?(&value), "{}", "Using API base URL for code signing");
            value
        }
    };

    Ok(Arc::new(CodeSignMCPServer::new(
        base_server,
        auth_context,
        working_dir,
        signing_server_url,
        startup_context,
    )))
}

/// code-sign 在全部启动上下文中启用。
#[must_use]
pub fn should_register(
    _startup_context: Option<&StartupContext>,
    _claude_config: Option<&ClaudeConfig>,
) -> bool {
    true
}
