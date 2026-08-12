//! Supabase MCP registration 与 auth/startup 配置。
//!
//! PAT 是唯一硬性认证前置条件；已有 project ref 时立即构造 Management API client，
//! 否则 server 等待 CCR credentials endpoint。仅 baku 环境启用，且环境变量
//! `SUPABASE_MCP_DISABLED` 必须恰好等于 `true` 才关闭。

use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use serde_json::json;

use crate::internal::{
    auth::AuthContext,
    config::{ClaudeConfig, StartupContext},
    mcp::{
        BaseServer, MCPServer, MCPServerConfigurator, MCPServerRegistration, McpError, McpResult,
        new_registration, types::log_attributes,
    },
    session::ActivityRecorder,
};

use super::{Client, SupabaseMCPServer, client::DEFAULT_MANAGEMENT_API_BASE_URL};

static REGISTRATION: OnceLock<Arc<MCPServerRegistration>> = OnceLock::new();

/// 返回 Supabase registration，并在首次调用时加入全局 MCP 注册表。
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
            "supabase",
            "Supabase database management: migrations, SQL queries, and TypeScript type generation",
            Some(configurator),
            Arc::new(should_register),
            Arc::new(String::new),
        )
    }))
}

/// 从 auth/startup context 创建 Supabase server。
pub async fn configure_server(
    base_server: Arc<BaseServer>,
    auth_context: Option<Arc<AuthContext>>,
    startup_context: Option<Arc<StartupContext>>,
    working_dir: PathBuf,
    _activity_recorder: Arc<dyn ActivityRecorder>,
) -> McpResult<Arc<SupabaseMCPServer>> {
    let Some(auth_context) = auth_context else {
        return Err(McpError::message(
            "supabase PAT not configured in auth context",
        ));
    };
    let pat = auth_context.get_supabase_pat().to_owned();
    if pat.is_empty() {
        return Err(McpError::message(
            "supabase PAT not configured in auth context",
        ));
    }
    let api_base_url = startup_context
        .as_deref()
        .map(|context| context.api_base_url.clone())
        .unwrap_or_default();
    let lazy_mode = startup_context.as_deref().is_some_and(|context| {
        context
            .environment_variables
            .get("_BAKU_SUPABASE_LAZY")
            .is_some_and(|value| !value.is_empty())
    });
    let client = if auth_context.get_supabase_project_ref().is_empty() {
        None
    } else {
        Some(Arc::new(
            Client::new(
                pat.clone(),
                auth_context.get_supabase_project_ref(),
                DEFAULT_MANAGEMENT_API_BASE_URL,
            )
            .map_err(|error| {
                McpError::message(format!("failed to create supabase client: {error}"))
            })?,
        ))
    };

    if auth_context.get_supabase_project_ref().is_empty() {
        base_server.logger.info(
            "Supabase MCP server started (credentials pending)",
            log_attributes([("working_dir", json!(working_dir.to_string_lossy()))]),
        );
    } else {
        base_server.logger.info(
            "Supabase MCP server configured",
            log_attributes([
                (
                    "project_ref",
                    json!(auth_context.get_supabase_project_ref()),
                ),
                ("working_dir", json!(working_dir.to_string_lossy())),
            ]),
        );
    }

    Ok(Arc::new(SupabaseMCPServer::new(
        base_server,
        client,
        pat,
        working_dir,
        auth_context.get_session_id(),
        api_base_url,
        auth_context.get_session_ingress_token(),
        lazy_mode,
    )))
}

/// 判断当前启动上下文是否应注册 Supabase MCP。
#[must_use]
pub fn should_register(
    startup_context: Option<&StartupContext>,
    _claude_config: Option<&ClaudeConfig>,
) -> bool {
    if std::env::var("SUPABASE_MCP_DISABLED").as_deref() == Ok("true") {
        return false;
    }
    startup_context.is_some_and(|context| context.environment_sub_type == "baku")
}
