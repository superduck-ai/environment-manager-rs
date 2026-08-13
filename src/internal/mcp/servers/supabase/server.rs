//! Supabase MCP tools、credentials 解析与 database provisioning。
//!
//! server 可在已有 project-ref 的 eager 模式运行，也可从 CCR API 延迟取得凭据。
//! 204 属于可重试状态，500 属于永久失败并缓存；lazy baku 模式额外提供
//! `provision_database`，以 2/4/8 秒退避轮询最多 90 秒。

use std::{
    fs::{DirBuilder, OpenOptions},
    io::Write as _,
    os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use chrono::Local;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    mcp::{
        CallToolRequest, CallToolResult, MCPServer, McpError, McpResult, ServerConfig, Tool,
        ToolConfig, ToolHandler, new_tool_result_error, new_tool_result_text,
    },
    util::go_path_escape,
};

use super::{
    Client, ProvisionStatus, SupabaseCredentials, SupabaseMCPServer,
    client::{DEFAULT_MANAGEMENT_API_BASE_URL, read_limited_response},
    functions::{is_function_slug, query_escape},
};

/// 永久 provisioning 失败的 sentinel 文本。
pub const ERR_PROVISIONING_FAILED: &str = "supabase provisioning failed";
const CONTROL_RESPONSE_LIMIT: usize = 1024;

/// 使用 `0755` 创建目录树，对齐 递归建目录。
///
/// 显式 mode 很重要：Rust `create_dir_all` 默认请求 `0777`，在宽松 umask 下会让
/// migrations 或生成类型的父目录额外可写。这里仍让内核应用当前 umask，且不会
/// 修改已经存在的目录权限，与 创建语义一致。
///
/// # 错误
/// 任一级目录不能创建，或路径组件被普通文件占用时返回底层 I/O 错误。
pub(super) fn create_dir_all_0755(path: &Path) -> std::io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true).mode(0o755).create(path)
}

/// 从带 sentinel 的错误文本中提取后端正文。
#[must_use]
pub fn permanent_provisioning_err_body(error: &str) -> String {
    let prefix = format!("{ERR_PROVISIONING_FAILED}: ");
    let body = error.strip_prefix(&prefix).unwrap_or(error);
    if body.is_empty() {
        "(no error details from server)".to_owned()
    } else {
        body.to_owned()
    }
}

impl SupabaseMCPServer {
    /// 若 workspace 有 `project/` 目录则使用它，否则使用 workspace 本身。
    #[must_use]
    pub fn get_project_dir(&self) -> PathBuf {
        let project_directory = self.base_working_dir.join("project");
        if project_directory.is_dir() {
            project_directory
        } else {
            self.base_working_dir.clone()
        }
    }

    /// 返回已有 client，或从 CCR credentials API 延迟配置它。
    pub async fn ensure_configured(
        &self,
        cancellation: &CancellationToken,
    ) -> McpResult<Arc<Client>> {
        {
            let state = self.state.read().await;
            if let Some(client) = &state.client {
                return Ok(Arc::clone(client));
            }
            if !state.provisioning_error.is_empty() {
                return Err(McpError::message(format!(
                    "{ERR_PROVISIONING_FAILED}: {}",
                    state.provisioning_error
                )));
            }
        }

        let credentials = match self.fetch_credentials(cancellation).await {
            Ok(credentials) => credentials,
            Err(error) if is_permanent_error(&error) => {
                let body = permanent_provisioning_err_body(&error.to_string());
                self.state.write().await.provisioning_error = body;
                tracing::error!("error" = ?(error.to_string()), "{}", "Supabase provisioning permanently failed");
                return Err(error);
            }
            Err(error) => {
                tracing::warn!("error" = ?(error.to_string()), "{}", "Supabase credentials not yet available");
                if self.lazy_mode {
                    return Err(McpError::message(
                        "no Supabase database has been provisioned for this session — if this app needs server-side persistence (user accounts, shared state, cross-device data), call the provision_database tool first; for client-side-only state, use localStorage instead",
                    ));
                }
                return Err(McpError::message(
                    "supabase database is still being provisioned, please try again in a moment",
                ));
            }
        };

        let mut state = self.state.write().await;
        if let Some(client) = &state.client {
            return Ok(Arc::clone(client));
        }
        let client = Arc::new(
            Client::new(
                self.pat.clone(),
                credentials.project_ref.clone(),
                DEFAULT_MANAGEMENT_API_BASE_URL,
            )
            .map_err(|error| McpError::message(format!("create supabase client: {error}")))?,
        );
        self.write_env_files(&credentials);
        state.client = Some(Arc::clone(&client));
        tracing::info!("project_ref" = ?(&credentials.project_ref), "{}", "Supabase credentials resolved via API");
        Ok(client)
    }

    /// 从 CCR control plane 读取本 session 的 Supabase credentials。
    pub async fn fetch_credentials(
        &self,
        cancellation: &CancellationToken,
    ) -> McpResult<SupabaseCredentials> {
        let api_base_url = normalize_api_base_url(&self.api_base_url);
        let endpoint = format!(
            "{api_base_url}/v2/ccr-sessions/{}/supabase-credentials",
            go_path_escape(&self.session_id)
        );
        let request = self
            .credentials_http_client
            .get(endpoint)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.session_ingress_token),
            )
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "ccr-byoc-2025-07-29");
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(McpError::message("http request: context canceled")),
            result = request.send() => result.map_err(|error| McpError::message(format!("http request: {error}")))?,
        };
        let status = response.status();
        match status {
            reqwest::StatusCode::NO_CONTENT => {
                Err(McpError::message("credentials not yet available (204)"))
            }
            reqwest::StatusCode::INTERNAL_SERVER_ERROR => {
                let body = read_limited_response(cancellation, response, CONTROL_RESPONSE_LIMIT)
                    .await
                    .unwrap_or_default();
                Err(McpError::message(format!(
                    "{ERR_PROVISIONING_FAILED}: {}",
                    String::from_utf8_lossy(&body)
                )))
            }
            reqwest::StatusCode::OK => {
                let bytes = tokio::select! {
                    _ = cancellation.cancelled() => return Err(McpError::message("decode response: context canceled")),
                    result = response.bytes() => result.map_err(|error| McpError::message(format!("decode response: {error}")))?,
                };
                let credentials: SupabaseCredentials = serde_json::from_slice(&bytes)
                    .map_err(|error| McpError::message(format!("decode response: {error}")))?;
                if credentials.project_ref.is_empty() {
                    return Err(McpError::message("empty project_ref in response"));
                }
                Ok(credentials)
            }
            _ => {
                let body = read_limited_response(cancellation, response, CONTROL_RESPONSE_LIMIT)
                    .await
                    .unwrap_or_default();
                Err(McpError::message(format!(
                    "unexpected status {}: {}",
                    status.as_u16(),
                    String::from_utf8_lossy(&body)
                )))
            }
        }
    }

    /// 以 0600 写入 browser 和 database 连接环境文件；失败只降级，不影响 client。
    pub fn write_env_files(&self, credentials: &SupabaseCredentials) {
        let project_directory = self.get_project_dir();
        let env = format!(
            "VITE_SUPABASE_URL=https://{}.supabase.co\nVITE_SUPABASE_ANON_KEY={}\n",
            credentials.project_ref, credentials.anon_key
        );
        let local_env = format!(
            "DATABASE_URL=postgresql://postgres:{}@db.{}.supabase.co:5432/postgres\n",
            query_escape(&credentials.db_pass),
            credentials.project_ref
        );
        if let Err(error) =
            write_file_with_mode(&project_directory.join(".env"), env.as_bytes(), 0o600)
        {
            tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to write .env file");
        }
        if let Err(error) = write_file_with_mode(
            &project_directory.join(".env.local"),
            local_env.as_bytes(),
            0o600,
        ) {
            tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to write .env.local file");
        }
    }

    /// 创建 eager 模式的五个工具；lazy 模式在首位追加 provisioning 工具。
    #[must_use]
    pub fn get_tools(self: &Arc<Self>) -> Vec<ToolConfig> {
        let mut tools = Vec::with_capacity(6);
        if self.lazy_mode {
            tools.push(tool_config(
                self,
                Tool::new(
                    "provision_database",
                    "Provisions a Postgres database for this app. Call this ONLY when the app needs server-side persistence: user accounts, multi-user shared state, data that must survive across devices or after browser storage is cleared. DO NOT call for: static content, single-user preferences (use localStorage), demo/mock data, client-side-only apps. Provisioning takes 30-60 seconds and creates real infrastructure.",
                    json!({"type": "object", "properties": {}}),
                ),
                |server, cancellation, request| async move {
                    server.handle_provision_database(&cancellation, &request).await
                },
            ));
        }
        tools.push(tool_config(
            self,
            Tool::new(
                "migrate",
                "Apply a SQL migration to the Supabase database. Creates a versioned migration file and automatically regenerates TypeScript types. Use this for all schema changes: CREATE TABLE, ALTER TABLE, RLS policies, functions, triggers.",
                json!({
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "The SQL to execute (CREATE TABLE, ALTER TABLE, CREATE POLICY, CREATE FUNCTION, etc.)"},
                        "name": {"type": "string", "description": "Short descriptive name for this migration (e.g., 'create_todos_table'). Auto-generated if omitted."}
                    },
                    "required": ["sql"]
                }),
            ),
            |server, cancellation, request| async move {
                server.handle_migrate(&cancellation, &request).await
            },
        ));
        tools.push(tool_config(
            self,
            Tool::new(
                "query",
                "Execute a SQL query against the Supabase database. Use for inspecting schema, checking data, seeding test data, or debugging. For schema changes, prefer the migrate tool instead.",
                json!({
                    "type": "object",
                    "properties": {"sql": {"type": "string", "description": "The SQL query to execute"}},
                    "required": ["sql"]
                }),
            ),
            |server, cancellation, request| async move {
                server.handle_query(&cancellation, &request).await
            },
        ));
        tools.push(tool_config(
            self,
            Tool::new(
                "list_migrations",
                "List all migrations that have been applied to the database.",
                json!({"type": "object", "properties": {}}),
            ),
            |server, cancellation, request| async move {
                server.handle_list_migrations(&cancellation, &request).await
            },
        ));
        tools.push(tool_config(
            self,
            Tool::new(
                "generate_types",
                "Regenerate TypeScript types from the current database schema. Usually called automatically after migrate, but can be called manually if needed.",
                json!({"type": "object", "properties": {}}),
            ),
            |server, cancellation, request| async move {
                server.handle_generate_types(&cancellation, &request).await
            },
        ));
        tools.push(tool_config(
            self,
            Tool::new(
                "deploy_function",
                "Deploy a Supabase Edge Function to this project. The function source must already be written to supabase/functions/{slug}/index.ts (use the Write tool first). Shared code goes in supabase/functions/_shared/. After deployment, invoke from React with supabase.functions.invoke('{slug}', { body: {...} }). Edge Functions run Deno server-side and can access secrets via Deno.env.get('NAME').",
                json!({
                    "type": "object",
                    "properties": {"slug": {"type": "string", "description": "Function slug (URL-safe name). Must match the directory name under supabase/functions/."}},
                    "required": ["slug"]
                }),
            ),
            |server, cancellation, request| async move {
                server.handle_deploy_function(&cancellation, &request).await
            },
        ));
        tools
    }

    /// 应用 migration、尽力写本地文件/生成 types，并通知 PostgREST reload。
    pub async fn handle_migrate(
        &self,
        cancellation: &CancellationToken,
        request: &CallToolRequest,
    ) -> McpResult<CallToolResult> {
        let client = match self.ensure_configured(cancellation).await {
            Ok(client) => client,
            Err(error) => return Ok(new_tool_result_error(error.to_string())),
        };
        let sql = match request.require_string("sql") {
            Ok(sql) => sql,
            Err(error) => {
                return Ok(new_tool_result_error(format!("sql is required: {error}")));
            }
        };
        if sql.is_empty() {
            return Ok(new_tool_result_error("sql must not be empty"));
        }
        let mut name = request.get_string("name", "");
        if name.is_empty() {
            let seconds = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            name = format!("migration_{seconds}");
        }
        name = Path::new(&name).file_name().map_or_else(
            || name.clone(),
            |value| value.to_string_lossy().into_owned(),
        );
        if !is_migration_name(&name) {
            return Ok(new_tool_result_error(
                "Migration name must contain only letters, numbers, and underscores",
            ));
        }
        tracing::info!("migration_name" = ?(&name), "sql_length" = ?(sql.len()), "{}", "Applying migration");
        if let Err(error) = client.apply_migration(cancellation, &name, &sql).await {
            tracing::error!("migration_name" = ?(&name), "error" = ?(error.to_string()), "{}", "Migration failed");
            return Ok(new_tool_result_error(format!("Migration failed: {error}")));
        }

        let mut migration_file_message = String::new();
        let migrations_directory = self.get_project_dir().join("supabase/migrations");
        if let Err(error) = create_dir_all_0755(&migrations_directory) {
            tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to create migrations dir");
            migration_file_message =
                format!("\nWarning: could not create migrations directory: {error}");
        } else {
            let timestamp = Local::now().format("%Y%m%d%H%M%S");
            let migration_path = migrations_directory.join(format!("{timestamp}_{name}.sql"));
            if let Err(error) =
                write_file_with_mode(&migration_path, format!("{sql}\n").as_bytes(), 0o644)
            {
                tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to write migration file");
                migration_file_message =
                    format!("\nWarning: migration applied but local file write failed: {error}");
            }
        }

        let mut types_message = String::new();
        match client.generate_types(cancellation).await {
            Err(error) => {
                tracing::warn!("error" = ?(error.to_string()), "{}", "Type generation failed after migration (non-fatal)");
                types_message = format!("\nWarning: type generation failed: {error}");
            }
            Ok(types) => {
                let types_path = self.get_project_dir().join("src/lib/database.types.ts");
                if let Some(parent) = types_path.parent()
                    && create_dir_all_0755(parent).is_ok()
                {
                    match write_file_with_mode(&types_path, types.as_bytes(), 0o644) {
                        Err(error) => {
                            tracing::warn!("error" = ?(error.to_string()), "{}", "Failed to write types file");
                            types_message = "\nWarning: failed to write types file".to_owned();
                        }
                        Ok(()) => {
                            types_message =
                                "\nTypeScript types regenerated at src/lib/database.types.ts"
                                    .to_owned();
                        }
                    }
                }
            }
        }
        if let Err(error) = client
            .run_query(cancellation, "NOTIFY pgrst, 'reload schema'")
            .await
        {
            tracing::warn!("error" = ?(error.to_string()), "{}", "PostgREST reload notification failed (non-fatal)");
        }
        tracing::info!("migration_name" = ?(&name), "{}", "Migration applied successfully");
        Ok(new_tool_result_text(format!(
            "Migration '{name}' applied successfully.{migration_file_message}{types_message}"
        )))
    }

    /// 执行 SQL 并在合法 JSON 时返回两空格 pretty-print 文本。
    pub async fn handle_query(
        &self,
        cancellation: &CancellationToken,
        request: &CallToolRequest,
    ) -> McpResult<CallToolResult> {
        let client = match self.ensure_configured(cancellation).await {
            Ok(client) => client,
            Err(error) => return Ok(new_tool_result_error(error.to_string())),
        };
        let sql = match request.require_string("sql") {
            Ok(sql) => sql,
            Err(error) => {
                return Ok(new_tool_result_error(format!("sql is required: {error}")));
            }
        };
        if sql.is_empty() {
            return Ok(new_tool_result_error("sql must not be empty"));
        }
        tracing::info!("sql_length" = ?(sql.len()), "{}", "Executing query");
        let mut result = match client.run_query(cancellation, &sql).await {
            Ok(result) => result,
            Err(error) => return Ok(new_tool_result_error(format!("Query failed: {error}"))),
        };
        if let Ok(mut value) = serde_json::from_str::<Value>(&result) {
            sort_json_object_keys(&mut value);
            if let Ok(pretty) = serde_json::to_string_pretty(&value) {
                result = pretty;
            }
        }
        Ok(new_tool_result_text(result))
    }

    /// 列出全部 applied migrations。
    pub async fn handle_list_migrations(
        &self,
        cancellation: &CancellationToken,
        _request: &CallToolRequest,
    ) -> McpResult<CallToolResult> {
        let client = match self.ensure_configured(cancellation).await {
            Ok(client) => client,
            Err(error) => return Ok(new_tool_result_error(error.to_string())),
        };
        let migrations = match client.list_migrations(cancellation).await {
            Ok(migrations) => migrations,
            Err(error) => {
                return Ok(new_tool_result_error(format!(
                    "Failed to list migrations: {error}"
                )));
            }
        };
        if migrations.is_empty() {
            return Ok(new_tool_result_text("No migrations have been applied yet."));
        }
        match serde_json::to_string_pretty(&migrations) {
            Ok(result) => Ok(new_tool_result_text(result)),
            Err(error) => Ok(new_tool_result_error(format!(
                "Failed to format migrations: {error}"
            ))),
        }
    }

    /// 重新生成 TypeScript types 并写到约定路径。
    pub async fn handle_generate_types(
        &self,
        cancellation: &CancellationToken,
        _request: &CallToolRequest,
    ) -> McpResult<CallToolResult> {
        let client = match self.ensure_configured(cancellation).await {
            Ok(client) => client,
            Err(error) => return Ok(new_tool_result_error(error.to_string())),
        };
        let types = match client.generate_types(cancellation).await {
            Ok(types) => types,
            Err(error) => {
                return Ok(new_tool_result_error(format!(
                    "Type generation failed: {error}"
                )));
            }
        };
        let types_path = self.get_project_dir().join("src/lib/database.types.ts");
        if let Some(parent) = types_path.parent()
            && let Err(error) = create_dir_all_0755(parent)
        {
            return Ok(new_tool_result_error(format!(
                "Failed to create types directory: {error}"
            )));
        }
        if let Err(error) = write_file_with_mode(&types_path, types.as_bytes(), 0o644) {
            return Ok(new_tool_result_error(format!(
                "Failed to write types file: {error}"
            )));
        }
        Ok(new_tool_result_text(
            "TypeScript types regenerated at src/lib/database.types.ts",
        ))
    }

    /// 验证 function source 后调用 multipart deploy API。
    pub async fn handle_deploy_function(
        &self,
        cancellation: &CancellationToken,
        request: &CallToolRequest,
    ) -> McpResult<CallToolResult> {
        let client = match self.ensure_configured(cancellation).await {
            Ok(client) => client,
            Err(error) => return Ok(new_tool_result_error(error.to_string())),
        };
        let slug = match request.require_string("slug") {
            Ok(slug) => slug,
            Err(error) => {
                return Ok(new_tool_result_error(format!("slug is required: {error}")));
            }
        };
        if slug.is_empty() {
            return Ok(new_tool_result_error("slug must not be empty"));
        }
        if !is_function_slug(&slug) {
            return Ok(new_tool_result_error(
                "slug must start with a letter and contain only letters, digits, underscores, hyphens",
            ));
        }
        let project_directory = self.get_project_dir();
        let function_directory = project_directory.join("supabase/functions").join(&slug);
        let shared_directory = project_directory.join("supabase/functions/_shared");
        if let Err(error) = std::fs::metadata(function_directory.join("index.ts")) {
            return Ok(new_tool_result_error(format!(
                "index.ts not found at supabase/functions/{slug}/ ({error}). Write the function source first."
            )));
        }
        tracing::info!("slug" = ?(&slug), "func_dir" = ?(function_directory.to_string_lossy()), "{}", "Deploying Edge Function");
        let result = match client
            .deploy_function(
                cancellation,
                &slug,
                &function_directory,
                Some(&shared_directory),
                &project_directory,
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::error!("slug" = ?(&slug), "error" = ?(error.to_string()), "{}", "Edge Function deploy failed");
                return Ok(new_tool_result_error(format!("Deploy failed: {error}")));
            }
        };
        tracing::info!("slug" = ?(&slug), "version" = ?(result.version), "status" = ?(&result.status), "{}", "Edge Function deployed");
        let function_url = format!(
            "https://{}.supabase.co/functions/v1/{slug}",
            client.project_ref()
        );
        Ok(new_tool_result_text(format!(
            "Edge Function '{slug}' deployed (version {}, status: {}).\nURL: {function_url}\nInvoke from React: supabase.functions.invoke('{slug}', {{ body: {{...}} }})",
            result.version, result.status
        )))
    }

    /// POST provisioning trigger，并区分 200/202/500。
    pub async fn trigger_provision(
        &self,
        cancellation: &CancellationToken,
    ) -> McpResult<ProvisionStatus> {
        let api_base_url = normalize_api_base_url(&self.api_base_url);
        let endpoint = format!(
            "{api_base_url}/v2/ccr-sessions/{}/supabase-provision",
            go_path_escape(&self.session_id)
        );
        let request = self
            .credentials_http_client
            .post(endpoint)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.session_ingress_token),
            )
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "ccr-byoc-2025-07-29");
        let response = tokio::select! {
            _ = cancellation.cancelled() => return Err(McpError::message("http request: context canceled")),
            result = request.send() => result.map_err(|error| McpError::message(format!("http request: {error}")))?,
        };
        let status = response.status();
        match status {
            reqwest::StatusCode::OK => Ok(ProvisionStatus::Already),
            reqwest::StatusCode::ACCEPTED => Ok(ProvisionStatus::Started),
            reqwest::StatusCode::INTERNAL_SERVER_ERROR => {
                let body = read_limited_response(cancellation, response, CONTROL_RESPONSE_LIMIT)
                    .await
                    .unwrap_or_default();
                Err(McpError::message(format!(
                    "{ERR_PROVISIONING_FAILED}: {}",
                    String::from_utf8_lossy(&body)
                )))
            }
            _ => {
                let body = read_limited_response(cancellation, response, CONTROL_RESPONSE_LIMIT)
                    .await
                    .unwrap_or_default();
                Err(McpError::message(format!(
                    "unexpected status {}: {}",
                    status.as_u16(),
                    String::from_utf8_lossy(&body)
                )))
            }
        }
    }

    /// 启动或等待按需 database provisioning。
    pub async fn handle_provision_database(
        &self,
        cancellation: &CancellationToken,
        _request: &CallToolRequest,
    ) -> McpResult<CallToolResult> {
        {
            let state = self.state.read().await;
            if let Some(client) = &state.client {
                return Ok(new_tool_result_text(format!(
                    "Database already provisioned (project: {}). .env and .env.local are in the project directory.",
                    client.project_ref()
                )));
            }
            if !state.provisioning_error.is_empty() {
                return Ok(new_tool_result_error(format!(
                    "Supabase provisioning permanently failed: {}. The database cannot be created for this session.",
                    state.provisioning_error
                )));
            }
        }

        tracing::info!("session_id" = ?(&self.session_id), "{}", "Triggering on-demand Supabase provisioning");
        let status = match self.trigger_provision(cancellation).await {
            Ok(status) => status,
            Err(error) => {
                tracing::error!("error" = ?(error.to_string()), "{}", "Failed to trigger database provisioning");
                if is_permanent_error(&error) {
                    let body = permanent_provisioning_err_body(&error.to_string());
                    self.state.write().await.provisioning_error = body.clone();
                    return Ok(new_tool_result_error(format!(
                        "Supabase provisioning permanently failed: {body}. The database cannot be created for this session."
                    )));
                }
                return Ok(new_tool_result_error(format!(
                    "Failed to trigger database provisioning: {error}"
                )));
            }
        };
        if status == ProvisionStatus::Already {
            return match self.ensure_configured(cancellation).await {
                Ok(client) => Ok(new_tool_result_text(format!(
                    "Database already provisioned (project: {}). .env and .env.local written to project directory.",
                    client.project_ref()
                ))),
                Err(error) => Ok(new_tool_result_error(format!(
                    "Database reported as provisioned but credentials unavailable: {error}"
                ))),
            };
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        let mut delay = Duration::from_secs(2);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Ok(new_tool_result_error("Database provisioning did not complete within 1m30s. It may still be in progress \n try calling any Supabase tool again in a minute to check."));
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Ok(new_tool_result_error("Database provisioning did not complete within 1m30s. It may still be in progress \n try calling any Supabase tool again in a minute to check."));
                }
                _ = tokio::time::sleep(delay) => {}
            }
            match self.ensure_configured(cancellation).await {
                Ok(client) => {
                    tracing::info!("project_ref" = ?(client.project_ref()), "{}", "Supabase provisioning complete");
                    return Ok(new_tool_result_text(format!(
                        "Database provisioned successfully (project: {}). .env and .env.local written to project directory. Use the migrate tool to create your schema.",
                        client.project_ref()
                    )));
                }
                Err(error) if is_permanent_error(&error) => {
                    return Ok(new_tool_result_error(format!(
                        "Supabase provisioning failed: {}",
                        permanent_provisioning_err_body(&error.to_string())
                    )));
                }
                Err(_) => {}
            }
            delay = (delay * 2).min(Duration::from_secs(8));
        }
    }

    /// Supabase 总是由 factory 注册到 Claude。
    #[must_use]
    pub fn should_register_with_claude(&self) -> bool {
        true
    }
}

#[async_trait]
impl MCPServer for SupabaseMCPServer {
    /// 返回基础 server 名称。
    fn get_name(&self) -> String {
        self.base_server.get_name()
    }

    /// 生成当前 lazy/eager 工具集合。
    fn get_tools(self: Arc<Self>) -> Vec<ToolConfig> {
        SupabaseMCPServer::get_tools(&self)
    }

    /// 启动基础 loopback server。
    async fn start(&self, tools: Vec<ToolConfig>) -> McpResult<ServerConfig> {
        self.base_server.start(tools).await
    }

    /// 停止基础 loopback server。
    async fn stop(&self) {
        self.base_server.stop().await;
    }

    /// 返回默认 true。
    fn should_register_with_claude(&self) -> bool {
        true
    }
}

/// 为捕获 Arc server 的异步 method 构造 ToolConfig。
fn tool_config<F, Fut>(server: &Arc<SupabaseMCPServer>, tool: Tool, handler: F) -> ToolConfig
where
    F: Fn(Arc<SupabaseMCPServer>, CancellationToken, CallToolRequest) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: std::future::Future<Output = McpResult<CallToolResult>> + Send + 'static,
{
    let server = Arc::clone(server);
    ToolConfig {
        tool,
        handler: ToolHandler::new(move |cancellation, request| {
            handler(Arc::clone(&server), cancellation, request)
        }),
    }
}

/// 判断错误是否携带永久 provisioning sentinel。
fn is_permanent_error(error: &McpError) -> bool {
    error.to_string().starts_with(ERR_PROVISIONING_FAILED)
}

/// API base URL 缺 scheme 时补生产所用的 `https://`。
fn normalize_api_base_url(value: &str) -> String {
    if value.starts_with("http://") || value.starts_with("https://") {
        value.to_owned()
    } else {
        format!("https://{value}")
    }
}

/// 无 regex 依赖地实现 `^[a-zA-Z0-9_]+$`。
fn is_migration_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// 递归按 key 排序 object，使 pretty JSON 的对象顺序稳定。
fn sort_json_object_keys(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            for (_, child) in &mut entries {
                sort_json_object_keys(child);
            }
            object.extend(entries);
        }
        Value::Array(values) => values.iter_mut().for_each(sort_json_object_keys),
        _ => {}
    }
}

/// 创建或截断文件，并应用指定 Unix mode。
fn write_file_with_mode(path: &Path, bytes: &[u8], mode: u32) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(mode)
        .open(path)?;
    file.write_all(bytes)
}
