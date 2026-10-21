//! Tool adapter for the official rmcp SDK. Only MCP 2026-07-28 is accepted.
use crate::{ToolResult, plugins::Manager, tools::Registry};
use axum::Router;
use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock as Content,
        InitializeRequestParams, ListToolsResult, PaginatedRequestParams,
        ProgressNotificationParam, ProtocolVersion, ServerCapabilities, ServerConfig, Tool,
    },
    service::RequestContext,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde_json::{Value, json};
use std::{borrow::Cow, io, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

pub const PROTOCOL_VERSION: &str = "2026-07-28";
pub const VERSION_META: &str = "io.modelcontextprotocol/protocolVersion";
pub const CAPABILITIES_META: &str = "io.modelcontextprotocol/clientCapabilities";

#[derive(Clone, Debug)]
pub struct Config {
    pub name: String,
    pub version: String,
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            name: "sandbox-runtime-tools".into(),
            version: "0.1.0".into(),
            allowed_hosts: vec!["localhost".into(), "127.0.0.1".into(), "[::1]".into()],
            allowed_origins: Vec::new(),
        }
    }
}
pub struct Server {
    pub registry: Arc<Registry>,
    pub plugins: Arc<Manager>,
    pub config: Config,
}
impl Server {
    pub fn new(registry: Arc<Registry>, plugins: Arc<Manager>, mut config: Config) -> Self {
        let default = Config::default();
        if config.name.is_empty() {
            config.name = default.name;
        }
        if config.version.is_empty() {
            config.version = default.version;
        }
        Self {
            registry,
            plugins,
            config,
        }
    }
    /// SDK-owned stdio framing, metadata validation, discovery, and cancellation.
    pub async fn serve_stdio(self: Arc<Self>) -> io::Result<()> {
        self.serve_stream(tokio::io::stdin(), tokio::io::stdout())
            .await
    }
    pub async fn serve_stream(
        self: Arc<Self>,
        input: impl AsyncRead + Send + Unpin + 'static,
        output: impl AsyncWrite + Send + Unpin + 'static,
    ) -> io::Result<()> {
        self.serve((input, output))
            .await
            .map_err(io::Error::other)?
            .waiting()
            .await
            .map_err(io::Error::other)?;
        Ok(())
    }
    pub fn router(self: Arc<Self>) -> Router {
        self.router_with_cancellation(CancellationToken::new())
    }
    /// Share shutdown with the embedding application's lifecycle and active requests.
    pub fn router_with_cancellation(self: Arc<Self>, cancellation: CancellationToken) -> Router {
        let mut config = StreamableHttpServerConfig::default()
            .with_cancellation_token(cancellation)
            .with_legacy_session_mode(false)
            .with_json_response(false)
            .with_allowed_origins(self.config.allowed_origins.clone())
            .enforce_origin_validation();
        config.allowed_hosts = self.config.allowed_hosts.clone();
        config.stateless_protocol_metadata_required = true;
        let service = StreamableHttpService::new(
            move || Ok(self.clone()),
            Arc::new(LocalSessionManager::default()),
            config,
        );
        Router::new().nest_service("/mcp", service)
    }
    pub async fn serve_http(self: Arc<Self>, address: &str) -> io::Result<()> {
        let address = if address.starts_with(':') {
            format!("[::]{address}")
        } else {
            address.to_owned()
        };
        axum::serve(tokio::net::TcpListener::bind(address).await?, self.router()).await
    }
    async fn run_tool(
        &self,
        request: CallToolRequestParams,
        cancellation: CancellationToken,
    ) -> Result<CallToolResponse, ErrorData> {
        let tool = self
            .registry
            .get(&request.name)
            .or_else(|| self.plugins.get_tool(&request.name))
            .ok_or_else(|| {
                ErrorData::invalid_params(format!("Tool not found: {}", request.name), None)
            })?;
        let arguments = Value::Object(request.arguments.unwrap_or_default());
        let definition = tool.definition();
        let validation = validate_schema(&definition["inputSchema"], &arguments)
            .and_then(|()| tool.validate(&arguments));
        let result = match validation {
            Err(message) => tool.error_result(format!("Validation failed: {message}")),
            Ok(()) => tool
                .handle(cancellation, arguments)
                .await
                .unwrap_or_else(|message| {
                    tool.error_result(format!("Execution failed: {message}"))
                }),
        };
        if let Some(schema) = definition.get("outputSchema") {
            let execution = serde_json::to_value(&result.structured_content)
                .map_err(|e| ErrorData::internal_error(e.to_string(), None))?;
            validate_schema(schema, &execution).map_err(|message| {
                ErrorData::internal_error(
                    format!("Tool output violates outputSchema: {message}"),
                    None,
                )
            })?;
        }
        Ok(sdk_result(result)?.into())
    }
}
impl ServerHandler for Server {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Owned(vec![ProtocolVersion::V_2026_07_28])
    }
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::new(ServerCapabilities::builder().enable_tools().build());
        info.server_info.name = self.config.name.clone();
        info.server_info.version = self.config.version.clone();
        info
    }
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<ServerConfig, ErrorData> {
        Err(ErrorData::unsupported_protocol_version(
            request.protocol_version,
            &[ProtocolVersion::V_2026_07_28],
        ))
    }
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        if request
            .and_then(|p| p.cursor)
            .is_some_and(|cursor| !cursor.is_empty())
        {
            return Err(ErrorData::invalid_params("Invalid cursor", None));
        }
        let tools = self
            .registry
            .list()
            .iter()
            .chain(self.plugins.list_tools().iter())
            .map(|tool| {
                serde_json::from_value(tool.definition())
                    .map_err(|e| ErrorData::internal_error(e.to_string(), None))
            })
            .collect::<Result<Vec<Tool>, _>>()?;
        Ok(ListToolsResult::with_all_items(tools))
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.registry
            .get(name)
            .or_else(|| self.plugins.get_tool(name))
            .and_then(|t| serde_json::from_value(t.definition()).ok())
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if let Some(token) = context.meta.get_progress_token() {
            let _ = context
                .peer
                .notify_progress(ProgressNotificationParam::new(token, 0.0))
                .await;
        }
        self.run_tool(request, context.ct).await
    }
}
fn sdk_result(result: ToolResult) -> Result<CallToolResult, ErrorData> {
    let content = result
        .content
        .into_iter()
        .map(|block| match block.kind.as_str() {
            "text" => Ok(Content::text(block.text)),
            "image" => Ok(Content::image(block.data, block.mime_type)),
            _ => Err(ErrorData::internal_error(
                "Unsupported tool content type",
                None,
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut converted = CallToolResult::success(content);
    converted.is_error = Some(result.is_error);
    converted.structured_content = result
        .structured_content
        .map(|execution| serde_json::to_value(execution).expect("execution result"));
    Ok(converted)
}
fn validate_schema(schema: &Value, instance: &Value) -> Result<(), String> {
    if let Some(dialect) = schema.get("$schema").and_then(Value::as_str)
        && dialect != "https://json-schema.org/draft/2020-12/schema"
    {
        return Err(format!("Unsupported JSON Schema dialect: {dialect}"));
    }
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(schema)
        .map_err(|e| e.to_string())?;
    validator.validate(instance).map_err(|e| e.to_string())
}
/// Construct per-request metadata for embedders and wire tests.
pub fn request_meta() -> Value {
    json!({VERSION_META:PROTOCOL_VERSION,CAPABILITIES_META:{}})
}
