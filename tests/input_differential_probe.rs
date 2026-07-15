//! `internal/input` 的 Linux ARM64 Go/Rust 严格差分 probe。
//!
//! 本文件对应 Go overlay `compat/differential/probes/go/input_probe_test.go`。它通过
//! 生产 V0/V1 parser、固定 fixture provider 和内存 logger 生成第六份 transcript；
//! 原始输入、错误、panic、日志属性顺序以及解析后的运行时语义都参与比较。

use std::{
    any::Any,
    collections::BTreeMap,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
};

use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE},
};
use environment_manager::internal::{
    auth::AuthContext,
    config::StartupContext,
    input::{
        EnvironmentConfigResponse, Outcomes, ParsedContext, SessionContext, SessionContextProvider,
        SessionContextRequest, V0Parser, V1Parser,
    },
    logger::{LogAttributes, LogHandler, LogHandlerError, LogLevel, LogRecord, Logger},
};
use serde::Serialize;
use serde_json::{Value, json};

const V0_FULL_WIRE: &str = r#"{"startup_context":{"api_base_url":"api.example.test/base","sources":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/demo","ref":"main","url":"https://github.example/anthropic/demo.git","host":"github.example","auth":{"type":"anthropic_api","token":"source-token"},"allow_unrestricted_git_push":true}}],"outcomes":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/demo","branches":["task-branch"]}},{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/skipped","branches":["one","two"]}},{"type":"other","git_info":{"type":"github","repo":"anthropic/ignored","branches":["ignored"]}}],"custom_system_prompt":"custom","append_system_prompt":"append","model":"claude-test","mcp_config":{"mcpServers":{"remote":{"type":"http","url":"http://localhost:4321/mcp","headers":{"X-Test":"one"},"tools":[{"name":"echo","enabled":true,"permission_policy":"allow"}]}}},"allowed_tools":["Read","Bash"],"disallowed_tools":["Write"],"enabled_tools":["Read"],"claude_code_args":{"model":"sonnet","verbose":"true"},"mcp_config_file":{"path":"/tmp/mcp.json","content":"eyJ6IjoxfQ==","mode":384},"use_sandbox_gateway_config":true,"entrypoint":"/bin/start","environment_variables":{"A":"B","EMPTY":""},"environment_sub_type":"baku","use_code_sessions":false},"environment":{"environment_type":"anthropic","cwd":"/workspace","allowed_tools":"Read","disallowed_tools":"Bash","unknown":{"nested":true}},"auth":[{"type":"session_ingress","token":"v0-ingress"},{"type":"anthropic_api","token":"api-token"},{"type":"anthropic_oauth","token":"oauth-token"},{"type":"vercel_deploy","token":"vercel-token"}]}"#;
const V0_NULL_WIRE: &str = r#"{"startup_context":{"api_base_url":"null.example.test","sources":null,"outcomes":null,"mcp_config":null,"allowed_tools":null,"disallowed_tools":null,"enabled_tools":null,"claude_code_args":null,"mcp_config_file":null,"environment_variables":null},"environment":{"environment_type":"byoc","cwd":null,"allowed_tools":null,"disallowed_tools":null,"unknown":null},"auth":null}"#;
const V0_MISSING_AUTH_WIRE: &str = r#"{"startup_context":{"api_base_url":"missing-auth.example.test","mcp_config_file":{"path":"/tmp/invalid-mcp.json","content":"%%%","mode":384}},"environment":{"environment_type":"byoc","cwd":"/fallback"}}"#;
const V0_INVALID_ENVIRONMENT_WIRE: &str = r#"{"startup_context":{"api_base_url":"invalid-env.example.test"},"environment":null,"auth":null}"#;
const V0_MISSING_STARTUP_WIRE: &str = "{}";
const V0_NULL_STARTUP_WIRE: &str =
    r#"{"startup_context":null,"environment":{"environment_type":"byoc"},"auth":null}"#;

const V1_CODE_SESSION_SECRET: &str = r#"{"version":1,"session_ingress_token":"secret-ingress","api_base_url":"code.example.test","sources":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/code-session","ref":"feature","host":"github.com"}}],"auth":[{"type":"session_ingress","token":"old-ingress"},{"type":"anthropic_api","token":"code-api"}],"claude_code_args":{"model":"opus"},"mcp_config":{"path":"/tmp/code-mcp.json","content":"e30=","mode":384},"use_sandbox_gateway_config":true,"environment_variables":{"CODE":"1"},"use_code_sessions":true}"#;
const V1_HEALTHCHECK_SECRET: &str = r#"{"version":1,"session_ingress_token":null,"api_base_url":"health.example.test","sources":null,"auth":null,"claude_code_args":null,"mcp_config":null,"use_sandbox_gateway_config":null,"environment_variables":null,"use_code_sessions":null}"#;
const V1_BACKEND_FULL_SECRET: &str = r#"{"version":1,"session_ingress_token":"backend-ingress","api_base_url":"http://127.0.0.1:41791","sources":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/backend","ref":null,"url":null,"host":"github.com","auth":null,"allow_unrestricted_git_push":null}}],"auth":[{"type":"session_ingress","token":"old-backend-ingress"},{"type":"anthropic_oauth","token":"backend-oauth"}],"claude_code_args":null,"mcp_config":null,"use_sandbox_gateway_config":false,"environment_variables":null,"use_code_sessions":false}"#;
const V1_BACKEND_ZERO_SECRET: &str = r#"{"version":1,"session_ingress_token":"backend-ingress","api_base_url":"http://127.0.0.1:41791","sources":null,"auth":null,"claude_code_args":null,"mcp_config":null,"environment_variables":null,"use_code_sessions":false}"#;
const INPUT_BACKEND_URL: &str = "http://127.0.0.1:41791";

/// 一条保持原始插入顺序的 logger 属性。
#[derive(Clone, Debug, Serialize)]
struct ProtocolInputAttribute {
    name: String,
    value: Value,
}

/// 一条 input/auth 阶段日志；原始 `parse_duration_ms` 留给 comparator 审核。
#[derive(Clone, Debug, Serialize)]
struct ProtocolInputLog {
    level: String,
    message: String,
    attributes: Vec<ProtocolInputAttribute>,
}

/// V1 parser 调用 session-context provider 时传递的三个稳定参数。
#[derive(Clone, Debug, Serialize)]
struct ProtocolInputProviderRequest {
    api_base_url: String,
    session_ingress_token: String,
    session_id: String,
}

/// 后续执行层可以观察到的 startup context 语义。
#[derive(Debug, Serialize)]
struct ProtocolInputStartupSummary {
    sources: Value,
    api_base_url: String,
    outcome_fields: Value,
    custom_system_prompt: String,
    append_system_prompt: String,
    model: String,
    mcp_config_present: bool,
    mcp_servers: Value,
    mcp_extra: Value,
    allowed_tools: Value,
    disallowed_tools: Value,
    enabled_tools: Value,
    claude_code_args: Value,
    mcp_config_file: Value,
    use_sandbox_gateway_config: bool,
    entrypoint: String,
    environment_variables: Value,
    environment_sub_type: String,
    use_code_sessions: bool,
}

/// parser 返回的环境类型、完整配置 object 和工具字符串。
#[derive(Debug, Serialize)]
struct ProtocolInputEnvironmentSummary {
    environment_type: String,
    config: Value,
    allowed_tools: String,
    disallowed_tools: String,
}

/// 从 AuthContext getter 读取的固定测试凭据结果。
#[derive(Debug, Serialize)]
struct ProtocolInputAuthSummary {
    session_ingress_token: String,
    anthropic_api_token: String,
    anthropic_oauth_token: String,
    vercel_deploy_token: String,
    antspace_control_plane_url: String,
    antspace_auth_token: String,
    supabase_project_ref: String,
    supabase_anon_key: String,
    supabase_db_pass: String,
    supabase_pat: String,
    session_id: String,
}

/// V0/V1 成功结果的共同语言无关视图。
#[derive(Debug, Serialize)]
struct ProtocolInputParsedSummary {
    startup: Option<ProtocolInputStartupSummary>,
    environment: Option<ProtocolInputEnvironmentSummary>,
    auth: Option<ProtocolInputAuthSummary>,
    outcomes: BTreeMap<String, Vec<String>>,
    session_id: String,
    work_id: String,
    environment_id: String,
    is_healthcheck: bool,
}

/// 一条原始输入及其 result/error/panic、provider 调用与日志。
#[derive(Debug, Serialize)]
struct ProtocolInputCase {
    name: &'static str,
    input_wire_base64: String,
    result: Option<ProtocolInputParsedSummary>,
    error: String,
    panic: String,
    provider_request: Option<ProtocolInputProviderRequest>,
    logs: Vec<ProtocolInputLog>,
}

/// 第六类 input 协议差分文档。
#[derive(Debug, Serialize)]
struct ProtocolInputTranscript {
    schema_version: u32,
    protocol: &'static str,
    cases: Vec<ProtocolInputCase>,
}

/// 内存 logger 的共享状态。
#[derive(Clone, Default)]
struct InputLogState {
    records: Arc<Mutex<Vec<ProtocolInputLog>>>,
}

/// 只捕获 input parser 及同步 auth 配置事件的 handler。
#[derive(Clone, Default)]
struct InputLogCapture {
    state: InputLogState,
    bound_attributes: LogAttributes,
    group: String,
}

impl InputLogCapture {
    /// 创建没有 bound attribute/group 的捕获器。
    fn new() -> Self {
        Self::default()
    }

    /// 返回当前记录的深拷贝，避免序列化期间持有 mutex。
    fn snapshot(&self) -> Vec<ProtocolInputLog> {
        self.state.records.lock().unwrap().clone()
    }
}

impl LogHandler for InputLogCapture {
    /// Probe 打开全部日志级别，确保 DEBUG 阶段也参与差分。
    fn enabled(&self, _level: LogLevel) -> bool {
        true
    }

    /// 保留允许的 input/auth 消息和原始属性顺序，不在 producer 中改写任何值。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError> {
        if !is_input_parser_log(&record.message) {
            return Ok(());
        }
        let mut attributes = Vec::with_capacity(
            self.bound_attributes
                .len()
                .saturating_add(record.attributes.len()),
        );
        for (name, value) in self.bound_attributes.iter().chain(record.attributes.iter()) {
            attributes.push(ProtocolInputAttribute {
                name: grouped_name(&self.group, name),
                value: value.clone(),
            });
        }
        self.state.records.lock().unwrap().push(ProtocolInputLog {
            level: record.level.as_str().to_owned(),
            message: record.message.clone(),
            attributes,
        });
        Ok(())
    }

    /// 派生捕获器时追加 bound attributes，同时保留父捕获器的共享记录状态。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler> {
        let mut clone = self.clone();
        clone.bound_attributes.extend(attributes);
        Arc::new(clone)
    }

    /// 派生捕获器时追加 group path；当前生产 parser 不使用 group，但不能静默丢弃。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler> {
        let mut clone = self.clone();
        clone.group = grouped_name(&clone.group, name);
        Arc::new(clone)
    }
}

/// 生成 group 前缀下的稳定属性名。
fn grouped_name(group: &str, name: &str) -> String {
    if group.is_empty() {
        name.to_owned()
    } else {
        format!("{group}.{name}")
    }
}

/// 只允许 input parser 和其同步 auth 构造事件进入本 transcript。
fn is_input_parser_log(message: &str) -> bool {
    matches!(
        message,
        "Parsing V0 input format"
            | "Parsed startup context from V0 input"
            | "Received pre-computed args from sandbox-gateway"
            | "Pre-computed MCP config file"
            | "Parsed environment from V0 input"
            | "Configured session ingress token"
            | "Configured Anthropic API token"
            | "Configured Anthropic OAuth token"
            | "Configured Vercel deploy token"
            | "Configured Antspace deploy credentials"
            | "Configured Supabase credentials"
            | "Parsed auth from V0 input"
            | "Outcome skipped: exactly one branch must be specified"
            | "Parsed outcomes from V0 input"
            | "Repository outcome"
            | "Completed parsing V0 input"
            | "Parsing V1 input format"
            | "Parsed V1 work response"
            | "Processing healthcheck work type"
            | "Completed parsing V1 healthcheck input"
            | "Decoded work secret"
            | "Parsed outcomes from V1 session context"
            | "Completed parsing V1 input"
    )
}

/// Fixture provider 的共享 context 与调用记录。
#[derive(Clone)]
struct FixtureSessionContextProvider {
    contexts: Arc<BTreeMap<String, SessionContext>>,
    requests: Arc<Mutex<Vec<ProtocolInputProviderRequest>>>,
}

impl FixtureSessionContextProvider {
    /// 构造 full、missing 和 explicit-null 三种 session context。
    fn new() -> Self {
        let full = SessionContext {
            cwd: "/session/workspace".to_owned(),
            outcomes: Some(json!([
                {
                    "type": "git_repository",
                    "git_info": {
                        "type": "github",
                        "repo": "anthropic/backend",
                        "branches": ["backend-branch"]
                    }
                },
                {
                    "type": "git_repository",
                    "git_info": {
                        "type": "github",
                        "repo": "anthropic/backend-skipped",
                        "branches": ["one", "two"]
                    }
                }
            ])),
            custom_system_prompt: "session-custom".to_owned(),
            append_system_prompt: "session-append".to_owned(),
            model: "claude-session".to_owned(),
            mcp_config: Some(json!({
                "mcpServers": {
                    "session": {
                        "type": "http",
                        "url": "http://localhost:7654/mcp",
                        "headers": {"X-Session": "yes"}
                    }
                }
            })),
        };
        let explicit_null = SessionContext {
            outcomes: Some(Value::Null),
            mcp_config: Some(Value::Null),
            ..SessionContext::default()
        };
        Self {
            contexts: Arc::new(BTreeMap::from([
                ("session-full".to_owned(), full),
                ("session-missing".to_owned(), SessionContext::default()),
                ("session-null".to_owned(), explicit_null),
            ])),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 取出指定 session 的首次 provider 调用；未调用时返回 None。
    fn take_request(&self, session_id: &str) -> Option<ProtocolInputProviderRequest> {
        if session_id.is_empty() {
            return None;
        }
        let mut requests = self.requests.lock().unwrap();
        requests
            .iter()
            .position(|request| request.session_id == session_id)
            .map(|index| requests.remove(index))
    }
}

impl SessionContextProvider for FixtureSessionContextProvider {
    /// 记录生产 parser 传来的 provider 参数并返回对应 fixture。
    fn get_session_context<'a>(
        &'a self,
        request: SessionContextRequest<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<SessionContext, String>> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(
                request.api_base_url, INPUT_BACKEND_URL,
                "V1 fixture provider 必须收到固定 backend URL"
            );
            self.requests
                .lock()
                .unwrap()
                .push(ProtocolInputProviderRequest {
                    api_base_url: request.api_base_url.to_owned(),
                    session_ingress_token: request.session_ingress_token.to_owned(),
                    session_id: request.session_id.to_owned(),
                });
            self.contexts
                .get(request.session_id)
                .cloned()
                .ok_or_else(|| format!("missing fixture for session {}", request.session_id))
        })
    }
}

/// 生成 Rust input transcript；只由 Linux ARM64 差分 runner 显式运行。
#[tokio::test]
#[ignore = "由 Linux ARM64 协议差分门禁显式运行"]
async fn input_protocol_differential_transcript() {
    let output_dir = PathBuf::from(
        std::env::var_os("PROTOCOL_TRANSCRIPT_DIR").expect("PROTOCOL_TRANSCRIPT_DIR is required"),
    );
    std::fs::create_dir_all(&output_dir).expect("create Rust transcript directory");

    let provider = FixtureSessionContextProvider::new();
    let mut cases = vec![
        run_v0_input_case("v0_full", V0_FULL_WIRE),
        run_v0_input_case("v0_null_optional", V0_NULL_WIRE),
        run_v0_input_case("v0_missing_auth_invalid_mcp_base64", V0_MISSING_AUTH_WIRE),
        run_v0_input_case("v0_invalid_environment", V0_INVALID_ENVIRONMENT_WIRE),
        run_v0_input_case("v0_missing_startup_context", V0_MISSING_STARTUP_WIRE),
        run_v0_input_case("v0_null_startup_context", V0_NULL_STARTUP_WIRE),
    ];

    let code_wire = make_v1_wire(
        "work-code",
        "env-code",
        "running",
        "session",
        "session-code",
        &encode_work_secret(V1_CODE_SESSION_SECRET),
    );
    cases.push(run_v1_input_case("v1_code_session_full", &code_wire, provider.clone()).await);

    let health_wire = make_v1_wire(
        "work-health",
        "env-health",
        "ready",
        "healthcheck",
        "",
        &encode_work_secret(V1_HEALTHCHECK_SECRET),
    );
    cases.push(
        run_v1_input_case(
            "v1_healthcheck_null_optional",
            &health_wire,
            provider.clone(),
        )
        .await,
    );

    let full_wire = make_v1_wire(
        "work-backend-full",
        "env-backend",
        "running",
        "session",
        "session-full",
        &encode_work_secret(V1_BACKEND_FULL_SECRET),
    );
    cases.push(run_v1_input_case("v1_session_context_full", &full_wire, provider.clone()).await);

    let missing_wire = make_v1_wire(
        "work-backend-missing",
        "env-backend",
        "running",
        "session",
        "session-missing",
        &encode_work_secret(V1_BACKEND_ZERO_SECRET),
    );
    cases.push(
        run_v1_input_case(
            "v1_session_context_missing",
            &missing_wire,
            provider.clone(),
        )
        .await,
    );

    let null_wire = make_v1_wire(
        "work-backend-null",
        "env-backend",
        "running",
        "session",
        "session-null",
        &encode_work_secret(V1_BACKEND_ZERO_SECRET),
    );
    cases.push(run_v1_input_case("v1_session_context_null", &null_wire, provider.clone()).await);

    let invalid_base64_wire = make_v1_wire(
        "work-invalid-secret",
        "env-invalid",
        "running",
        "session",
        "session-invalid",
        "not_base64",
    );
    cases.push(run_v1_input_case("v1_invalid_base64", &invalid_base64_wire, provider).await);

    write_transcript(
        &output_dir.join("input.json"),
        &ProtocolInputTranscript {
            schema_version: 1,
            protocol: "input",
            cases,
        },
    );
}

/// 通过真实 V0 parser 执行输入，并把 error 与 panic 分成两个互斥字段。
fn run_v0_input_case(name: &'static str, wire: &str) -> ProtocolInputCase {
    let capture = InputLogCapture::new();
    let logger = Logger::new(Arc::new(capture.clone()));
    let parser = V0Parser {
        session_id: "session-v0".to_owned(),
    };
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        parser.parse_with_logger(wire.as_bytes(), &logger)
    }));
    let (result, error, panic) = match outcome {
        Ok(Ok(parsed)) => (
            Some(summarize_parsed_context(&parsed)),
            String::new(),
            String::new(),
        ),
        Ok(Err(error)) => (None, error.to_string(), String::new()),
        Err(payload) => (None, String::new(), panic_payload_string(payload.as_ref())),
    };
    ProtocolInputCase {
        name,
        input_wire_base64: STANDARD.encode(wire.as_bytes()),
        result,
        error,
        panic,
        provider_request: None,
        logs: capture.snapshot(),
    }
}

/// 通过真实 V1 parser 与 fixture provider 执行输入。
async fn run_v1_input_case(
    name: &'static str,
    wire: &str,
    provider: FixtureSessionContextProvider,
) -> ProtocolInputCase {
    let capture = InputLogCapture::new();
    let logger = Logger::new(Arc::new(capture.clone()));
    let parser = V1Parser::new("/fallback/workspace", provider.clone());
    let parsed = parser.parse_with_logger(wire.as_bytes(), &logger).await;
    let session_id = parsed
        .as_ref()
        .map_or("", |parsed| parsed.session_id.as_str());
    let provider_request = provider.take_request(session_id);
    let (result, error) = match parsed {
        Ok(parsed) => (Some(summarize_parsed_context(&parsed)), String::new()),
        Err(error) => (None, error.to_string()),
    };
    ProtocolInputCase {
        name,
        input_wire_base64: STANDARD.encode(wire.as_bytes()),
        result,
        error,
        panic: String::new(),
        provider_request,
        logs: capture.snapshot(),
    }
}

/// 把 Rust panic payload 转成 Go recover 使用的字符串表现。
fn panic_payload_string(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

/// 使用 padded URL-safe Base64 编码固定 work secret。
fn encode_work_secret(secret: &str) -> String {
    URL_SAFE.encode(secret.as_bytes())
}

/// 以和 Go probe 完全相同的字段顺序构造 V1 work envelope。
fn make_v1_wire(
    work_id: &str,
    environment_id: &str,
    state: &str,
    data_type: &str,
    session_id: &str,
    secret: &str,
) -> String {
    format!(
        r#"{{"id":"{work_id}","type":"work","environment_id":"{environment_id}","state":"{state}","data":{{"type":"{data_type}","id":"{session_id}"}},"secret":"{secret}","created_at":"2026-01-02T03:04:05Z"}}"#
    )
}

/// 把成功 ParsedContext 转成语言无关语义视图。
fn summarize_parsed_context(parsed: &ParsedContext) -> ProtocolInputParsedSummary {
    ProtocolInputParsedSummary {
        startup: Some(summarize_startup_context(&parsed.startup_context)),
        environment: parsed.environment.as_ref().map(summarize_environment),
        auth: Some(summarize_auth(&parsed.auth_context)),
        outcomes: summarize_outcomes(&parsed.outcomes),
        session_id: parsed.session_id.clone(),
        work_id: parsed.work_id.clone(),
        environment_id: parsed.environment_id.clone(),
        is_healthcheck: parsed.is_healthcheck,
    }
}

/// 展开执行层读取的全部 startup 字段，并显式保留 MCP config presence。
fn summarize_startup_context(startup: &StartupContext) -> ProtocolInputStartupSummary {
    let (mcp_config_present, mcp_servers, mcp_extra) = startup.mcp_config.as_ref().map_or_else(
        || (false, json!({}), json!({})),
        |config| {
            (
                true,
                json_value(&config.mcp_servers),
                json_value(&config.extra),
            )
        },
    );
    ProtocolInputStartupSummary {
        sources: json_value(&startup.sources),
        api_base_url: startup.api_base_url.clone(),
        outcome_fields: json_value(&startup.outcomes),
        custom_system_prompt: startup.custom_system_prompt.clone(),
        append_system_prompt: startup.append_system_prompt.clone(),
        model: startup.model.clone(),
        mcp_config_present,
        mcp_servers,
        mcp_extra,
        allowed_tools: json_value(&startup.allowed_tools),
        disallowed_tools: json_value(&startup.disallowed_tools),
        enabled_tools: json_value(&startup.enabled_tools),
        claude_code_args: json_value(&startup.claude_code_args),
        mcp_config_file: json_value(&startup.mcp_config_file),
        use_sandbox_gateway_config: startup.use_sandbox_gateway_config,
        entrypoint: startup.entrypoint.clone(),
        environment_variables: json_value(&startup.environment_variables),
        environment_sub_type: startup.environment_sub_type.clone(),
        use_code_sessions: startup.use_code_sessions,
    }
}

/// 保留环境完整 JSON object 及工具字段。
fn summarize_environment(
    environment: &EnvironmentConfigResponse,
) -> ProtocolInputEnvironmentSummary {
    ProtocolInputEnvironmentSummary {
        environment_type: environment.environment_type.clone(),
        config: environment.config.clone(),
        allowed_tools: environment.allowed_tools.clone(),
        disallowed_tools: environment.disallowed_tools.clone(),
    }
}

/// 通过 getter 读取所有 AuthContext 字段；使用的都是固定测试假 token。
fn summarize_auth(auth: &AuthContext) -> ProtocolInputAuthSummary {
    ProtocolInputAuthSummary {
        session_ingress_token: auth.get_session_ingress_token().to_owned(),
        anthropic_api_token: auth.get_anthropic_api_token().to_owned(),
        anthropic_oauth_token: auth.get_anthropic_oauth_token().to_owned(),
        vercel_deploy_token: auth.get_vercel_deploy_token().to_owned(),
        antspace_control_plane_url: auth.get_antspace_control_plane_url().to_owned(),
        antspace_auth_token: auth.get_antspace_auth_token().to_owned(),
        supabase_project_ref: auth.get_supabase_project_ref().to_owned(),
        supabase_anon_key: auth.get_supabase_anon_key().to_owned(),
        supabase_db_pass: auth.get_supabase_db_pass().to_owned(),
        supabase_pat: auth.get_supabase_pat().to_owned(),
        session_id: auth.get_session_id().to_owned(),
    }
}

/// 把 outcome map 复制到 BTreeMap，保证 transcript 文本也便于人工复核。
fn summarize_outcomes(outcomes: &Outcomes) -> BTreeMap<String, Vec<String>> {
    outcomes
        .as_map()
        .iter()
        .map(|(repository, branches)| (repository.clone(), branches.clone()))
        .collect()
}

/// 将任意可序列化 DTO 转为 JSON 值；失败代表 probe 与生产 DTO 契约冲突。
fn json_value(value: &impl Serialize) -> Value {
    serde_json::to_value(value).expect("serialize input summary value")
}

/// 把 transcript 写为紧凑 UTF-8 JSON 和单个结尾换行。
fn write_transcript(path: &Path, transcript: &ProtocolInputTranscript) {
    let mut encoded = serde_json::to_vec(transcript).expect("serialize Rust input transcript");
    encoded.push(b'\n');
    std::fs::write(path, encoded).expect("write Rust input transcript");
}
