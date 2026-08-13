//! Supabase client、server state 与 wire 数据类型。
//!
//! 类型保持 Management API/CCR API 的原字段名。客户端分开保存 30 秒管理 API
//! client 和无全局 timeout 的 deploy client，使 Edge Function 的 120 秒请求窗口与
//! 一致。

use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::internal::mcp::BaseServer;

/// Supabase Management API client。
#[derive(Clone, Debug)]
pub struct Client {
    /// migration/query/type 请求使用的 client。
    pub(crate) http_client: reqwest::Client,
    /// 允许 120 秒部署窗口的独立 client。
    pub(crate) deploy_http_client: reqwest::Client,
    /// Personal access token。
    pub(crate) pat: String,
    /// Supabase project reference。
    pub(crate) project_ref: String,
    /// Management API 根 URL。
    pub(crate) base_url: String,
}

impl Client {
    /// 以标准 30 秒管理 API timeout 创建 client。
    pub fn new(
        pat: impl Into<String>,
        project_ref: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            http_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
            deploy_http_client: reqwest::Client::new(),
            pat: pat.into(),
            project_ref: project_ref.into(),
            base_url: base_url.into(),
        })
    }

    /// 注入定向测试 client，同时保留生产字段形状。
    #[must_use]
    pub fn with_http_clients(
        mut self,
        management: reqwest::Client,
        deploy: reqwest::Client,
    ) -> Self {
        self.http_client = management;
        self.deploy_http_client = deploy;
        self
    }

    /// 返回 project reference。
    #[must_use]
    pub fn project_ref(&self) -> &str {
        &self.project_ref
    }
}

/// Edge Function deploy API 成功结果。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeployFunctionResult {
    /// Function ID。
    #[serde(default)]
    pub id: String,
    /// URL slug。
    #[serde(default)]
    pub slug: String,
    /// Function display name。
    #[serde(default)]
    pub name: String,
    /// ACTIVE 等部署状态。
    #[serde(default)]
    pub status: String,
    /// 部署版本号。
    #[serde(default)]
    pub version: i64,
}

/// 已应用 migration 的 API 记录。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MigrationResponse {
    /// Supabase migration version/timestamp。
    #[serde(default)]
    pub version: String,
    /// migration 名称。
    #[serde(default)]
    pub name: String,
}

/// Management API 的结构化错误正文。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct QueryErrorResponse {
    /// 人类可读 API 错误。
    #[serde(default)]
    pub message: String,
}

/// 可变 Supabase server state。
#[derive(Debug, Default)]
pub(crate) struct SupabaseState {
    /// 已知 project ref 后创建的 Management API client。
    pub client: Option<Arc<Client>>,
    /// 永久 provisioning 失败正文；非空后不再请求 credentials。
    pub provisioning_error: String,
}

/// Supabase MCP 业务 server。
#[derive(Debug)]
pub struct SupabaseMCPServer {
    /// loopback MCP server。
    pub base_server: Arc<BaseServer>,
    /// client/provisioning error 的异步读写状态。
    pub(crate) state: RwLock<SupabaseState>,
    /// Supabase PAT。
    pub(crate) pat: String,
    /// workspace 或其 `project` 子目录的基准。
    pub(crate) base_working_dir: PathBuf,
    /// CCR session ID。
    pub(crate) session_id: String,
    /// 环境 API 根 URL。
    pub(crate) api_base_url: String,
    /// CCR ingress Bearer token。
    pub(crate) session_ingress_token: String,
    /// baku 按需 provisioning 模式。
    pub(crate) lazy_mode: bool,
    /// credentials/provision control plane client。
    pub(crate) credentials_http_client: reqwest::Client,
}

impl SupabaseMCPServer {
    /// 创建 Supabase server；调用方可传入已有 project client。
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_server: Arc<BaseServer>,
        client: Option<Arc<Client>>,
        pat: impl Into<String>,
        base_working_dir: PathBuf,
        session_id: impl Into<String>,
        api_base_url: impl Into<String>,
        session_ingress_token: impl Into<String>,
        lazy_mode: bool,
    ) -> Self {
        Self {
            base_server,
            state: RwLock::new(SupabaseState {
                client,
                provisioning_error: String::new(),
            }),
            pat: pat.into(),
            base_working_dir,
            session_id: session_id.into(),
            api_base_url: api_base_url.into(),
            session_ingress_token: session_ingress_token.into(),
            lazy_mode,
            credentials_http_client: reqwest::Client::new(),
        }
    }

    /// 注入 control-plane HTTP client，供本地协议测试使用。
    #[must_use]
    pub fn with_credentials_http_client(mut self, client: reqwest::Client) -> Self {
        self.credentials_http_client = client;
        self
    }

    /// 返回 lazy provisioning 标志。
    #[must_use]
    pub fn lazy_mode(&self) -> bool {
        self.lazy_mode
    }
}

/// TypeScript type generation API 响应。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct TypesResponse {
    /// 生成的 TypeScript 源码。
    #[serde(default)]
    pub types: String,
}

/// Function deploy multipart 的 metadata 字段。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FunctionDeployMetadata {
    /// 相对入口文件，固定为 `index.ts`。
    pub entrypoint_path: String,
    /// Function slug/name。
    pub name: String,
    /// 是否要求 JWT，当前固定 true。
    pub verify_jwt: bool,
}

/// 收集后待加入 multipart 的一个普通文件。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionFile {
    /// 信任根内的绝对路径。
    pub abs_path: PathBuf,
    /// multipart filename，路径分隔符固定 `/`。
    pub rel_path: String,
}

/// provisioning trigger 的三态结果。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProvisionStatus {
    /// 202，异步创建已开始。
    Started,
    /// 200，数据库已经存在。
    Already,
    /// 500，永久失败。
    Failed,
}

/// CCR credentials API 成功响应。
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SupabaseCredentials {
    /// Supabase project reference。
    #[serde(default)]
    pub project_ref: String,
    /// Browser anon key。
    #[serde(default)]
    pub anon_key: String,
    /// Postgres password。
    #[serde(default)]
    pub db_pass: String,
}
