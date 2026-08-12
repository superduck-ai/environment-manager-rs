//! Deploy action 的数据模型与客户端状态。
//!
//! 字段对应 Go `internal/tunnel/actions/deploy/zz_recovered_types.go`。HTTP wire 类型显式
//! 标注 serde 字段名；其中 `aliases` 使用 `Option<Vec<_>>`，用于保留 Go 中未出现的
//! `[]string` 会在最终 JSON 中编码为 `null`、而显式空数组仍编码为 `[]` 的差异。

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::internal::logger::Logger;
use crate::internal::tunnel::actions::ProgressReporter;

/// Antspace control plane 客户端。
///
/// `reqwest::Client` 自带连接池并可廉价克隆；固定十五分钟超时对应 Go 构造函数中的
/// `http.Client{Timeout: 15 * time.Minute}`。
#[derive(Clone)]
pub struct AntspaceClient {
    pub(crate) control_plane_url: String,
    pub(crate) auth_token: String,
    pub(crate) http_client: reqwest::Client,
    pub(crate) logger: Option<Logger>,
}

/// Antspace 成功响应中由调用方使用的两个字段。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntspaceDeployResult {
    /// 部署后的公开 URL。
    pub url: String,
    /// control plane 返回的 service 名称。
    pub service: String,
}

/// `deploy` action 的配置和可选 Antspace 客户端。
pub struct DeployAction {
    pub(crate) logger: Option<Logger>,
    pub(crate) vercel_token: String,
    pub(crate) vercel_team_id: String,
    pub(crate) antspace_client: Option<AntspaceClient>,
    pub(crate) default_target: String,
}

/// Vercel deployment API 的部署对象。
///
/// 所有字符串和时间字段都有默认值，复刻 Go `encoding/json` 对缺失 struct 字段使用
/// 零值的行为。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Deployment {
    /// Vercel deployment ID。
    pub id: String,
    /// 不带协议的 Vercel deployment URL。
    pub url: String,
    /// deployment 名称。
    pub name: String,
    /// `BUILDING`、`READY`、`ERROR` 或 `CANCELED`。
    #[serde(rename = "readyState")]
    pub ready_state: String,
    /// Unix 毫秒时间戳。
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    /// 开始构建的 Unix 毫秒时间戳。
    #[serde(rename = "buildingAt")]
    pub building_at: i64,
    /// 构建完成的 Unix 毫秒时间戳。
    pub ready: i64,
    /// Vercel target，当前请求固定为 `production`。
    pub target: String,
    /// Vercel project ID。
    #[serde(rename = "projectId")]
    pub project_id: String,
    /// Vercel inspector URL。
    #[serde(rename = "inspectorUrl")]
    pub inspector_url: String,
    /// 部署别名；`None` 保留 Go nil slice 的 JSON `null` 语义。
    pub aliases: Option<Vec<String>>,
}

/// Vercel deployment 请求中的一个文件索引项。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    /// 相对 `dist` 根目录的路径。
    pub file: String,
    /// 文件内容的十六进制 SHA-1。
    pub sha: String,
    /// 文件字节数。
    pub size: usize,
}

/// Vercel REST 客户端。
///
/// 字段保持 crate 可见，便于同一迁移工程中的行为测试注入本地 HTTP server；生产入口
/// 仍通过 `VercelClient::new` 建立一分钟超时的客户端。
#[derive(Clone)]
pub struct VercelClient {
    pub(crate) token: String,
    pub(crate) team_id: String,
    pub(crate) http_client: reqwest::Client,
    pub(crate) base_url: String,
}

/// `deploy` action 的可选 JSON 请求。
#[derive(Debug, Default, Deserialize)]
pub(crate) struct DeployRequest {
    #[serde(default)]
    pub(crate) project_dir: String,
    #[serde(default, rename = "target")]
    pub(crate) deploy_target: String,
}

/// Vercel 路径成功时返回给 tunnel 调用方的 JSON。
///
/// 字段声明顺序严格对应 Go struct，从而让 `serde_json::to_vec` 产生相同的稳定键顺序。
#[derive(Debug, Serialize)]
pub(crate) struct DeployResponse {
    pub(crate) url: String,
    pub(crate) deploy_url: String,
    pub(crate) deploy_id: String,
    pub(crate) project_name: String,
    pub(crate) project_id: String,
    pub(crate) commit_sha: String,
    pub(crate) ready_state: String,
    pub(crate) target: String,
    pub(crate) inspector_url: String,
    pub(crate) aliases: Option<Vec<String>>,
    pub(crate) created_at: i64,
    pub(crate) files_uploaded: i64,
    pub(crate) files_cached: i64,
}

/// Vercel `missing_files` 错误外壳。
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct VercelErrorResponse {
    pub(crate) error: VercelError,
}

/// Vercel 错误详情；缺失字段按 Go 零值处理。
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct VercelError {
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) missing: Vec<String>,
}

/// Antspace HTTP 响应的完整 JSON 形状。
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct AntspaceResponse {
    pub(crate) status: String,
    pub(crate) url: String,
    pub(crate) service: String,
    pub(crate) error: String,
}

/// Antspace 成功响应发出的可选进度 reporter。
///
/// Go API 接受 nil interface，因此客户端层保留 `Option`；action trait 自身始终传入
/// `Some`。
pub(crate) type OptionalProgress = Option<Arc<dyn ProgressReporter>>;
