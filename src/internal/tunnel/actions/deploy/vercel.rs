//! Vercel 文件索引、上传与部署轮询。
//!
//! 该实现保留 Go 的 100 MiB 上限、SHA-1 content-addressed 上传、只接受 HTTP 200、
//! 429/5xx 轮询重试，以及未经额外转义直接拼接 `teamId` 的请求形状。

use std::collections::HashMap;
use std::path::{Component, Path};
use std::time::{Duration, Instant};

use reqwest::{Method, StatusCode};
use serde::Serialize;
use sha1::{Digest as _, Sha1};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::internal::tunnel::actions::ActionError;

use super::zz_recovered_types::{Deployment, FileEntry, VercelClient, VercelErrorResponse};

/// 对应 Go `maxDeploymentBytes`：单次部署所有普通文件合计最多 100 MiB。
pub const MAX_DEPLOYMENT_BYTES: usize = 100 * 1024 * 1024;
/// `collect_files` 的文件索引与按 SHA-1 去重内容映射返回值。
///
/// 使用命名别名仅为降低公开签名复杂度；wire 数据和 Go 的“双返回值”语义不变。
pub type CollectedFiles = (Vec<FileEntry>, HashMap<String, Vec<u8>>);
/// Vercel HTTP 客户端的整次请求超时。
const VERCEL_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// 非终态 deployment 和可重试 HTTP 状态之间的固定等待间隔。
const DEPLOYMENT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// deployment API 请求体。
///
/// 字段按 Go `encoding/json` 对 map key 的字典序排列，既保持 wire 值，也让原始 JSON
/// 字节在诊断和录制测试中稳定。
#[derive(Serialize)]
struct DeploymentRequest<'a> {
    files: &'a [FileEntry],
    name: &'a str,
    project: &'a str,
    #[serde(rename = "projectSettings")]
    project_settings: ProjectSettings,
    target: &'static str,
}

/// Vercel 项目构建设置；部署内容已经在本地 `dist` 中，因此禁用远端构建命令。
#[derive(Serialize)]
struct ProjectSettings {
    #[serde(rename = "buildCommand")]
    build_command: &'static str,
    framework: Option<&'static str>,
    #[serde(rename = "installCommand")]
    install_command: &'static str,
    #[serde(rename = "outputDirectory")]
    output_directory: &'static str,
}

impl VercelClient {
    /// 创建生产 Vercel 客户端。
    ///
    /// 固定 API base URL 与一分钟请求超时分别对应 Go action 中的字面量和
    /// `http.Client{Timeout: time.Minute}`。
    #[must_use]
    pub fn new(token: impl Into<String>, team_id: impl Into<String>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(VERCEL_REQUEST_TIMEOUT)
            .build()
            .expect("building a reqwest client with a fixed timeout cannot fail");
        Self {
            token: token.into(),
            team_id: team_id.into(),
            http_client,
            base_url: "https://api.vercel.com".to_owned(),
        }
    }

    /// 对应 Go：`(*VercelClient).CreateDeployment`。
    ///
    /// 成功返回 deployment；Vercel 的 `missing_files` 错误即使使用非 200 状态也不算
    /// action 失败，而是返回待上传 SHA 列表。其他非 200 状态保留完整响应体。
    pub async fn create_deployment(
        &self,
        cancellation: &CancellationToken,
        project_name: &str,
        files: &[FileEntry],
    ) -> Result<(Option<Deployment>, Vec<String>), ActionError> {
        let payload = DeploymentRequest {
            files,
            name: project_name,
            project: project_name,
            project_settings: ProjectSettings {
                build_command: "",
                framework: None,
                install_command: "",
                output_directory: ".",
            },
            target: "production",
        };
        let mut url = format!("{}/v13/deployments", self.base_url);
        append_team_id(&mut url, &self.team_id);
        let request = self
            .http_client
            .request(Method::POST, url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .json(&payload)
            .build()
            .map_err(|error| format!("failed to create request: {error}"))?;
        let response = execute_request(&self.http_client, request, cancellation)
            .await
            .map_err(|error| format!("deployment request failed: {error}"))?;
        let status = response.status();
        let response_body = read_response(response, cancellation)
            .await
            .map_err(|error| format!("failed to read response: {error}"))?;

        if status != StatusCode::OK {
            if let Ok(vercel_error) = serde_json::from_slice::<VercelErrorResponse>(&response_body)
                && vercel_error.error.code == "missing_files"
            {
                return Ok((None, vercel_error.error.missing));
            }
            return Err(format!(
                "deployment failed (status {}): {}",
                status.as_u16(),
                String::from_utf8_lossy(&response_body)
            )
            .into());
        }

        let deployment = serde_json::from_slice(&response_body)
            .map_err(|error| format!("failed to parse deployment response: {error}"))?;
        Ok((Some(deployment), Vec::new()))
    }

    /// 对应 Go：`(*VercelClient).UploadFile`。
    ///
    /// 原始文件字节通过 `/v2/files` 上传，SHA 放在 `x-vercel-digest`；只有精确的 200
    /// 被接受。成功响应体被消费但消费错误与 Go `io.Copy(io.Discard)` 一样忽略。
    pub async fn upload_file(
        &self,
        cancellation: &CancellationToken,
        sha: &str,
        data: &[u8],
    ) -> Result<(), ActionError> {
        let mut url = format!("{}/v2/files", self.base_url);
        append_team_id(&mut url, &self.team_id);
        let request = self
            .http_client
            .request(Method::POST, url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/octet-stream")
            .header("x-vercel-digest", sha)
            .body(data.to_vec())
            .build()
            .map_err(|error| format!("failed to create upload request: {error}"))?;
        let response = execute_request(&self.http_client, request, cancellation)
            .await
            .map_err(|error| format!("file upload failed: {error}"))?;
        let status = response.status();
        if status != StatusCode::OK {
            let body = read_response(response, cancellation)
                .await
                .unwrap_or_default();
            return Err(format!(
                "file upload failed (status {}): {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            )
            .into());
        }

        let _ignored = read_response(response, cancellation).await;
        Ok(())
    }

    /// 对应 Go：`(*VercelClient).WaitForReady`。
    ///
    /// 每次循环先检查 deadline 和取消，再查询 deployment。429 与所有 5xx 等待两秒
    /// 后重试；`READY` 返回，`ERROR`/`CANCELED` 立即失败，其他状态继续轮询。两秒等待
    /// 不会被剩余 deadline 截短，保持 Go 可能略微越过 deadline 的行为。
    pub async fn wait_for_ready(
        &self,
        cancellation: &CancellationToken,
        deployment_id: &str,
        timeout: Duration,
    ) -> Result<Deployment, ActionError> {
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        while Instant::now() < deadline {
            if cancellation.is_cancelled() {
                return Err(
                    "context cancelled while waiting for deployment: context canceled".into(),
                );
            }

            let mut url = format!("{}/v13/deployments/{deployment_id}", self.base_url);
            append_team_id(&mut url, &self.team_id);
            let request = self
                .http_client
                .request(Method::GET, url)
                .header("Authorization", format!("Bearer {}", self.token))
                .build()
                .map_err(|error| format!("failed to create status request: {error}"))?;
            let response = execute_request(&self.http_client, request, cancellation)
                .await
                .map_err(|error| format!("status check failed: {error}"))?;
            let status = response.status();
            let body = read_response(response, cancellation)
                .await
                .map_err(|error| format!("failed to read status response: {error}"))?;

            if status != StatusCode::OK {
                if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                    wait_for_deployment_poll(cancellation).await?;
                    continue;
                }
                return Err(format!(
                    "deployment status check failed (status {}): {}",
                    status.as_u16(),
                    String::from_utf8_lossy(&body)
                )
                .into());
            }

            let deployment: Deployment = serde_json::from_slice(&body)
                .map_err(|error| format!("failed to parse status response: {error}"))?;
            match deployment.ready_state.as_str() {
                "READY" => return Ok(deployment),
                "ERROR" | "CANCELED" => {
                    return Err(format!(
                        "deployment failed with state: {}",
                        deployment.ready_state
                    )
                    .into());
                }
                _ => wait_for_deployment_poll(cancellation).await?,
            }
        }

        Err(format!("deployment not ready after {}", format_go_duration(timeout)).into())
    }
}

/// 对应 Go：`CollectFiles`。
///
/// 以目录项名称排序做深度优先遍历；目录与 symlink 不进入文件列表。每个普通/特殊
/// 非目录项完整读取后才累计上限，复刻 Go 在发现超限前已经读取当前文件的行为。
/// 相同内容共享 `file_data` 中的一份 SHA 映射，但文件索引仍逐项保留。
pub fn collect_files(directory: impl AsRef<Path>) -> Result<CollectedFiles, ActionError> {
    let directory = directory.as_ref();
    let mut files = Vec::new();
    let mut file_data = HashMap::new();
    let mut total_bytes = 0usize;

    let walker = WalkDir::new(directory).sort_by_file_name();
    for entry in walker {
        let entry = entry.map_err(|error| {
            format!(
                "collect files from {}: walk error: {error}",
                directory.display()
            )
        })?;
        if entry.file_type().is_dir() || entry.file_type().is_symlink() {
            continue;
        }

        let path = entry.path();
        let data = std::fs::read(path).map_err(|error| {
            format!(
                "collect files from {}: read file {}: {error}",
                directory.display(),
                path.display()
            )
        })?;
        total_bytes = total_bytes.saturating_add(data.len());
        if total_bytes > MAX_DEPLOYMENT_BYTES {
            return Err(format!(
                "collect files from {}: deployment exceeds max size",
                directory.display()
            )
            .into());
        }

        let relative = path.strip_prefix(directory).map_err(|error| {
            format!(
                "collect files from {}: relative path {}: {error}",
                directory.display(),
                path.display()
            )
        })?;
        let relative = if relative.as_os_str().is_empty() {
            Path::new(".")
        } else {
            relative
        };
        let digest = Sha1::digest(&data);
        let sha = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        files.push(FileEntry {
            file: relative.to_string_lossy().into_owned(),
            sha: sha.clone(),
            size: data.len(),
        });
        file_data.insert(sha, data);
    }

    Ok((files, file_data))
}

/// 对应 Go：`ValidateProjectDir`。
///
/// 只做词法 clean 和前缀判断，不解析 symlink；这既允许
/// `/home/claude/project/../project`，也保留原实现由后续文件操作决定 symlink 目标的
/// 行为。
pub fn validate_project_dir(directory: &str) -> Result<(), ActionError> {
    let clean = clean_unix_path(directory);
    if !clean.starts_with("/home/claude/") {
        return Err(format!("project_dir must be under /home/claude/, got: {clean}").into());
    }
    Ok(())
}

/// 对应 Go：`ProjectName`。
///
/// 最多保留去前缀后的前 20 个 UTF-8 **字节**，随后做 Unicode simple lowercase；
/// ASCII 小写字母、数字和连字符之外的每个 rune 都变成一个连字符。无效或被截断的
/// UTF-8 字节按 Go range 规则逐字节变成连字符。
#[must_use]
pub fn project_name(session_id: &str) -> String {
    let stripped = session_id
        .strip_prefix("session_staging_")
        .or_else(|| session_id.strip_prefix("session_"))
        .unwrap_or(session_id);
    let bytes = &stripped.as_bytes()[..stripped.len().min(20)];
    let mut normalized = String::with_capacity(bytes.len());
    let mut offset = 0usize;
    while offset < bytes.len() {
        match std::str::from_utf8(&bytes[offset..]) {
            Ok(valid) => {
                for character in valid.chars() {
                    push_project_character(&mut normalized, character);
                }
                break;
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                if valid_up_to > 0 {
                    // SAFETY-free decode: `valid_up_to` is guaranteed to end on a UTF-8 boundary.
                    let valid = std::str::from_utf8(&bytes[offset..offset + valid_up_to])
                        .expect("Utf8Error::valid_up_to identifies valid UTF-8");
                    for character in valid.chars() {
                        push_project_character(&mut normalized, character);
                    }
                    offset += valid_up_to;
                }
                let invalid_bytes = error.error_len().unwrap_or(bytes.len() - offset);
                for _ in 0..invalid_bytes {
                    normalized.push('-');
                }
                offset += invalid_bytes;
            }
        }
    }
    format!("baku-{normalized}")
}

/// 把一个 Unicode rune 依照 Go `strings.ToLower` 后的 allow-list 写入项目名。
fn push_project_character(output: &mut String, character: char) {
    let lower = character.to_lowercase().next().unwrap_or(character);
    if lower.is_ascii_lowercase() || lower.is_ascii_digit() || lower == '-' {
        output.push(lower);
    } else {
        output.push('-');
    }
}

/// 对单次 HTTP 请求施加 action 取消令牌。
///
/// 丢弃 reqwest future 会关闭本次请求；错误文本使用 Go context 的稳定
/// `context canceled`，由各 API 方法再加各自的外层操作说明。
async fn execute_request(
    client: &reqwest::Client,
    request: reqwest::Request,
    cancellation: &CancellationToken,
) -> Result<reqwest::Response, String> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err("context canceled".to_owned()),
        result = client.execute(request) => result.map_err(|error| error.to_string()),
    }
}

/// 读取响应体，并让读取阶段继续遵守 action 取消令牌。
async fn read_response(
    response: reqwest::Response,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, String> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err("context canceled".to_owned()),
        result = response.bytes() => result
            .map(|bytes| bytes.to_vec())
            .map_err(|error| error.to_string()),
    }
}

/// 固定等待一次 deployment poll；取消时返回 Go 的原始错误包装文本。
async fn wait_for_deployment_poll(cancellation: &CancellationToken) -> Result<(), ActionError> {
    tokio::select! {
        () = tokio::time::sleep(DEPLOYMENT_POLL_INTERVAL) => Ok(()),
        () = cancellation.cancelled() => {
            Err("context cancelled during poll: context canceled".into())
        }
    }
}

/// 在 URL 后按 Go 字符串拼接方式附加 team ID，不额外 percent-encode。
fn append_team_id(url: &mut String, team_id: &str) {
    if !team_id.is_empty() {
        url.push_str("?teamId=");
        url.push_str(team_id);
    }
}

/// 实现 Unix `filepath.Clean` 的词法部分，供项目目录边界检查使用。
fn clean_unix_path(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut components: Vec<&str> = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                if components.last().is_some_and(|value| *value != "..") {
                    components.pop();
                } else if !absolute {
                    components.push("..");
                }
            }
            Component::Normal(value) => components.push(value.to_str().unwrap_or("\u{fffd}")),
            Component::Prefix(_) => unreachable!("Unix paths never expose a Windows prefix"),
        }
    }
    if absolute {
        if components.is_empty() {
            "/".to_owned()
        } else {
            format!("/{}", components.join("/"))
        }
    } else if components.is_empty() {
        ".".to_owned()
    } else {
        components.join("/")
    }
}

/// 把 Rust `Duration` 转成 Go `time.Duration.String` 的常用正数格式。
///
/// action 公开路径使用 `2m0s`；完整实现也覆盖测试和错误路径中的零、小时、毫秒、
/// 微秒和纳秒，并去掉小数尾部零。
fn format_go_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return format_subsecond(nanos, 1_000, "µs");
    }
    if nanos < 1_000_000_000 {
        return format_subsecond(nanos, 1_000_000, "ms");
    }

    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    let second_text = if duration.subsec_nanos() == 0 {
        format!("{seconds}s")
    } else {
        let fraction = format!("{:09}", duration.subsec_nanos())
            .trim_end_matches('0')
            .to_owned();
        format!("{seconds}.{fraction}s")
    };
    if hours > 0 {
        format!("{hours}h{minutes}m{second_text}")
    } else if minutes > 0 {
        format!("{minutes}m{second_text}")
    } else {
        second_text
    }
}

/// 格式化小于一秒的 Go duration 单位，并去掉九进制小数尾部零。
fn format_subsecond(nanos: u128, unit_nanos: u128, suffix: &str) -> String {
    let whole = nanos / unit_nanos;
    let remainder = nanos % unit_nanos;
    if remainder == 0 {
        return format!("{whole}{suffix}");
    }
    let width = unit_nanos.ilog10() as usize;
    let fraction = format!("{remainder:0width$}")
        .trim_end_matches('0')
        .to_owned();
    format!("{whole}.{fraction}{suffix}")
}
