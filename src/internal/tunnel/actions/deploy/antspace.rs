//! Antspace tar.gz 打包和 control plane 上传。
//!
//! 项目根目录及其子目录都会写入 tar header，symlink 被跳过，文件总量受与 Vercel
//! 相同的 100 MiB 上限约束。上传使用完整内存 multipart body，匹配 Go 实现的内存与
//! 请求时序。

use std::io::Cursor;
use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};
use std::path::Path;
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::StatusCode;
use tar::{Builder as TarBuilder, Header};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::internal::tunnel::actions::{ActionError, ProgressUpdate};

use super::types::{AntspaceClient, AntspaceDeployResult, AntspaceResponse, OptionalProgress};
use super::vercel::MAX_DEPLOYMENT_BYTES;

/// Antspace 整次 HTTP 请求的固定超时，对应 Go 的十五分钟 `http.Client.Timeout`。
const ANTSPACE_REQUEST_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// 对应 Go：`NewAntspaceClient`。
///
/// URL 原样保存，不删除末尾斜杠；部署时固定再拼 `"/deploy"`，因此配置自带斜杠时
/// 仍会产生 Go 一样的双斜杠路径。
#[must_use]
pub fn new_antspace_client(
    control_plane_url: impl Into<String>,
    auth_token: impl Into<String>,
) -> AntspaceClient {
    let http_client = reqwest::Client::builder()
        .timeout(ANTSPACE_REQUEST_TIMEOUT)
        .build()
        .expect("building a reqwest client with a fixed timeout cannot fail");
    AntspaceClient {
        control_plane_url: control_plane_url.into(),
        auth_token: auth_token.into(),
        http_client,
    }
}

/// 对应 Go：`CreateTarball`。
///
/// 使用名称排序的深度优先遍历，先保留根目录 `.` 和每个目录 header，再写普通或其他
/// 非目录、非 symlink 项的数据。文件读取完成后才检查累计上限；任何遍历/读取/写入
/// 失败都会丢弃部分 archive，并带上项目根目录上下文。
pub fn create_tarball(
    project_directory: impl AsRef<Path>,
) -> Result<(Vec<u8>, usize), ActionError> {
    let project_directory = project_directory.as_ref();
    let gzip_writer = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar_writer = TarBuilder::new(gzip_writer);
    let mut file_count = 0usize;
    let mut total_bytes = 0usize;

    for entry in WalkDir::new(project_directory).sort_by_file_name() {
        let entry = entry.map_err(|error| {
            format!(
                "create tarball from {}: walk error: {error}",
                project_directory.display()
            )
        })?;
        if entry.file_type().is_symlink() {
            continue;
        }

        let path = entry.path();
        let relative = path.strip_prefix(project_directory).map_err(|error| {
            format!(
                "create tarball from {}: relative path {}: {error}",
                project_directory.display(),
                path.display()
            )
        })?;
        let relative = if relative.as_os_str().is_empty() {
            Path::new(".")
        } else {
            relative
        };
        let metadata = std::fs::symlink_metadata(path).map_err(|error| {
            format!(
                "create tarball from {}: stat {}: {error}",
                project_directory.display(),
                path.display()
            )
        })?;
        if metadata.file_type().is_socket() {
            return Err(format!(
                "create tarball from {}: tar header {}: archive/tar: sockets not supported",
                project_directory.display(),
                path.display()
            )
            .into());
        }
        let mut header = Header::new_ustar();
        header.set_metadata(&metadata);
        // tar-rs 默认写入 `st_mode`（含 S_IFREG/S_IFDIR）；Go FileInfoHeader 只保留权限
        // 与 setuid/setgid/sticky 位，文件类型由 typeflag 单独表达。
        header.set_mode(metadata.mode() & 0o7777);

        if entry.file_type().is_dir() {
            tar_writer
                .append_data(&mut header, relative, std::io::empty())
                .map_err(|error| {
                    format!(
                        "create tarball from {}: write tar header {}: {error}",
                        project_directory.display(),
                        path.display()
                    )
                })?;
            continue;
        }

        file_count += 1;
        let data = std::fs::read(path).map_err(|error| {
            format!(
                "create tarball from {}: read file {}: {error}",
                project_directory.display(),
                path.display()
            )
        })?;
        total_bytes = total_bytes.saturating_add(data.len());
        if total_bytes > MAX_DEPLOYMENT_BYTES {
            return Err(format!(
                "create tarball from {}: deployment exceeds max size",
                project_directory.display()
            )
            .into());
        }
        tar_writer
            .append_data(&mut header, relative, Cursor::new(data))
            .map_err(|error| {
                format!(
                    "create tarball from {}: write tar data {}: {error}",
                    project_directory.display(),
                    path.display()
                )
            })?;
    }

    let gzip_writer = tar_writer
        .into_inner()
        .map_err(|error| format!("close tar writer: {error}"))?;
    let bytes = gzip_writer
        .finish()
        .map_err(|error| format!("close gzip writer: {error}"))?;
    Ok((bytes, file_count))
}

impl AntspaceClient {
    /// 对应 Go：`(*AntspaceClient).Deploy`。
    ///
    /// multipart 字段顺序为 `app` 后 `dist`，文件名固定为 `dist.tar.gz`，并设置 bearer
    /// token。成功的 control plane 响应由 `read_deploy_response` 统一解析和上报 100%。
    pub async fn deploy(
        &self,
        cancellation: &CancellationToken,
        app_name: &str,
        tarball: Vec<u8>,
        progress: OptionalProgress,
    ) -> Result<Option<AntspaceDeployResult>, ActionError> {
        let (body, content_type) = build_multipart_body(app_name, &tarball);
        let url = format!("{}/deploy", self.control_plane_url);
        let request = self
            .http_client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .header("Content-Type", content_type)
            .body(body)
            .build()
            .map_err(|error| format!("failed to create deploy request: {error}"))?;
        self.read_deploy_response(cancellation, request, progress)
            .await
    }

    /// 对应 Go：`(*AntspaceClient).readDeployResponse`。
    ///
    /// 只接受 200；非 200 原样包含响应体。成功体的 JSON 解析错误同时附上原 body，
    /// API 的 `error` 字段优先转为业务错误。最终进度发送失败仅记录 warning，不改变已
    /// 成功的部署结果。
    async fn read_deploy_response(
        &self,
        cancellation: &CancellationToken,
        request: reqwest::Request,
        progress: OptionalProgress,
    ) -> Result<Option<AntspaceDeployResult>, ActionError> {
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err("deploy request failed: context canceled".into());
            }
            result = self.http_client.execute(request) => {
                result.map_err(|error| format!("deploy request failed: {error}"))?
            }
        };
        let status = response.status();
        if status != StatusCode::OK {
            // Go 在非 200 分支忽略 `io.ReadAll` 错误；取消或读取失败不能覆盖已经取得的
            // HTTP status，因此没有可用 body 时仍返回 status 错误。
            let body = tokio::select! {
                biased;
                () = cancellation.cancelled() => Vec::new(),
                result = response.bytes() => {
                    result.map_or_else(|_| Vec::new(), |bytes| bytes.to_vec())
                }
            };
            return Err(format!(
                "deploy request failed (status {}): {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            )
            .into());
        }
        let body = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err("failed to read deploy response: context canceled".into());
            }
            result = response.bytes() => {
                result
                    .map_err(|error| format!("failed to read deploy response: {error}"))?
                    .to_vec()
            }
        };

        let response: AntspaceResponse = serde_json::from_slice(&body).map_err(|error| {
            format!(
                "failed to parse deploy response: {error}\nbody: {}",
                String::from_utf8_lossy(&body)
            )
        })?;
        let _status_is_intentionally_ignored = &response.status;
        if !response.error.is_empty() {
            return Err(format!("antspace deploy error: {}", response.error).into());
        }

        if let Some(progress) = progress
            && let Err(error) = progress
                .send_progress(ProgressUpdate {
                    step: "deployed".to_owned(),
                    message: format!("Deployed to {}", response.url),
                    percent: 100.0,
                })
                .await
        {
            // 此事件必须在 Go 对应分支直接调用 facade；若经动态 wrapper 转发，
            // `#[track_caller]` 会把 source 错记为 wrapper，而不是进度失败分支。
            tracing::warn!("error" = ?(error.to_string()), "{}", "antspace_progress_send_error");
        }

        Ok(Some(AntspaceDeployResult {
            url: response.url,
            service: response.service,
        }))
    }
}

/// 在内存中构造与 Go `mime/multipart.Writer` 相同顺序的完整请求体。
///
/// boundary 使用 30 个随机字节的 60 位小写十六进制；`app` field 后接 `dist` file，
/// 每段采用 CRLF，并以 `--boundary--\r\n` 收尾。提前完成整个 body 保留 Go 在发 HTTP
/// 请求前已经承担全部 multipart 内存开销和序列化工作的时序。
fn build_multipart_body(app_name: &str, tarball: &[u8]) -> (Vec<u8>, String) {
    let random_bytes = rand::random::<[u8; 30]>();
    let boundary = random_bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut body = Vec::with_capacity(app_name.len() + tarball.len() + 512);
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"app\"\r\n\r\n");
    body.extend_from_slice(app_name.as_bytes());
    body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"dist\"; filename=\"dist.tar.gz\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
    body.extend_from_slice(tarball);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let content_type = format!("multipart/form-data; boundary={boundary}");
    (body, content_type)
}
