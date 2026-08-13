//! Supabase Edge Function 文件收集与 multipart deploy。
//!
//! 所有上传文件必须解析到显式 trust root 内；symlink 和非普通文件不会上传。单文件
//! 上限 1 MiB、总上限 10 MiB，multipart 中 function 文件相对 slug 目录，shared
//! 文件则保留 `_shared/` 前缀。

use std::path::{Path, PathBuf};

use reqwest::multipart::{Form, Part};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

use crate::internal::mcp::{McpError, McpResult};

use super::{
    Client, DeployFunctionResult, FunctionDeployMetadata, FunctionFile,
    client::read_limited_response,
};

const MAX_SLUG_LENGTH: usize = 64;
const MAX_FUNCTION_FILE_SIZE: u64 = 1024 * 1024;
const MAX_FUNCTION_TOTAL_SIZE: u64 = 10 * 1024 * 1024;
const DEPLOY_RESPONSE_LIMIT: usize = 64 * 1024;

impl Client {
    /// 验证并部署一个 Edge Function 目录。
    pub async fn deploy_function(
        &self,
        cancellation: &CancellationToken,
        slug: &str,
        function_directory: &Path,
        shared_directory: Option<&Path>,
        trust_root: &Path,
    ) -> McpResult<DeployFunctionResult> {
        if slug.len() > MAX_SLUG_LENGTH {
            return Err(McpError::message(format!(
                "slug {slug:?} exceeds {MAX_SLUG_LENGTH} characters"
            )));
        }
        if !is_function_slug(slug) {
            return Err(McpError::message(format!(
                "invalid slug {slug:?}: must match [A-Za-z][A-Za-z0-9_-]*"
            )));
        }
        let entrypoint = function_directory.join("index.ts");
        if let Err(error) = std::fs::metadata(&entrypoint) {
            return Err(McpError::message(format!(
                "entrypoint index.ts not found in {}: {error}",
                function_directory.display()
            )));
        }

        let (files, total_size) =
            collect_function_files(function_directory, shared_directory, trust_root)
                .map_err(|error| McpError::message(format!("collect function files: {error}")))?;
        if total_size > MAX_FUNCTION_TOTAL_SIZE {
            return Err(McpError::message(format!(
                "function source exceeds {MAX_FUNCTION_TOTAL_SIZE} bytes (got {total_size})"
            )));
        }

        let metadata = FunctionDeployMetadata {
            entrypoint_path: "index.ts".to_owned(),
            name: slug.to_owned(),
            verify_jwt: true,
        };
        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|error| McpError::message(format!("encode metadata: {error}")))?;
        let mut form = Form::new().text("metadata", metadata_json);
        for file in files {
            let bytes = tokio::fs::read(&file.abs_path).await.map_err(|error| {
                McpError::message(format!("open {}: {error}", file.abs_path.display()))
            })?;
            let part = Part::bytes(bytes).file_name(file.rel_path.clone());
            form = form.part("file", part);
        }

        let escaped_slug = query_escape(slug);
        let endpoint = format!(
            "{}/projects/{}/functions/deploy?slug={escaped_slug}",
            self.base_url, self.project_ref
        );
        let request = self
            .deploy_http_client
            .post(endpoint)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.pat),
            )
            .multipart(form);
        let response = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(McpError::message("deploy request failed: context canceled"));
            }
            result = tokio::time::timeout(std::time::Duration::from_secs(120), request.send()) => {
                match result {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => return Err(McpError::message(format!("deploy request failed: {error}"))),
                    Err(_) => return Err(McpError::message("deploy request failed: context deadline exceeded")),
                }
            }
        };
        let status = response.status();
        let body = read_limited_response(cancellation, response, DEPLOY_RESPONSE_LIMIT)
            .await
            .map_err(|error| McpError::message(format!("read deploy response: {error}")))?;
        match status {
            reqwest::StatusCode::CREATED => serde_json::from_slice(&body)
                .map_err(|error| McpError::message(format!("parse deploy response: {error}"))),
            reqwest::StatusCode::PAYMENT_REQUIRED => Err(McpError::message(
                "max functions reached for this Supabase plan (402)",
            )),
            reqwest::StatusCode::TOO_MANY_REQUESTS => {
                Err(McpError::message("rate limited by Supabase API (429)"))
            }
            _ => Err(McpError::message(format!(
                "deploy failed with status {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body).trim()
            ))),
        }
    }
}

/// 收集 function 和可选 shared 目录中的普通文件。
pub fn collect_function_files(
    function_directory: &Path,
    shared_directory: Option<&Path>,
    trust_root: &Path,
) -> McpResult<(Vec<FunctionFile>, u64)> {
    let resolved_root = std::fs::canonicalize(trust_root)
        .map_err(|error| McpError::message(format!("resolve trustRoot: {error}")))?;
    let resolved_function = require_dir_under(function_directory, &resolved_root)
        .map_err(|error| McpError::message(format!("funcDir: {error}")))?;

    let mut files = Vec::new();
    let mut total_size = 0_u64;
    collect_directory(
        &resolved_function,
        &resolved_function,
        &mut files,
        &mut total_size,
    )
    .map_err(|error| McpError::message(format!("walk funcDir: {error}")))?;

    if let Some(shared_directory) = shared_directory {
        // EvalSymlinks on a missing/broken shared directory returns ErrNotExist; ignores it.
        match std::fs::canonicalize(shared_directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(McpError::message(format!("sharedDir: {error}"))),
            Ok(_) => {
                let resolved_shared = require_dir_under(shared_directory, &resolved_root)
                    .map_err(|error| McpError::message(format!("sharedDir: {error}")))?;
                let relative_root = resolved_shared.parent().unwrap_or(&resolved_root);
                collect_directory(&resolved_shared, relative_root, &mut files, &mut total_size)
                    .map_err(|error| McpError::message(format!("walk sharedDir: {error}")))?;
            }
        }
    }
    Ok((files, total_size))
}

/// 解析目录并确认它没有逃出 canonical trust root。
pub fn require_dir_under(path: &Path, resolved_root: &Path) -> McpResult<PathBuf> {
    let resolved_path =
        std::fs::canonicalize(path).map_err(|error| McpError::message(error.to_string()))?;
    if resolved_path.strip_prefix(resolved_root).is_err() {
        return Err(McpError::message(format!(
            "{} resolves to {}, outside trust root",
            path.display(),
            resolved_path.display()
        )));
    }
    let metadata = std::fs::metadata(&resolved_path)
        .map_err(|error| McpError::message(format!("stat resolved path: {error}")))?;
    if !metadata.is_dir() {
        return Err(McpError::message(format!(
            "{} is not a directory",
            resolved_path.display()
        )));
    }
    Ok(resolved_path)
}

/// 遍历一个已验证目录，跳过 symlink/设备并计算 multipart 相对路径。
fn collect_directory(
    directory: &Path,
    relative_root: &Path,
    files: &mut Vec<FunctionFile>,
    total_size: &mut u64,
) -> McpResult<()> {
    for entry in WalkDir::new(directory).sort_by_file_name() {
        let entry = entry.map_err(|error| McpError::message(error.to_string()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let metadata = entry.metadata().map_err(|error| {
            McpError::message(format!("stat {}: {error}", entry.path().display()))
        })?;
        if metadata.len() > MAX_FUNCTION_FILE_SIZE {
            return Err(McpError::message(format!(
                "file {} exceeds {MAX_FUNCTION_FILE_SIZE} bytes",
                entry.path().display()
            )));
        }
        let relative = entry.path().strip_prefix(relative_root).map_err(|error| {
            McpError::message(format!(
                "compute relative path for {}: {error}",
                entry.path().display()
            ))
        })?;
        files.push(FunctionFile {
            abs_path: entry.path().to_path_buf(),
            rel_path: relative
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/"),
        });
        *total_size += metadata.len();
    }
    Ok(())
}

/// 无 regex 依赖地实现 `^[A-Za-z][A-Za-z0-9_-]*$`。
pub(crate) fn is_function_slug(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// 实现form URL 编码（空格写成 `+`）。
pub(crate) fn query_escape(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut escaped = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            // encodeQueryComponent 只保留 RFC 3986 unreserved 字节；
            // WHATWG form serializer 会额外保留 `*` 并转义 `~`，不能在这里复用。
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(char::from(byte));
            }
            b' ' => escaped.push('+'),
            _ => {
                escaped.push('%');
                escaped.push(char::from(HEX[usize::from(byte >> 4)]));
                escaped.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
    escaped
}
