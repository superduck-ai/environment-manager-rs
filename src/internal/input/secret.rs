//! V1 work response 校验与 work secret 解码。
//!
//! secret 使用带 padding 的 URL-safe Base64；对外错误刻意隐藏底层 Base64/JSON
//! 细节，避免把秘密材料或解析位置写入用户可见日志。

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use thiserror::Error;

use super::types::{V1WorkResponse, WorkSecret};

/// secret 或 V1 work envelope 无法解析。
#[derive(Debug, Error)]
pub enum SecretError {
    /// secret 字符串为空。
    #[error("secret is empty")]
    Empty,
    /// Base64、内部 JSON 或 source 类型无效；具体错误不向外泄露。
    #[error("invalid secret")]
    Invalid,
    /// secret schema 版本不受当前 runner 支持。
    #[error(
        "unsupported secret version. Download the latest version of the Claude Environment Runner"
    )]
    UnsupportedVersion,
    /// V1 work envelope JSON 无法解析。
    #[error("failed to parse V1 work response: {0}")]
    InvalidWorkResponse(#[source] serde_json::Error),
    /// 顶层 work type 不是固定值 `work`。
    #[error("unexpected type {actual:?} (expected 'work')")]
    UnexpectedWorkType {
        /// 实际收到的 type。
        actual: String,
    },
    /// session work 缺少 data.id。
    #[error("missing session ID in data.id")]
    MissingSessionId,
    /// data.type 既不是 session 也不是 healthcheck。
    #[error("unexpected data.type {actual:?} (expected 'session' or 'healthcheck')")]
    UnexpectedDataType {
        /// 实际收到的 data.type。
        actual: String,
    },
}

/// 解码 padded URL-safe Base64 work secret，并检查 schema 版本。
pub fn decode_work_secret(encoded: &str) -> Result<WorkSecret, SecretError> {
    if encoded.is_empty() {
        return Err(SecretError::Empty);
    }
    let decoded = URL_SAFE.decode(encoded).map_err(|_| SecretError::Invalid)?;
    let secret: WorkSecret = serde_json::from_slice(&decoded).map_err(|_| SecretError::Invalid)?;
    if secret.version != 1 {
        return Err(SecretError::UnsupportedVersion);
    }
    Ok(secret)
}

/// 解析并校验 V1 work envelope 的固定 type 与 data 联合体。
pub fn parse_v1_work_response(data: &[u8]) -> Result<V1WorkResponse, SecretError> {
    let response: V1WorkResponse =
        serde_json::from_slice(data).map_err(SecretError::InvalidWorkResponse)?;
    if response.work_type != "work" {
        return Err(SecretError::UnexpectedWorkType {
            actual: response.work_type,
        });
    }
    match response.data.data_type.as_str() {
        "session" if response.data.id.is_empty() => Err(SecretError::MissingSessionId),
        "session" | "healthcheck" => Ok(response),
        _ => Err(SecretError::UnexpectedDataType {
            actual: response.data.data_type,
        }),
    }
}

impl V1WorkResponse {
    /// 解码当前 response 携带的 work secret。
    pub fn decode_secret(&self) -> Result<WorkSecret, SecretError> {
        decode_work_secret(&self.secret)
    }
}
