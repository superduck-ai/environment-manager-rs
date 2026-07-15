//! V1 work response 校验与 work secret 解码。
//!
//! secret 使用带 padding 的 URL-safe Base64；对外错误刻意隐藏底层 Base64/JSON
//! 细节，避免把秘密材料或解析位置写入用户可见日志。

use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use serde::{Deserialize, Deserializer};
use thiserror::Error;

use crate::internal::auth::AuthConfig;
use crate::internal::config::{McpConfigFile, Source};

use super::zz_recovered_types::{V1WorkResponse, WorkSecret};

/// 把 work secret 中显式 JSON `null` 的值类型字段还原为 Go 零值。
///
/// Serde 的 `default` 只覆盖字段缺失；Go `encoding/json` 还允许 string、int 和 bool
/// 字段显式为 null。统一 helper 可保证版本校验等后续分支顺序不被提前类型错误改变。
fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

/// 解析可选的 Go 值类型 slice，并把其中 null 元素转换为元素零值。
///
/// 整个字段为 null 时仍返回 None，以保留 nil 与显式空数组的运行时差别；auth 数组
/// 中的 null 元素则对应一个零值 AuthConfig，随后由认证层按 Go 顺序报告未知 provider。
fn deserialize_optional_null_default_vec<'de, D, T>(
    deserializer: D,
) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    let values = Option::<Vec<Option<T>>>::deserialize(deserializer)?;
    Ok(values.map(|values| values.into_iter().map(Option::unwrap_or_default).collect()))
}

/// 解析可选 `map[string]T`，保留 null map，并把 null value 写成元素零值。
///
/// Go 会把 `{"KEY":null}` 解为包含空字符串的 map；该细节会影响环境变量与预计算
/// Claude 参数是否存在，不能把整个 map 因单个 null value 判为无效 secret。
fn deserialize_optional_null_default_map_values<'de, D, T>(
    deserializer: D,
) -> Result<Option<HashMap<String, T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    let values = Option::<HashMap<String, Option<T>>>::deserialize(deserializer)?;
    Ok(values.map(|values| {
        values
            .into_iter()
            .map(|(key, value)| (key, value.unwrap_or_default()))
            .collect()
    }))
}

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

#[derive(Default, Deserialize)]
struct RawWorkSecret {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    version: i64,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    session_ingress_token: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    api_base_url: String,
    #[serde(default)]
    sources: Option<Vec<Source>>,
    #[serde(default, deserialize_with = "deserialize_optional_null_default_vec")]
    auth: Option<Vec<AuthConfig>>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_null_default_map_values"
    )]
    claude_code_args: Option<HashMap<String, String>>,
    #[serde(default, rename = "mcp_config")]
    mcp_config: Option<McpConfigFile>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    use_sandbox_gateway_config: bool,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_null_default_map_values"
    )]
    environment_variables: Option<HashMap<String, String>>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    use_code_sessions: bool,
}

impl<'de> Deserialize<'de> for WorkSecret {
    /// 解析 work secret，并将显式 JSON `null` 的 source/auth 数组映射为空数组。
    ///
    /// 该 serde trait 实现就是 Go `WorkSecret.UnmarshalJSON` 的强类型入口：先解析
    /// 固定 wire 字段，再由 `Source` 的封闭反序列化逐项验证 source 类型；任何未知
    /// 类型或字段类型错误都会沿当前 deserializer 返回，绝不静默构造半成品。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Go 把顶层 null 解进 raw struct 后继续使用全部零值，最终由 version 门禁
        // 返回“unsupported”，而不是在 JSON 阶段折叠为“invalid secret”。
        let raw = Option::<RawWorkSecret>::deserialize(deserializer)?.unwrap_or_default();
        Ok(Self {
            version: raw.version,
            session_ingress_token: raw.session_ingress_token,
            api_base_url: raw.api_base_url,
            sources: raw.sources.unwrap_or_default(),
            auth: raw.auth.unwrap_or_default(),
            claude_code_args: raw.claude_code_args,
            mcp_config: raw.mcp_config,
            use_sandbox_gateway_config: raw.use_sandbox_gateway_config,
            environment_variables: raw.environment_variables,
            use_code_sessions: raw.use_code_sessions,
        })
    }
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
