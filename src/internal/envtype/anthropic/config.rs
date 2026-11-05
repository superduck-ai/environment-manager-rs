//! Anthropic JSON 配置的解码与必填字段校验。

use super::{AnthropicError, Config};

/// 解码一份 Anthropic 环境配置，并要求 `cwd` 非空。
///
/// 缺失字段采用类型默认值；字段类型错误与缺少 CWD 的英文文本保持稳定前缀。
pub fn decode_config(data: &[u8]) -> Result<Config, AnthropicError> {
    let config: Config = serde_json::from_slice(data).map_err(|error| {
        AnthropicError::with_source("failed to deserialize anthropic config", error)
    })?;
    if config.cwd.is_empty() {
        return Err(AnthropicError::message(
            "anthropic config missing required cwd",
        ));
    }
    Ok(config)
}
