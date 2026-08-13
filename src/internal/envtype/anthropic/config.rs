//! Anthropic JSON 配置的解码与必填字段校验。

use super::{AnthropicError, Config};

/// 解码一份 Anthropic 环境配置，并要求 `cwd` 非空。
///
/// JSON `null` 对字符串、切片和 map 的处理由类型层映射为默认值；解析错误
/// 与缺少 CWD 的英文文本保持既定的稳定前缀。
pub fn decode_config(data: &[u8]) -> Result<Config, AnthropicError> {
    let config: Config = serde_json::from_slice::<Option<Config>>(data)
        .map_err(|error| {
            AnthropicError::with_source("failed to unmarshal anthropic config", error)
        })?
        .unwrap_or_default();
    if config.cwd.is_empty() {
        return Err(AnthropicError::message(
            "anthropic config missing required cwd",
        ));
    }
    Ok(config)
}
