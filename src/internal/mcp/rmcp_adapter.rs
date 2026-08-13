//! `rmcp` 官方 SDK 与保真 wire 类型之间的隔离适配层。
//!
//! 本层隔离 `rmcp` SDK；外层仍由 `server.rs` 实现 MCP wire 的状态码、错误文本、字段集合和未知字段
//! 保留规则，本层只用官方 SDK 校验标准 MCP 数据模型。这样升级 SDK 不会悄悄改变
//! 已经固化的对外协议。

use std::sync::Arc;

use serde_json::Value;

use super::{CallToolResult, McpError, McpResult, Tool};

/// 使用 `rmcp::model::Tool` 校验工具声明，同时返回未经裁剪的 wire JSON。
///
/// 把本地工具声明转换为 SDK 校验模型。
///
/// # 参数
/// - `tools`：已经按名称排序的本地工具声明；其 `inputSchema` 必须是 JSON object。
///
/// # 返回
/// 成功时返回保持原字段顺序和扩展字段的 JSON array；schema 不是 object，或本地类型
/// 无法序列化时返回 MCP 内部错误。
///
/// # Wire 保真
/// `rmcp` 的 `Tool` 会随新版 MCP 增加字段。这里只构造 SDK 模型来验证标准字段，最终
/// 仍序列化本地值，从而保留 MCP wire 接受的未知字段，避免 SDK 默认值污染 wire 输出。
pub(super) fn serialize_tool_declarations(tools: &[Tool]) -> McpResult<Value> {
    let mut compatible_values = Vec::with_capacity(tools.len());
    for tool in tools {
        let schema = tool.input_schema.as_object().cloned().ok_or_else(|| {
            McpError::message(format!(
                "tool '{}' inputSchema must be a JSON object",
                tool.name
            ))
        })?;

        // 构造官方 SDK 模型是有意的协议校验点；不能直接拿它的序列化结果响应，
        // 因为 SDK 不认识的扩展字段会在这里被裁剪。
        let _validated = rmcp::model::Tool::new(
            tool.name.clone(),
            tool.description.clone(),
            Arc::new(schema),
        );
        let compatible_value = serde_json::to_value(tool)
            .map_err(|error| McpError::message(format!("serialize MCP tool: {error}")))?;
        let object = compatible_value.as_object().ok_or_else(|| {
            McpError::message(format!(
                "serialize MCP tool '{}': expected object",
                tool.name
            ))
        })?;
        let mut fields = object.iter().collect::<Vec<_>>();
        fields.sort_by_key(|(left, _)| *left);
        let mut sorted = serde_json::Map::with_capacity(fields.len());
        for (name, value) in fields {
            sorted.insert(name.clone(), value.clone());
        }
        // MCP wire 要求动态 map key 按字典序输出。Tool 在边界经过
        // map 序列化，因此这个顺序是 transcript 的可观察部分，不能依赖
        // Rust struct 字段声明顺序。
        compatible_values.push(Value::Object(sorted));
    }
    Ok(Value::Array(compatible_values))
}

/// 使用 `rmcp::model::CallToolResult` 校验工具结果，并保留 wire 字段集合。
///
/// 把 `tools/call` handler 结果转换为 JSON-RPC wire 值。
///
/// # 参数
/// - `result`：业务 handler 返回的本地结果，可能包含 MCP wire 保留下来的扩展字段。
///
/// # 返回
/// 成功时返回原始 wire JSON object；若结果不符合官方 MCP 模型则返回内部错误。
///
/// # Wire 保真
/// 反序列化只作为标准模型校验，响应继续使用校验前的 JSON，因而 `isError: false` 的
/// JSON 零值省略行为和未知字段都不会被 `rmcp` 的默认序列化策略改变。
pub(super) fn serialize_call_tool_result(result: CallToolResult) -> McpResult<Value> {
    let compatible_value = serde_json::to_value(result)
        .map_err(|error| McpError::message(format!("serialize MCP tool result: {error}")))?;
    let _: rmcp::model::CallToolResult = serde_json::from_value(compatible_value.clone())
        .map_err(|error| McpError::message(format!("validate MCP tool result: {error}")))?;
    Ok(compatible_value)
}
