//! `rmcp` 隔离适配层的 wire 保真测试。
//!
//! 这些测试验证官方 SDK 参与模型校验时，不会裁剪 mcp-go 接受的未知字段，也不会
//! 改写 `omitempty` 结果。它们补充 `server_test.rs` 的 HTTP/JSON-RPC 端到端断言。

use serde_json::{Map, json};

use super::{CallToolResult, TextContent, Tool, rmcp_adapter};

/// 验证工具声明经过 SDK 校验后仍原样保留未知扩展字段。
#[test]
fn tool_declaration_preserves_go_extensions() {
    let mut tool = Tool::new(
        "inspect",
        "检查环境",
        json!({"type": "object", "properties": {}}),
    );
    tool.extra.insert("annotations".to_owned(), json!({}));
    tool.extra.insert("futureField".to_owned(), json!(7));

    let value = rmcp_adapter::serialize_tool_declarations(&[tool]).expect("valid MCP tool");

    assert_eq!(value[0]["futureField"], json!(7));
    assert_eq!(value[0]["inputSchema"]["type"], json!("object"));
    assert_eq!(
        value[0],
        json!({
            "annotations": {},
            "description": "检查环境",
            "futureField": 7,
            "inputSchema": {"type": "object", "properties": {}},
            "name": "inspect"
        })
    );
}

/// 验证非 object schema 在进入 HTTP 响应前被适配层拒绝。
#[test]
fn tool_declaration_rejects_non_object_schema() {
    let tool = Tool::new("invalid", "无效 schema", json!(["not", "an", "object"]));

    let error = rmcp_adapter::serialize_tool_declarations(&[tool])
        .expect_err("array schema must be rejected");

    assert!(
        error
            .to_string()
            .contains("inputSchema must be a JSON object")
    );
}

/// 验证成功结果保持 mcp-go 的 `isError` 省略规则和扩展字段。
#[test]
fn call_tool_result_preserves_omitempty_and_extensions() {
    let mut extra = Map::new();
    extra.insert("futureResultField".to_owned(), json!(true));
    let result = CallToolResult {
        content: vec![TextContent {
            content_type: "text".to_owned(),
            text: "ok".to_owned(),
            extra: Map::new(),
        }],
        structured_content: None,
        is_error: false,
        extra,
    };

    let value = rmcp_adapter::serialize_call_tool_result(result).expect("valid MCP result");

    assert!(value.get("isError").is_none());
    assert_eq!(value["futureResultField"], json!(true));
}
