//! 本地 Model Context Protocol 服务器与注册表。
//!
//! [`BaseServer`] 在 loopback 随机端口上暴露 `/mcp`，
//! [`MCPServerRegistration`] 把业务服务器接入 Claude CLI。业务代码只依赖稳定的
//! JSON-RPC/MCP wire 层，不直接依赖第三方 MCP 框架，
//! 官方 `rmcp` SDK 被限制在适配层内，不会改变 codesign/supabase API。

mod registry;
mod rmcp_adapter;
mod server;
pub mod servers;
mod types;

pub use registry::{
    get_mcp_registrations, new_registration, register_server, register_with_claude,
};
pub use server::{MCP_ENDPOINT_PATH, dispatch_json_rpc};
pub use types::{
    BaseServer, CallToolParams, CallToolRequest, CallToolResult, MCPServer, MCPServerConfigurator,
    MCPServerFactory, MCPServerRegistration, McpError, McpResult, ServerConfig, ShouldRegister,
    TextContent, Tool, ToolConfig, ToolHandler, new_tool_result_error, new_tool_result_text,
};

#[cfg(test)]
mod registry_test;
#[cfg(test)]
mod rmcp_adapter_test;
#[cfg(test)]
mod server_test;
