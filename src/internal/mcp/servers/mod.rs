//! 内置 MCP 业务服务器。
//!
//! 子模块保留 包边界：codesign 负责远程 Ed25519 签名，supabase 负责数据库
//! 管理与 Edge Function 部署。注册动作由上层启动流程显式调用，避免 Rust 模块加载
//! 隐式修改全局状态。

pub mod codesign;
pub mod supabase;

/// 本地文件、命令和搜索工具的兼容 MCP 服务。
pub use mcp_server as runtime_tools;
