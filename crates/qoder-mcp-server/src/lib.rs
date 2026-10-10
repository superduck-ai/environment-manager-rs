//! 与 sandbox-runtime-tools 兼容的本地 MCP 服务。
//!
//! 工具实现使用 Rust 标准库、Tokio 和主流编解码库；兼容层保留工具的原始
//! 参数转换、错误文本和内容块字段省略规则。

pub mod environment;
mod json;
pub mod logging;
pub mod plugins;
pub mod protocol;
pub mod tools;
mod util;
mod wire_json;

pub use protocol::{Config, Server};
pub use tools::{ContentBlock, Registry, Tool, ToolResult};
