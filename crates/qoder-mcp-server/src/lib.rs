//! Runtime tools exposed through the official rmcp SDK using MCP 2026-07-28.
//! Tool execution remains independent of the protocol and transport adapter.

pub mod environment;
pub mod execution;
mod json;
pub mod logging;
pub mod plugins;
pub mod protocol;
pub mod tools;
mod util;
mod wire_json;

pub use protocol::{Config, Server};
pub use tools::{ContentBlock, Registry, Tool, ToolResult};
