//! code-sign MCP server。
//!
//! 服务读取绝对路径文件，把经过脱敏的 source 信息发送到环境 API，并将返回的
//! SSH-style Ed25519 signature 以 0600 mode 写回本地。它复用顶层 MCP 协议层，但
//! 不通过 `claude mcp add` 注册，因为 Claude 由专用环境变量发现它。

mod registration;
mod server;
mod sign_operations;
mod types;

pub use registration::{configure_server, registration, should_register};
pub use sign_operations::{
    ERR_EMPTY_CONTENT, ERR_SIGNING_FAILED, read_file_content, sanitize_source,
};
pub use types::{
    CodeSignMCPServer, RemoteSignRequest, RemoteSignResponse, SignRequest, SignResponse, SourceInfo,
};

#[cfg(test)]
mod server_test;
