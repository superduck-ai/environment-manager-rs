//! 后端 Tunnel、HTTP/WebSocket 转发与控制 action。
//!
//! 对应 Go `internal/tunnel`。本阶段先暴露已经迁移的 `actions`；Tunnel WebSocket
//! 客户端和本地代理处理器将在同一目录继续接入这些强类型协议消息。

pub mod actions;
mod client;
#[cfg(test)]
mod client_test;
mod handler;
#[cfg(test)]
mod handler_test;
mod helpers;
mod types;
mod ws_handler;
#[cfg(test)]
mod ws_handler_test;

pub(crate) use client::connect_websocket_with_config;
pub use client::{add_jitter, new_client, with_action_registry};
pub use handler::{HTTP_TUNNEL_CHUNK_SIZE, new_handler, new_handler_with_timing_for_probe};
pub use helpers::{
    apply_tunnel_headers, apply_websocket_tunnel_headers, flatten_http_headers, marshal_protojson,
};
pub use types::{
    Client, ClientOption, Handler, TunnelError, WsHandler, WsResponseSender, WsSenderError,
};
pub(crate) use types::{
    ClientWsResponseSender, HttpResponseSender, LocalWebSocket, LocalWebSocketSink,
    LocalWebSocketStream, WsTunnel,
};
pub use ws_handler::new_ws_handler;

pub use crate::r#gen::proto::anthropic::sessions::tunnel::v1alpha::{
    Header, HttpTunnelCancel, HttpTunnelRequest, HttpTunnelResponseChunk, HttpTunnelResponseError,
    HttpTunnelResponseHeaders, TunnelRequest, TunnelResponse, WsTunnelClose, WsTunnelError,
    WsTunnelMessage, WsTunnelOpen, WsTunnelOpened, tunnel_request, tunnel_response,
};
pub use actions::ResponseSender;
