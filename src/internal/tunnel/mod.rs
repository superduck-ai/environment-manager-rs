//! 后端 Tunnel、HTTP/WebSocket 转发与控制 action。
//!
//! `actions`、Tunnel WebSocket 客户端和本地代理处理器共享强类型协议消息。

pub mod actions;
mod client;
#[cfg(test)]
mod client_test;
mod flow;
#[cfg(test)]
mod flow_test;
mod handler;
#[cfg(test)]
mod handler_test;
mod helpers;
mod local_ws;
mod types;
mod ws_handler;
#[cfg(test)]
mod ws_handler_test;

pub(crate) use client::connect_websocket_with_config;
pub use client::{add_jitter, new_client, with_action_registry};
pub use flow::FLOW_CONTROL_WINDOW;
#[cfg(test)]
pub(crate) use handler::new_handler_with_timing_for_test;
pub use handler::{HTTP_TUNNEL_CHUNK_SIZE, new_handler};
pub use helpers::{
    apply_tunnel_headers, apply_websocket_tunnel_headers, encode_protojson, flatten_http_headers,
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
    HttpTunnelResponseHeaders, TunnelRequest, TunnelResponse, TunnelWindowUpdate, WsTunnelClose,
    WsTunnelError, WsTunnelMessage, WsTunnelOpen, WsTunnelOpened, tunnel_request, tunnel_response,
};
pub use actions::ResponseSender;
