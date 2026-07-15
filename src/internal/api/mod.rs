//! 对应 Go `internal/api` 包的 HTTP 客户端、会话事件和后端适配器。
//!
//! 模块保持 Go 文件边界，但使用异步 Reqwest 和 `CancellationToken` 表达 context 取消。
//! 所有 wire DTO 都显式声明 serde 名称，避免 Rust 字段命名影响现有后端协议。

mod ccr_backend;
mod client;
mod get_session_context;
mod noop_backend;
mod retry;
mod session_ingress_backend;
mod session_ingress_client;
mod session_ingress_types;
mod work_client;
mod zz_recovered_types;

pub use ccr_backend::CCRBackend;
pub use client::{ApiError, ClientOptions, HttpClient};
pub(crate) use client::{
    GoRequestErrorContext, mark_go_transport_response, prepare_go_transport_request,
    read_go_response_body,
};
pub use get_session_context::SessionsClient;
pub use noop_backend::NoopBackend;
pub use retry::RetryConfig;
pub use session_ingress_backend::SessionIngressBackend;
pub use session_ingress_client::HttpSessionIngressClient;
pub use session_ingress_types::{
    new_env_manager_log_event, new_env_manager_log_event_data, new_result_event,
    new_synthetic_assistant_message,
};
pub use work_client::WorkClient;
pub use zz_recovered_types::*;

#[cfg(test)]
#[path = "client_test.rs"]
mod client_test;
