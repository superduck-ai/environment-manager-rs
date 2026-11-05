//! 本地 Git smart-HTTP proxy。
//!
//! manager 负责幂等生命周期与 clone URL，server 只监听 IPv4 loopback，handler 把受控仓库请求转发到 session
//! ingress。网络栈统一使用 Tokio、Axum、Reqwest 与 rustls，不依赖 OpenSSL/git2。

mod handler;
mod manager;
mod server;
mod types;

pub use manager::new_manager;
pub use server::new_server;
pub use types::{
    Config, GitProxyError, GitProxyResult, Manager, RepoAuth, Server, SharedManager, SharedServer,
};

#[cfg(test)]
#[path = "gitproxy_test.rs"]
mod gitproxy_test;
