//! Supabase MCP server。
//!
//! 模块同时覆盖 Management API client、数据库迁移/查询/type generation、Edge Function
//! multipart deploy，以及 baku session 的按需数据库 provisioning。

mod client;
mod functions;
mod registration;
mod server;
mod types;

pub use client::{DEFAULT_MANAGEMENT_API_BASE_URL, MANAGEMENT_API_RESPONSE_LIMIT};
pub use functions::{collect_function_files, require_dir_under};
pub use registration::{configure_server, registration, should_register};
pub use server::{ERR_PROVISIONING_FAILED, permanent_provisioning_err_body};
pub use types::{
    Client, DeployFunctionResult, FunctionDeployMetadata, FunctionFile, MigrationResponse,
    ProvisionStatus, QueryErrorResponse, SupabaseCredentials, SupabaseMCPServer, TypesResponse,
};

#[cfg(test)]
mod client_functions_test;
#[cfg(test)]
mod server_registration_test;
