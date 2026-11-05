//! V0 stdin 与 V1 work response 输入解析。
//!
//! API/Claude 数据由本模块的 wire DTO 表达，并通过 `SessionContextProvider` 隔离网络
//! 获取，使 JSON 解析层不持有具体 HTTP 实现。

pub mod secret;
pub mod types;
pub mod v0_parser;
pub mod v1_parser;

pub use secret::{SecretError, decode_work_secret, parse_v1_work_response};
pub use types::{
    EnvironmentConfigResponse, InputParser, NoSessionContextProvider, Outcomes, ParsedContext,
    SessionContext, SessionContextProvider, SessionContextRequest, V0Parser, V1Parser, V1WorkData,
    V1WorkResponse, WorkSecret,
};
pub use v0_parser::V0ParseError;
pub use v1_parser::{V1ParseError, normalize_api_base_url};

#[cfg(test)]
mod input_test;
