//! V0 stdin 与 V1 work response 输入解析。
//!
//! 本模块机械对应 Go 的 `internal/input` 包。跨目录的 API/Claude 类型暂由本模块
//! wire DTO 表达，并通过 `SessionContextProvider` 隔离网络获取，后续可无损接到
//! Tokio/Reqwest API client，而不会让 JSON 解析层持有具体 HTTP 实现。

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
