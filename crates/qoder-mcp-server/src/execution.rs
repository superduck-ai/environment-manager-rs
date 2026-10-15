//! Bash execution result shared by structured content and its JSON text fallback.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    #[default]
    NotStarted,
    Running,
    Completed,
    TimedOut,
    Cancelled,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub status: ExecutionStatus,
    pub duration_ms: u64,
    pub timed_out: bool,
    pub cancelled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub signal: String,
    pub stdout_bytes: usize,
    pub stderr_bytes: usize,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stdout_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stderr_file: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

pub fn output_schema() -> Value {
    serde_json::from_str(include_str!("execution_schema.json")).expect("execution schema")
}
