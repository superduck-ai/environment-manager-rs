//! JSON text fallback for Bash execution results.
use crate::execution::ExecutionResult;
pub(crate) fn encode_execution(value: &ExecutionResult) -> Vec<u8> {
    serde_json::to_string(value)
        .expect("execution result")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
        .into_bytes()
}
