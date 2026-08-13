//! 进程执行结果与错误类型。

use std::{
    io,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use thiserror::Error;

/// 阻止进程启动的错误。
#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("path is empty")]
    EmptyPath,
    #[error("command was cancelled before it started")]
    Cancelled,
    #[error("failed to start {path}: {source}")]
    Start {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// 进程启动后观察到的失败。
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProcessExitError {
    #[error("process exited with code {0}")]
    ExitCode(i32),
    #[error("process failed: {0}")]
    Wait(String),
}

/// 一次进程执行的结果。
#[derive(Debug, Clone)]
pub struct ProcessResult {
    pub exit_code: i32,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub duration: Duration,
    pub error: Option<ProcessExitError>,
}
