//! Stream capture and execution snapshots for Bash.
use super::tail;
use crate::{
    ToolResult,
    execution::{ExecutionResult, ExecutionStatus},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

pub(super) fn result(
    text: impl Into<String>,
    is_error: bool,
    execution: ExecutionResult,
) -> ToolResult {
    let mut result = ToolResult::text(text);
    result.is_error = is_error;
    result.content.push(
        ToolResult::text(
            String::from_utf8(crate::wire_json::encode_execution(&execution)).expect("JSON UTF-8"),
        )
        .content
        .remove(0),
    );
    result.structured_content = Some(Box::new(execution));
    result
}

pub(crate) fn not_started(message: impl Into<String>) -> ToolResult {
    let message = message.into();
    result(
        message.clone(),
        true,
        ExecutionResult {
            error: message,
            ..Default::default()
        },
    )
}

#[derive(Clone)]
pub(super) struct Streams {
    pub stdout: PathBuf,
    pub stderr: PathBuf,
}
impl Streams {
    pub fn new(output: &Path) -> Self {
        Self {
            stdout: PathBuf::from(format!("{}.stdout", output.display())),
            stderr: PathBuf::from(format!("{}.stderr", output.display())),
        }
    }
    pub async fn open(&self) -> std::io::Result<(fs::File, fs::File)> {
        let stdout = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&self.stdout)
            .await?;
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&self.stderr)
            .await
        {
            Ok(stderr) => Ok((stdout, stderr)),
            Err(error) => {
                let _ = fs::remove_file(&self.stdout).await;
                Err(error)
            }
        }
    }
    pub async fn remove(&self) {
        let _ = fs::remove_file(&self.stdout).await;
        let _ = fs::remove_file(&self.stderr).await;
    }
    pub async fn snapshot(&self, execution: &mut ExecutionResult, max: usize) {
        let running = execution.status == ExecutionStatus::Running;
        for (name, path) in [("stdout", &self.stdout), ("stderr", &self.stderr)] {
            let (text, bytes, truncated, retained, error) = match tail(path, max).await {
                Ok((text, bytes)) => {
                    let truncated = bytes > max;
                    let retained = if running || truncated {
                        path.to_string_lossy().into_owned()
                    } else {
                        let _ = fs::remove_file(path).await;
                        String::new()
                    };
                    (text, bytes, truncated, retained, String::new())
                }
                Err(error) => (
                    String::new(),
                    0,
                    false,
                    path.to_string_lossy().into_owned(),
                    format!("failed to read {name}: {error}"),
                ),
            };
            if name == "stdout" {
                execution.stdout = text;
                execution.stdout_bytes = bytes;
                execution.stdout_truncated = truncated;
                execution.stdout_file = retained;
            } else {
                execution.stderr = text;
                execution.stderr_bytes = bytes;
                execution.stderr_truncated = truncated;
                execution.stderr_file = retained;
            }
            if !error.is_empty() {
                if !execution.error.is_empty() {
                    execution.error.push_str("; ");
                }
                execution.error.push_str(&error);
            }
        }
    }
}

pub(super) async fn capture(
    mut pipe: impl AsyncRead + Unpin,
    mut stream: fs::File,
    combined: Arc<Mutex<fs::File>>,
) -> std::io::Result<()> {
    let mut buffer = [0; 8192];
    loop {
        let n = pipe.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        stream.write_all(&buffer[..n]).await?;
        stream.flush().await?;
        let mut combined = combined.lock().await;
        combined.write_all(&buffer[..n]).await?;
        combined.flush().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unreadable_stream_is_reported_and_retains_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let streams = Streams::new(&dir.path().join("output"));
        fs::write(&streams.stderr, "warning").await.unwrap();
        let mut execution = ExecutionResult {
            status: ExecutionStatus::Completed,
            ..Default::default()
        };
        streams.snapshot(&mut execution, 30000).await;
        assert!(execution.error.contains("failed to read stdout:"));
        assert_eq!(execution.stdout_file, streams.stdout.to_string_lossy());
        assert_eq!(execution.stderr, "warning");
        assert_eq!(execution.stderr_bytes, 7);
        assert!(execution.stderr_file.is_empty());
        assert!(!streams.stderr.exists());
        assert!(result("capture failed", !execution.error.is_empty(), execution).is_error);
    }
}
