//! 对应 Go `internal/util/tailer_test.go` 的增量日志读取测试。
//!
//! fixture 使用真实临时文件和毫秒轮询，覆盖“启动时跳过旧内容”、半行跨写入、
//! 重复启动以及超长行丢弃；超时只用于防止测试在回归时永久等待。

use std::io::Write;

use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

use super::{DEFAULT_POLL_INTERVAL, MAX_LINE_LENGTH, Tailer, TailerError};

/// 创建带初始内容并立即 flush 的临时日志文件。
fn log_file(initial: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(initial).unwrap();
    file.flush().unwrap();
    file
}

/// 对应 Go：`tailer_test.go` 中的 `TestNewTailerDefaultsAndMissingFile`。
///
/// 验证缺失路径具有 stat 错误，并确认零间隔被替换为恢复出的默认轮询间隔。
#[test]
fn test_new_tailer_defaults_and_missing_file() {
    let missing = std::env::temp_dir().join(format!("missing-tailer-{}", std::process::id()));
    assert!(matches!(
        Tailer::new(missing, std::time::Duration::ZERO),
        Err(TailerError::Stat { .. })
    ));
    let file = log_file(b"old\n");
    let tailer = Tailer::new(file.path(), std::time::Duration::ZERO).unwrap();
    assert_eq!(tailer.poll_interval, DEFAULT_POLL_INTERVAL);
}

/// 对应 Go：`tailer_test.go` 中的 `TestTailerStreamsLinesWrittenAfterStart`。
///
/// 启动后分两次追加内容，确认旧行被跳过且 `partial` 与下一次写入正确拼接。
#[tokio::test]
async fn test_tailer_streams_lines_written_after_start() {
    let mut file = log_file(b"old\n");
    let tailer = Tailer::new(file.path(), std::time::Duration::from_millis(1)).unwrap();
    let mut lines = tailer.take_lines().unwrap();
    tailer.start(CancellationToken::new()).await.unwrap();

    append_to_file(&mut file, b"one\npartial");
    assert_eq!(wait_tail_line(&mut lines).await.text, "one");
    append_to_file(&mut file, b" two\n");
    assert_eq!(wait_tail_line(&mut lines).await.text, "partial two");
    tailer.stop().await;
}

/// 对应 Go：`tailer_test.go` 中的 `appendToFile`。
///
/// 向已打开的临时文件追加并立即 flush，使后台轮询不会观察到用户态缓冲造成的
/// 假延迟；测试结束时文件仍由 `NamedTempFile` RAII 清理。
fn append_to_file(file: &mut NamedTempFile, content: &[u8]) {
    file.write_all(content).unwrap();
    file.flush().unwrap();
}

/// 对应 Go：`tailer_test.go` 中的 `TestTailerStartReturnsErrAlreadyStarted`。
///
/// 同一 Tailer 只允许一个读取任务；第二次启动必须返回可判别的状态错误。
#[tokio::test]
async fn test_tailer_start_returns_err_already_started() {
    let file = log_file(b"");
    let tailer = Tailer::new(file.path(), std::time::Duration::from_millis(1)).unwrap();
    tailer.start(CancellationToken::new()).await.unwrap();
    assert!(matches!(
        tailer.start(CancellationToken::new()).await,
        Err(TailerError::AlreadyStarted)
    ));
    tailer.stop().await;
}

/// 对应 Go：`tailer_test.go` 中的 `TestTailerReadLinesDropsOversizedLines`。
///
/// 写入超过上限的完整行后再写短行，确认实现丢弃整条超长记录并恢复正常读取。
#[tokio::test]
async fn test_tailer_read_lines_drops_oversized_lines() {
    let mut file = log_file(b"");
    let tailer = Tailer::new(file.path(), std::time::Duration::from_millis(1)).unwrap();
    let mut lines = tailer.take_lines().unwrap();
    tailer.start(CancellationToken::new()).await.unwrap();
    file.write_all(&vec![b'x'; MAX_LINE_LENGTH + 1]).unwrap();
    file.write_all(b"\nshort\n").unwrap();
    file.flush().unwrap();
    assert_eq!(wait_tail_line(&mut lines).await.text, "short");
    assert!(lines.try_recv().is_err());
    tailer.stop().await;
}

/// 等待下一条日志行，超过一秒说明轮询任务或 channel 生命周期发生回归。
async fn wait_tail_line(
    lines: &mut tokio::sync::mpsc::Receiver<super::TailLine>,
) -> super::TailLine {
    tokio::time::timeout(std::time::Duration::from_secs(1), lines.recv())
        .await
        .expect("等待 tailer 输出超时")
        .expect("tailer channel 提前关闭")
}
