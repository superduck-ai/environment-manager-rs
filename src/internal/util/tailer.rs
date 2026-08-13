//! 轮询式增量文件跟随器。
//!
//! 启动时先 seek 到文件末尾，只发送之后追加的完整行。单行最大 2048 字节，
//! 超长行会被整行丢弃，防止 Claude 等外部进程输出无换行大块数据耗尽内存。

use std::{
    path::{Path, PathBuf},
    sync::{
        Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const READ_BUFFER_SIZE: usize = 4096;
const MAX_LINE_LENGTH: usize = 2048;
const LINE_CHANNEL_CAPACITY: usize = 100;

/// 文件跟随器的构造或启动错误。
#[derive(Debug, Error)]
pub enum TailerError {
    /// 构造时无法读取目标元数据。
    #[error("failed to stat file {path}: {source}")]
    Stat {
        /// 目标路径。
        path: PathBuf,
        /// 原始文件系统错误。
        #[source]
        source: std::io::Error,
    },
    /// 启动时无法打开文件。
    #[error("failed to open file {path}: {source}")]
    Open {
        /// 目标路径。
        path: PathBuf,
        /// 原始文件系统错误。
        #[source]
        source: std::io::Error,
    },
    /// 无法定位到启动时的文件末尾。
    #[error("failed to seek to end of file {path}: {source}")]
    Seek {
        /// 目标路径。
        path: PathBuf,
        /// 原始文件系统错误。
        #[source]
        source: std::io::Error,
    },
    /// 同一个实例只允许成功启动一次。
    #[error("tailer already started")]
    AlreadyStarted,
    /// 接收端只能被取走一次。
    #[error("tailer line receiver already taken")]
    ReceiverAlreadyTaken,
}

/// 一条完整文本行或一个终止读取错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailLine {
    /// 不包含换行符的文本；错误消息的该字段为空。
    pub text: String,
    /// 读取失败时的错误文本；正常行时为 `None`。
    pub error: Option<String>,
}

impl TailLine {
    /// 创建正常文本行。
    fn text(bytes: &[u8]) -> Self {
        Self {
            text: String::from_utf8_lossy(bytes).into_owned(),
            error: None,
        }
    }

    /// 创建终止读取错误消息。
    fn error(error: &std::io::Error) -> Self {
        Self {
            text: String::new(),
            error: Some(error.to_string()),
        }
    }
}

/// 可取消、带有界输出队列的文件跟随器。
pub struct Tailer {
    path: PathBuf,
    poll_interval: Duration,
    started: AtomicBool,
    cancellation: CancellationToken,
    sender: StdMutex<Option<mpsc::Sender<TailLine>>>,
    receiver: StdMutex<Option<mpsc::Receiver<TailLine>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Tailer {
    /// 校验文件存在并创建跟随器，但尚不打开或启动后台任务。
    pub fn new(path: impl AsRef<Path>, poll_interval: Duration) -> Result<Self, TailerError> {
        let path = path.as_ref().to_owned();
        std::fs::metadata(&path).map_err(|source| TailerError::Stat {
            path: path.clone(),
            source,
        })?;
        let poll_interval = if poll_interval.is_zero() {
            DEFAULT_POLL_INTERVAL
        } else {
            poll_interval
        };
        let (sender, receiver) = mpsc::channel(LINE_CHANNEL_CAPACITY);
        Ok(Self {
            path,
            poll_interval,
            started: AtomicBool::new(false),
            cancellation: CancellationToken::new(),
            sender: StdMutex::new(Some(sender)),
            receiver: StdMutex::new(Some(receiver)),
            task: Mutex::new(None),
        })
    }

    /// 取出行接收端。
    ///
    /// Tokio `mpsc::Receiver` 是单消费者，与 channel 使用方式一致；
    /// 因此该方法只能成功一次。
    pub fn take_lines(&self) -> Result<mpsc::Receiver<TailLine>, TailerError> {
        self.receiver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .ok_or(TailerError::ReceiverAlreadyTaken)
    }

    ///
    /// Tokio channel 明确要求单一消费者，所以第一次调用转移 receiver 所有权；
    /// 后续调用返回 [`TailerError::ReceiverAlreadyTaken`]，而不是复制消费端。
    pub fn lines(&self) -> Result<mpsc::Receiver<TailLine>, TailerError> {
        self.take_lines()
    }

    /// 从当前文件末尾开始后台轮询。
    ///
    /// `parent_cancellation` 允许调用方把跟随器纳入整个 session 的关闭树；
    /// 本实例自己的 `stop` token 同样会立即唤醒任务。
    pub async fn start(&self, parent_cancellation: CancellationToken) -> Result<(), TailerError> {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(TailerError::AlreadyStarted);
        }

        let mut file = match File::open(&self.path).await {
            Ok(file) => file,
            Err(source) => {
                self.started.store(false, Ordering::Release);
                return Err(TailerError::Open {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        if let Err(source) = file.seek(SeekFrom::End(0)).await {
            self.started.store(false, Ordering::Release);
            return Err(TailerError::Seek {
                path: self.path.clone(),
                source,
            });
        }

        let sender = self
            .sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("sender exists until the first successful start");
        let cancellation = self.cancellation.clone();
        let poll_interval = self.poll_interval;
        let task = tokio::spawn(async move {
            Tailer::run(
                &mut file,
                sender,
                poll_interval,
                cancellation,
                parent_cancellation,
            )
            .await;
        });
        *self.task.lock().await = Some(task);
        Ok(())
    }

    /// 请求停止，并等待后台文件句柄与 channel sender 被释放。
    pub async fn stop(&self) {
        self.cancellation.cancel();
        // 未启动时主动丢弃 sender，使等待中的接收端观察到 channel 关闭。
        if !self.started.load(Ordering::Acquire) {
            self.sender
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
        }
        if let Some(task) = self.task.lock().await.take()
            && let Err(error) = task.await
            && error.is_panic()
        {
            // tailer 后台任务 若 panic 会终止整个进程；Tokio 会把 panic
            // 收进 JoinError，因此必须在会合点重新抛出，不能额外写 Rust-only 日志。
            std::panic::resume_unwind(error.into_panic());
        }
    }
    /// 运行首次读取和后续轮询，直到任一取消 token 或读取错误触发。
    async fn run(
        file: &mut File,
        sender: mpsc::Sender<TailLine>,
        poll_interval: Duration,
        cancellation: CancellationToken,
        parent_cancellation: CancellationToken,
    ) {
        let mut buffer = Vec::new();
        let mut discarding = false;
        if !Self::read_lines(
            file,
            &sender,
            &mut buffer,
            &mut discarding,
            &cancellation,
            &parent_cancellation,
        )
        .await
        {
            return;
        }

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = parent_cancellation.cancelled() => break,
                _ = tokio::time::sleep(poll_interval) => {
                    if !Self::read_lines(
                        file,
                        &sender,
                        &mut buffer,
                        &mut discarding,
                        &cancellation,
                        &parent_cancellation,
                    ).await {
                        break;
                    }
                }
            }
        }
    }

    /// 读取当前可用字节，拆分完整行并保留末尾半行供下一次轮询拼接。
    async fn read_lines(
        file: &mut File,
        sender: &mpsc::Sender<TailLine>,
        buffer: &mut Vec<u8>,
        discarding: &mut bool,
        cancellation: &CancellationToken,
        parent_cancellation: &CancellationToken,
    ) -> bool {
        let mut chunk = [0_u8; READ_BUFFER_SIZE];
        loop {
            match file.read(&mut chunk).await {
                Ok(0) => return true,
                Ok(read) => {
                    buffer.extend_from_slice(&chunk[..read]);
                    if !drain_complete_lines(
                        sender,
                        buffer,
                        discarding,
                        cancellation,
                        parent_cancellation,
                    )
                    .await
                    {
                        return false;
                    }
                }
                Err(error) => {
                    let _ = Self::send_line(
                        sender,
                        TailLine::error(&error),
                        cancellation,
                        parent_cancellation,
                    )
                    .await;
                    return false;
                }
            }
        }
    }

    /// 在发送端背压、接收端关闭和取消之间进行选择。
    async fn send_line(
        sender: &mpsc::Sender<TailLine>,
        line: TailLine,
        cancellation: &CancellationToken,
        parent_cancellation: &CancellationToken,
    ) -> bool {
        tokio::select! {
            _ = cancellation.cancelled() => false,
            _ = parent_cancellation.cancelled() => false,
            result = sender.send(line) => result.is_ok(),
        }
    }
}

///
/// 保留自由函数命名，并把路径所有权和零间隔默认规则委托给
/// [`Tailer::new`]；不会在构造阶段打开长期文件描述符或启动任务。
pub fn new_tailer(path: impl AsRef<Path>, poll_interval: Duration) -> Result<Tailer, TailerError> {
    Tailer::new(path, poll_interval)
}

/// 从缓冲区发送所有完整且未超长的行，并限制未换行缓冲的最大尺寸。
async fn drain_complete_lines(
    sender: &mpsc::Sender<TailLine>,
    buffer: &mut Vec<u8>,
    discarding: &mut bool,
    cancellation: &CancellationToken,
    parent_cancellation: &CancellationToken,
) -> bool {
    while let Some(line_end) = buffer.iter().position(|byte| *byte == b'\n') {
        let mut rest = buffer.split_off(line_end + 1);
        let line = &buffer[..line_end];
        if *discarding {
            *discarding = false;
        } else if line.len() <= MAX_LINE_LENGTH
            && !Tailer::send_line(
                sender,
                TailLine::text(line),
                cancellation,
                parent_cancellation,
            )
            .await
        {
            return false;
        }
        std::mem::swap(buffer, &mut rest);
    }

    if buffer.len() > MAX_LINE_LENGTH {
        *discarding = true;
        buffer.clear();
    }
    true
}

#[cfg(test)]
#[path = "tailer_test.rs"]
mod tailer_test;
