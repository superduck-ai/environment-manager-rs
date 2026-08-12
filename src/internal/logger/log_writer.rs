//! 把任意字节写入转换为逐行结构化日志。
//!
//! 该组件可直接交给要求 `std::io::Write` 的库，也可包在 process streamer 中。
//! 半行跨 write 保留，完整行去掉 `\n` 后记录；超过 1 MiB 的无换行内容会整段
//! 附加截断标记后发送并清空，保持恢复 Go 实现的边界条件。

use std::io::{self, Write};

use tracing::Level;

/// 无换行单行允许保留的最大字节数，与 Go 实现一致为 1 MiB。
pub const MAX_LOG_WRITER_BUFFERED_LINE: usize = 1 << 20;

/// 面向字节流的逐行日志 writer。
pub struct LogWriter {
    level: Level,
    prefix: String,
    buffer: Vec<u8>,
}

impl LogWriter {
    /// 创建逐行 writer；prefix 会添加到每一条完整或截断消息前。
    pub fn new(level: Level, prefix: impl Into<String>) -> Self {
        Self {
            level,
            prefix: prefix.into(),
            buffer: Vec::new(),
        }
    }

    /// 记录一行字节，并按构造参数附加 prefix、属性和可选截断标记。
    ///
    /// Rust 字符串必须是合法 UTF-8，因此非法字节在进入结构化记录前使用替换
    /// 字符；这与最终 JSON 编码时的容错结果一致。caller 会继续传到外层
    /// `Write::write`，使 source 对应 Go 的 `LogWriter.Write` 而不是内部 helper。
    #[track_caller]
    fn emit(&self, bytes: &[u8], truncated: bool) {
        let mut message = String::with_capacity(self.prefix.len() + bytes.len() + 32);
        message.push_str(&self.prefix);
        message.push_str(&String::from_utf8_lossy(bytes));
        if truncated {
            message.push_str(" [line truncated - no newline]");
        }
        match self.level {
            Level::TRACE => tracing::trace!(%message),
            Level::DEBUG => tracing::debug!(%message),
            Level::INFO => tracing::info!(%message),
            Level::WARN => tracing::warn!(%message),
            Level::ERROR => tracing::error!(%message),
        }
    }

    /// 返回测试可见的半行缓冲长度，用于验证超长行发送后已经清空状态。
    #[cfg(test)]
    pub(super) fn buffered_len(&self) -> usize {
        self.buffer.len()
    }
}

impl Write for LogWriter {
    /// 缓冲输入并为每个 `\n` 结尾的完整行发送一条日志。
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        while let Some(line_end) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let mut rest = self.buffer.split_off(line_end + 1);
            self.emit(&self.buffer[..line_end], false);
            std::mem::swap(&mut self.buffer, &mut rest);
        }
        if self.buffer.len() > MAX_LOG_WRITER_BUFFERED_LINE {
            // Go 代码虽称其为 truncated line，却会保留并发送触发阈值的全部字节；
            // 这里不能只保留前 1 MiB，否则诊断内容和消息长度都会发生变化。
            self.emit(&self.buffer, true);
            self.buffer.clear();
        }
        Ok(bytes.len())
    }

    /// 不发送尚未换行的半行；完整行已在 `write` 时同步交给 handler。
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
