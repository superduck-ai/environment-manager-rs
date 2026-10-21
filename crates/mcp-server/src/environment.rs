//! 子进程环境快照及覆盖合并。
use async_trait::async_trait;
use serde::Deserialize;
use std::{collections::BTreeMap, io::Read, time::Duration};
use tokio::{io::AsyncReadExt, net::UnixStream, sync::mpsc};
use tokio_util::sync::CancellationToken;

/// 提供调用时的最新运行环境。
#[async_trait]
pub trait EnvironmentProvider: Send + Sync {
    /// 读取环境快照，支持超时和请求取消。
    async fn environ(
        &self,
        cancellation: CancellationToken,
    ) -> Result<BTreeMap<String, String>, String>;
}
/// 通过 Unix socket 接收单个 JSON 环境对象。
pub struct UnixSocketProvider {
    pub path: String,
    pub timeout: Duration,
}
impl UnixSocketProvider {
    /// 默认 socket 操作超时为 500 毫秒。
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            timeout: Duration::from_millis(500),
        }
    }
}
#[async_trait]
impl EnvironmentProvider for UnixSocketProvider {
    /// 分块解码第一个 JSON 值，不要求服务端主动断开连接。
    async fn environ(
        &self,
        cancellation: CancellationToken,
    ) -> Result<BTreeMap<String, String>, String> {
        if self.path.trim().is_empty() {
            return Err("environment socket path is empty".into());
        }
        let dial = tokio::time::timeout(self.timeout, UnixStream::connect(&self.path));
        let mut conn = tokio::select! { _ = cancellation.cancelled() => return Err("connect to environment socket: context canceled".into()), v = dial => v.map_err(|_|format!("connect to environment socket: dial unix {}: i/o timeout",self.path))?.map_err(|e|format!("connect to environment socket: dial unix {}: connect: {}",self.path,crate::util::errno(&e)))? };
        let read = async {
            let (input, receiver) = mpsc::channel(1);
            let mut decoded = tokio::task::spawn_blocking(move || decode_snapshot(receiver));
            let mut received = 0;
            let mut buffer = [0u8; 8192];
            loop {
                let n = tokio::select! {
                    biased;
                    result = &mut decoded => return result.expect("snapshot decoder task"),
                    result = conn.read(&mut buffer) => result.map_err(|e| format!("decode environment snapshot: {e}"))?,
                };
                received += n;
                if received > 1 << 20 {
                    return Err(format!("environment snapshot exceeds {} bytes", 1 << 20));
                }
                if n == 0 {
                    drop(input);
                    return decoded.await.expect("snapshot decoder task");
                }
                if input.send(buffer[..n].to_vec()).await.is_err() {
                    return decoded.await.expect("snapshot decoder task");
                }
            }
        };
        tokio::select! { _ = cancellation.cancelled() => Err("decode environment snapshot: context canceled".into()), v = tokio::time::timeout(self.timeout,read) => v.unwrap_or_else(|_|Err(format!("decode environment snapshot: read unix @->{}: i/o timeout",self.path))) }
    }
}
/// 将异步 socket 分块交给 serde 的同步流式扫描器，保留原字节用于兼容解码。
struct SnapshotReader {
    receiver: mpsc::Receiver<Vec<u8>>,
    data: Vec<u8>,
    position: usize,
}
impl Read for SnapshotReader {
    /// 每个输入字节只扫描一次；通道关闭即 EOF，取消不会遗留阻塞任务。
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.position == self.data.len() {
            let Some(chunk) = self.receiver.blocking_recv() else {
                return Ok(0);
            };
            self.data.extend_from_slice(&chunk);
        }
        let n = buffer.len().min(self.data.len() - self.position);
        buffer[..n].copy_from_slice(&self.data[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}
/// 使用 serde 检查第一个 JSON 值的边界，然后仅做一次兼容字段解码。
fn decode_snapshot(receiver: mpsc::Receiver<Vec<u8>>) -> Result<BTreeMap<String, String>, String> {
    let mut reader = SnapshotReader {
        receiver,
        data: Vec::new(),
        position: 0,
    };
    let mut decoder = serde_json::Deserializer::from_reader(&mut reader);
    let scanned = serde::de::IgnoredAny::deserialize(&mut decoder);
    if scanned.is_ok()
        && reader
            .data
            .iter()
            .find(|b| !b.is_ascii_whitespace())
            .is_some_and(|b| matches!(b, b'n' | b't' | b'f' | b'"'))
        && reader.position == reader.data.len()
    {
        // Go 对顶层字符串/null/bool 还需读取一个后续字节或 EOF 才确定结束。
        let _ = reader.read(&mut [0]);
    }
    crate::json::snapshot(&reader.data, true)
        .map_err(|e| format!("decode environment snapshot: {e}"))?
        .ok_or_else(|| "decode environment snapshot: unexpected EOF".into())
}
/// 将环境数组和覆盖映射合并，并按变量名排序。
pub fn merge(
    base: impl IntoIterator<Item = String>,
    overlays: &[BTreeMap<String, String>],
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for v in base {
        if let Some((k, v)) = v.split_once('=')
            && !k.is_empty()
        {
            out.insert(k.into(), v.into());
        }
    }
    for m in overlays {
        for (k, v) in m {
            if !k.is_empty() {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    out
}
