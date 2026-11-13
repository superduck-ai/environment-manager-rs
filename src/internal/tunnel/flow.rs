//! Tunnel 每流流控。
//!
//! 握手双方都声明 `flow-control-v1` 后启用：每个流（HTTP request ID 或 WebSocket
//! tunnel ID）初始拥有 1 MiB 发送额度，发送 `http_chunk` / `ws_message` 前等待额度大于
//! 零，发送后扣除整帧数据长度（允许透支一帧）。服务端消费后通过 `window_update` 归还
//! 额度。未协商时所有操作立即返回，行为与旧协议一致。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use super::TunnelError;

/// 握手请求与响应共用的能力声明 header。
pub(crate) const CAPABILITIES_HEADER: &str = "x-tunnel-capabilities";
/// 每流流控能力名称。
pub(crate) const FLOW_CONTROL_CAPABILITY: &str = "flow-control-v1";
/// 每个流的初始窗口；双方使用同一常量，不协商大小。
pub const FLOW_CONTROL_WINDOW: i64 = 1 << 20;
/// 每个数据帧（`http_chunk`、`ws_message`）在 data 长度之外固定计入的额度成本。
///
/// 与服务端 `FlowControlFrameOverhead` 一致；零长度帧因此也占额度，单流在途帧数
/// 不超过窗口 / 64。
pub const FLOW_CONTROL_FRAME_OVERHEAD: i64 = 64;

/// 计算一个数据帧的额度成本：data 长度加固定开销。
#[must_use]
pub fn frame_cost(data_len: usize) -> i64 {
    i64::try_from(data_len)
        .unwrap_or(i64::MAX)
        .saturating_add(FLOW_CONTROL_FRAME_OVERHEAD)
}

/// 单个流的发送额度状态。
#[derive(Debug)]
struct CreditState {
    /// 剩余额度；透支时为负。
    available: i64,
    /// 流已结束或连接已重置；等待方应立即返回错误。
    closed: bool,
}

/// 单个流的额度与唤醒通知。
#[derive(Debug)]
struct StreamCredit {
    /// 受互斥锁保护的额度状态，锁不跨 await 持有。
    state: Mutex<CreditState>,
    /// 额度增加或流关闭时唤醒全部等待方。
    notify: Notify,
}

impl StreamCredit {
    /// 创建拥有完整初始窗口的额度。
    fn new() -> Self {
        Self {
            state: Mutex::new(CreditState {
                available: FLOW_CONTROL_WINDOW,
                closed: false,
            }),
            notify: Notify::new(),
        }
    }

    /// 标记关闭并唤醒等待方。
    fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed = true;
        self.notify.notify_waiters();
    }
}

/// 一条 Tunnel 连接上全部流的发送额度。
#[derive(Debug, Default)]
pub(crate) struct FlowControl {
    /// 当前连接是否协商了流控。
    enabled: AtomicBool,
    /// 流 ID 到额度的映射。
    streams: Mutex<HashMap<String, Arc<StreamCredit>>>,
}

impl FlowControl {
    /// 返回当前连接是否启用了流控。
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// 切换到新连接：关闭并清空旧连接的全部额度，唤醒仍在等待的发送方。
    pub(crate) fn reset(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
        let drained = std::mem::take(&mut *self.lock_streams());
        for credit in drained.into_values() {
            credit.close();
        }
    }

    /// 为新流登记完整初始窗口；未启用流控时不登记。
    pub(crate) fn open(&self, stream_id: &str) {
        if !self.is_enabled() {
            return;
        }
        self.lock_streams()
            .insert(stream_id.to_owned(), Arc::new(StreamCredit::new()));
    }

    /// 结束一个流并唤醒等待该流额度的发送方。
    pub(crate) fn close(&self, stream_id: &str) {
        if let Some(credit) = self.lock_streams().remove(stream_id) {
            credit.close();
        }
    }

    /// 处理服务端 `window_update`，未知流忽略。
    pub(crate) fn grant(&self, stream_id: &str, bytes: u32) {
        let credit = self.lock_streams().get(stream_id).cloned();
        if let Some(credit) = credit {
            credit
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .available += i64::from(bytes);
            credit.notify.notify_waiters();
        }
    }

    /// 等待额度大于零后扣除一个 data 长度为 `data_len` 的数据帧的成本。
    ///
    /// 成本为 `data_len + 64`，零长度帧（包括 HTTP 结束块）也要占额度。未启用流控时
    /// 立即返回。流被关闭、连接被重置或流未登记时返回错误，调用方据此结束该流的发送。
    pub(crate) async fn acquire(
        &self,
        stream_id: &str,
        data_len: usize,
    ) -> Result<(), TunnelError> {
        if !self.is_enabled() {
            return Ok(());
        }
        let Some(credit) = self.lock_streams().get(stream_id).cloned() else {
            return Err(TunnelError::message(format!(
                "flow control stream closed: {stream_id}"
            )));
        };
        let cost = frame_cost(data_len);
        loop {
            let notified = credit.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = credit
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if state.closed {
                    return Err(TunnelError::message(format!(
                        "flow control stream closed: {stream_id}"
                    )));
                }
                if state.available > 0 {
                    state.available -= cost;
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// 获取映射锁；中毒时继续使用内部数据。
    fn lock_streams(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<StreamCredit>>> {
        self.streams
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// 判断握手响应是否声明了流控能力。
pub(crate) fn response_enables_flow_control(headers: &http::HeaderMap) -> bool {
    headers
        .get_all(CAPABILITIES_HEADER)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|capability| capability.trim() == FLOW_CONTROL_CAPABILITY)
}
