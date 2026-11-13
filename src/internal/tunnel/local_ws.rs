//! 流控模式下本地 WebSocket 的独立写队列。
//!
//! 主 Tunnel 读循环收到 `ws_message` / `ws_close` 后只入队，由每个 tunnel 的独立任务
//! 写入本地 socket，因此本地服务读得慢不会阻塞整条 Tunnel。每写出累计达到窗口一半的
//! 字节就向服务端回一次 `window_update`。

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::flow::{FLOW_CONTROL_WINDOW, frame_cost};
use super::{
    Client, TunnelError, TunnelResponse, TunnelWindowUpdate, WsTunnelClose, WsTunnelMessage,
    tunnel_response,
};

/// 写队列中的一项；关闭帧排在已入队消息之后执行。
pub(crate) enum LocalWsItem {
    /// 写给本地服务的数据消息。
    Message(WsTunnelMessage),
    /// 服务端要求关闭本地连接。
    Close(WsTunnelClose),
}

/// 一个本地 WebSocket 的写队列句柄。
pub(crate) struct LocalWsWriter {
    /// 入队端；丢弃后写任务在排空队列后结束。
    pub(crate) sender: mpsc::UnboundedSender<LocalWsItem>,
    /// 已收到但尚未通过 `window_update` 归还的字节数。
    pub(crate) unacknowledged: Arc<AtomicI64>,
}

impl Client {
    /// 流控模式下把服务端消息放入对应 tunnel 的写队列，不等待本地写入。
    ///
    /// 服务端在额度耗尽后仍发送视为协议错误：只关闭该 tunnel 并回报 `ws_error`。
    pub(crate) fn enqueue_local_ws_message(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
        message: WsTunnelMessage,
    ) {
        let tunnel_id = message.tunnel_id.clone();
        let cost = frame_cost(message.data.len());
        let (sender, unacknowledged) = self.local_ws_writer(cancellation, &tunnel_id);
        if unacknowledged.fetch_add(cost, Ordering::SeqCst) >= FLOW_CONTROL_WINDOW {
            tracing::error!("tunnel_id" = ?(tunnel_id.clone()), "{}", "ws_tunnel_flow_control_violation");
            self.remove_local_ws_writer(&tunnel_id);
            self.flow.close(&tunnel_id);
            let close = WsTunnelClose {
                tunnel_id,
                code: 1008,
                reason: "flow control window exceeded".to_owned(),
            };
            let _ = sender.send(LocalWsItem::Close(close.clone()));
            let client = Arc::clone(self);
            tokio::spawn(async move {
                let _ = client
                    .send_response(TunnelResponse {
                        payload: Some(tunnel_response::Payload::WsClose(close)),
                    })
                    .await;
            });
            return;
        }
        let _ = sender.send(LocalWsItem::Message(message));
    }

    /// 流控模式下把关闭请求排在已入队消息之后；没有写队列时返回 `false`。
    pub(crate) fn enqueue_local_ws_close(&self, message: WsTunnelClose) -> bool {
        let writer = self
            .ws_writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&message.tunnel_id);
        match writer {
            Some(writer) => writer.sender.send(LocalWsItem::Close(message)).is_ok(),
            None => false,
        }
    }

    /// 丢弃 tunnel 的写队列句柄；写任务排空后自然结束。
    pub(crate) fn remove_local_ws_writer(&self, tunnel_id: &str) {
        self.ws_writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(tunnel_id);
    }

    /// 返回已有写队列，或创建队列并启动写任务。
    fn local_ws_writer(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
        tunnel_id: &str,
    ) -> (mpsc::UnboundedSender<LocalWsItem>, Arc<AtomicI64>) {
        let mut writers = self
            .ws_writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(writer) = writers.get(tunnel_id) {
            return (writer.sender.clone(), Arc::clone(&writer.unacknowledged));
        }
        let (sender, receiver) = mpsc::unbounded_channel();
        let unacknowledged = Arc::new(AtomicI64::new(0));
        writers.insert(
            tunnel_id.to_owned(),
            LocalWsWriter {
                sender: sender.clone(),
                unacknowledged: Arc::clone(&unacknowledged),
            },
        );
        drop(writers);
        let client = Arc::clone(self);
        let task_cancellation = cancellation.clone();
        let task_tunnel_id = tunnel_id.to_owned();
        let task_unacknowledged = Arc::clone(&unacknowledged);
        tokio::spawn(async move {
            client
                .run_local_ws_writer(
                    task_cancellation,
                    task_tunnel_id,
                    receiver,
                    task_unacknowledged,
                )
                .await;
        });
        (sender, unacknowledged)
    }

    /// 依次写出队列中的消息与关闭帧，并按写出量回 `window_update`。
    async fn run_local_ws_writer(
        self: Arc<Self>,
        cancellation: CancellationToken,
        tunnel_id: String,
        mut receiver: mpsc::UnboundedReceiver<LocalWsItem>,
        unacknowledged: Arc<AtomicI64>,
    ) {
        let Some(handler) = self.ws_handler.clone() else {
            return;
        };
        let mut written = 0_i64;
        while let Some(item) = receiver.recv().await {
            match item {
                LocalWsItem::Message(message) => {
                    let cost = frame_cost(message.data.len());
                    if let Err(error) = handler
                        .send_message_with_timeout(
                            &cancellation,
                            message,
                            self.local_ws_write_timeout,
                        )
                        .await
                    {
                        tracing::error!("tunnel_id" = ?(tunnel_id.clone()), "error" = ?(error.to_string()), "{}", "ws_tunnel_message_failed");
                        self.report_local_ws_write_failure(&tunnel_id, &error).await;
                        break;
                    }
                    written += cost;
                    if written >= FLOW_CONTROL_WINDOW / 2 {
                        unacknowledged.fetch_sub(written, Ordering::SeqCst);
                        let update = TunnelResponse {
                            payload: Some(tunnel_response::Payload::WindowUpdate(
                                TunnelWindowUpdate {
                                    stream_id: tunnel_id.clone(),
                                    bytes: u32::try_from(written).unwrap_or(u32::MAX),
                                },
                            )),
                        };
                        written = 0;
                        if self.send_response(update).await.is_err() {
                            break;
                        }
                    }
                }
                LocalWsItem::Close(message) => {
                    if let Err(error) = handler.close_tunnel(&cancellation, message).await {
                        tracing::error!("tunnel_id" = ?(tunnel_id.clone()), "error" = ?(error.to_string()), "{}", "ws_tunnel_close_failed");
                    }
                    break;
                }
            }
        }
        self.remove_local_ws_writer(&tunnel_id);
    }

    /// 本地写失败或超时后通知服务端：以 1011 关闭该流，并结束其额度与写队列。
    ///
    /// 写失败时 `WsHandler` 已从 registry 移除并取消本地连接，这里负责让调用方立即
    /// 得到明确的关闭，而不是悬挂到自身超时。
    pub(crate) async fn report_local_ws_write_failure(&self, tunnel_id: &str, error: &TunnelError) {
        self.flow.close(tunnel_id);
        self.remove_local_ws_writer(tunnel_id);
        let detail = error.to_string();
        let reason = if detail.contains("timed out") {
            "local websocket write timed out"
        } else {
            "local websocket write failed"
        };
        let close = TunnelResponse {
            payload: Some(tunnel_response::Payload::WsClose(WsTunnelClose {
                tunnel_id: tunnel_id.to_owned(),
                code: 1011,
                reason: reason.to_owned(),
            })),
        };
        if let Err(error) = self.send_response(close).await {
            tracing::warn!("tunnel_id" = ?(tunnel_id.to_owned()), "error" = ?(error.to_string()), "{}", "ws_tunnel_close_notify_failed");
        }
    }
}
