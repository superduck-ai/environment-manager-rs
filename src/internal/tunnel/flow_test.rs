//! 每流流控、本地 WebSocket 写队列、重连退避与冻结探测测试。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use axum::routing::{any, get};
use futures_util::StreamExt as _;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::client::next_retry_delay;
use super::flow::{
    FLOW_CONTROL_FRAME_OVERHEAD, FLOW_CONTROL_WINDOW, FlowControl, frame_cost,
    response_enables_flow_control,
};
use super::{
    Client, ClientOption, HttpTunnelRequest, TunnelRequest, TunnelResponse, TunnelWindowUpdate,
    WsTunnelMessage, WsTunnelOpen, new_client, tunnel_request, tunnel_response,
};

const LOCAL_CLOSED: &[u8] = b"<local socket closed>";
const TUNNEL_PATH: &str = "/v2/sandbox/tunnel";

async fn start_server(router: Router) -> (i32, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = i32::from(listener.local_addr().unwrap().port());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (port, task)
}

async fn start_tunnel_server(
    advertise_flow_control: bool,
) -> (
    i32,
    mpsc::UnboundedReceiver<HeaderMap>,
    mpsc::UnboundedReceiver<WebSocket>,
    JoinHandle<()>,
) {
    let (header_tx, header_rx) = mpsc::unbounded_channel();
    let (socket_tx, socket_rx) = mpsc::unbounded_channel();
    let router = Router::new().route(
        TUNNEL_PATH,
        any(move |headers: HeaderMap, ws: WebSocketUpgrade| {
            let header_tx = header_tx.clone();
            let socket_tx = socket_tx.clone();
            async move {
                header_tx.send(headers).unwrap();
                let mut response = ws.on_upgrade(move |socket| async move {
                    socket_tx.send(socket).unwrap();
                });
                if advertise_flow_control {
                    response.headers_mut().insert(
                        "x-tunnel-capabilities",
                        HeaderValue::from_static("flow-control-v1"),
                    );
                }
                response
            }
        }),
    );
    let (port, task) = start_server(router).await;
    (port, header_rx, socket_rx, task)
}

async fn connected_client(
    advertise_flow_control: bool,
    options: Vec<ClientOption>,
) -> (Arc<Client>, WebSocket, HeaderMap, JoinHandle<()>) {
    let (port, mut header_rx, mut socket_rx, server) =
        start_tunnel_server(advertise_flow_control).await;
    let client = new_client(
        "session-flow",
        format!("ws://localhost:{port}"),
        "",
        options,
    )
    .unwrap();
    client.connect(&CancellationToken::new()).await.unwrap();
    let headers = timeout(Duration::from_secs(2), header_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let socket = timeout(Duration::from_secs(2), socket_rx.recv())
        .await
        .unwrap()
        .unwrap();
    (client, socket, headers, server)
}

fn run_client(
    client: &Arc<Client>,
) -> (
    CancellationToken,
    JoinHandle<Result<(), super::TunnelError>>,
) {
    let cancellation = CancellationToken::new();
    let running = Arc::clone(client);
    let token = cancellation.clone();
    (
        cancellation,
        tokio::spawn(async move { running.run(&token).await }),
    )
}

async fn send_request(socket: &mut WebSocket, payload: tunnel_request::Payload) {
    let request = TunnelRequest {
        session_id: "session-flow".to_owned(),
        payload: Some(payload),
    };
    socket
        .send(Message::Text(
            serde_json::to_string(&request).unwrap().into(),
        ))
        .await
        .unwrap();
}

async fn next_response(socket: &mut WebSocket, wait: Duration) -> Option<TunnelResponse> {
    loop {
        let message = timeout(wait, socket.next()).await.ok()??.ok()?;
        if let Message::Text(text) = message {
            return Some(serde_json::from_str(&text).unwrap());
        }
    }
}

async fn streaming_target(total: usize) -> (i32, JoinHandle<()>) {
    let router = Router::new().route(
        "/big",
        get(move || async move {
            let chunk = vec![b'b'; 32 * 1024];
            let stream = futures_util::stream::iter(
                (0..total / chunk.len()).map(move |_| Ok::<_, std::io::Error>(chunk.clone())),
            );
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from_stream(stream))
                .unwrap()
        }),
    );
    start_server(router).await
}

fn http_request(request_id: &str, port: i32, path: &str) -> tunnel_request::Payload {
    tunnel_request::Payload::HttpRequest(HttpTunnelRequest {
        request_id: request_id.to_owned(),
        port,
        method: "GET".to_owned(),
        path: path.to_owned(),
        ..Default::default()
    })
}

async fn chunk_bytes_within(socket: &mut WebSocket, request_id: &str, wait: Duration) -> usize {
    let mut total = 0;
    while let Some(response) = next_response(socket, wait).await {
        if let Some(tunnel_response::Payload::HttpChunk(chunk)) = response.payload
            && chunk.request_id == request_id
        {
            total += chunk.data.len();
        }
    }
    total
}

#[tokio::test]
async fn test_flow_control_acquire_fails_when_stream_closed_or_reset() {
    let flow = FlowControl::default();
    flow.reset(true);
    assert!(flow.acquire("unknown", 1).await.is_err());
    flow.open("closed");
    flow.acquire("closed", usize::try_from(FLOW_CONTROL_WINDOW).unwrap())
        .await
        .unwrap();
    let flow = Arc::new(flow);
    let waiting = Arc::clone(&flow);
    let waiter = tokio::spawn(async move { waiting.acquire("closed", 1).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished());
    flow.close("closed");
    assert!(waiter.await.unwrap().is_err());

    flow.open("reset");
    flow.acquire("reset", usize::try_from(FLOW_CONTROL_WINDOW).unwrap())
        .await
        .unwrap();
    let waiting = Arc::clone(&flow);
    let waiter = tokio::spawn(async move { waiting.acquire("reset", 1).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    flow.reset(true);
    assert!(waiter.await.unwrap().is_err());
}

#[tokio::test]
async fn test_flow_control_waits_for_window_update() {
    let flow = Arc::new(FlowControl::default());
    flow.reset(false);
    flow.acquire("legacy", 10 * 1024 * 1024).await.unwrap();
    flow.acquire("legacy", 10 * 1024 * 1024).await.unwrap();

    flow.reset(true);
    flow.open("stream");
    flow.acquire(
        "stream",
        usize::try_from(FLOW_CONTROL_WINDOW - FLOW_CONTROL_FRAME_OVERHEAD - 1).unwrap(),
    )
    .await
    .unwrap();
    flow.acquire("stream", 64 * 1024).await.unwrap();
    let waiting = Arc::clone(&flow);
    let waiter = tokio::spawn(async move { waiting.acquire("stream", 1).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "overdrawn stream must wait");
    flow.grant("stream", 32 * 1024);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "credit still not positive");
    flow.grant("stream", 64 * 1024);
    timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn test_response_capability_header_parsing() {
    let mut headers = http::HeaderMap::new();
    assert!(!response_enables_flow_control(&headers));
    headers.insert(
        "x-tunnel-capabilities",
        http::HeaderValue::from_static("other, flow-control-v1"),
    );
    assert!(response_enables_flow_control(&headers));
    headers.insert(
        "x-tunnel-capabilities",
        http::HeaderValue::from_static("flow-control-v2"),
    );
    assert!(!response_enables_flow_control(&headers));
}

#[test]
fn test_retry_delay_is_capped_at_five_seconds() {
    let mut delay = Duration::from_secs(1);
    let mut seen = Vec::new();
    for _ in 0..6 {
        delay = next_retry_delay(delay);
        seen.push(delay.as_secs());
    }
    assert_eq!(seen, vec![2, 4, 5, 5, 5, 5]);
    assert!(super::add_jitter(delay) < Duration::from_millis(7_500));
}

#[tokio::test]
async fn test_legacy_server_does_not_limit_chunks() {
    let (target_port, target) = streaming_target(3 * 1024 * 1024).await;
    let (client, mut socket, headers, server) = connected_client(false, Vec::new()).await;
    assert_eq!(
        headers.get("x-tunnel-capabilities").unwrap(),
        "flow-control-v1"
    );
    assert!(!client.flow.is_enabled());
    let (cancellation, run) = run_client(&client);
    send_request(&mut socket, http_request("legacy", target_port, "/big")).await;
    let received = chunk_bytes_within(&mut socket, "legacy", Duration::from_millis(800)).await;
    assert_eq!(received, 3 * 1024 * 1024);
    cancellation.cancel();
    let _ = run.await;
    target.abort();
    server.abort();
}

#[tokio::test]
async fn test_http_chunks_stop_at_window_until_window_update() {
    let (target_port, target) = streaming_target(4 * 1024 * 1024).await;
    let (client, mut socket, _headers, server) = connected_client(true, Vec::new()).await;
    assert!(client.flow.is_enabled());
    let (cancellation, run) = run_client(&client);
    send_request(&mut socket, http_request("bounded", target_port, "/big")).await;
    let first = chunk_bytes_within(&mut socket, "bounded", Duration::from_millis(500)).await;
    let window = usize::try_from(FLOW_CONTROL_WINDOW).unwrap();
    assert!(first >= window, "sent only {first} bytes of the window");
    assert!(
        first <= window + 32 * 1024,
        "sent {first} bytes past the window"
    );
    send_request(
        &mut socket,
        tunnel_request::Payload::WindowUpdate(TunnelWindowUpdate {
            stream_id: "bounded".to_owned(),
            bytes: u32::try_from(window).unwrap(),
        }),
    )
    .await;
    let second = chunk_bytes_within(&mut socket, "bounded", Duration::from_millis(500)).await;
    assert!(second >= window - 32 * 1024 && second <= window + 32 * 1024);
    cancellation.cancel();
    let _ = run.await;
    target.abort();
    server.abort();
}

async fn local_websocket_server() -> (i32, mpsc::UnboundedReceiver<Vec<u8>>, JoinHandle<()>) {
    let (received_tx, received_rx) = mpsc::unbounded_channel();
    let router = Router::new().route(
        "/ws",
        any(move |ws: WebSocketUpgrade| {
            let received_tx = received_tx.clone();
            async move {
                ws.on_upgrade(move |mut socket| async move {
                    while let Some(Ok(message)) = socket.next().await {
                        if let Message::Binary(data) = message {
                            let _ = received_tx.send(data.to_vec());
                        }
                    }
                    let _ = received_tx.send(LOCAL_CLOSED.to_vec());
                })
            }
        }),
    );
    let (port, task) = start_server(router).await;
    (port, received_rx, task)
}

#[tokio::test]
async fn test_slow_local_websocket_does_not_block_tunnel_read_loop() {
    let (local_port, mut received, local) = local_websocket_server().await;
    let (target_port, target) = streaming_target(32 * 1024).await;
    let (client, mut socket, _headers, server) = connected_client(true, Vec::new()).await;
    let (cancellation, run) = run_client(&client);
    send_request(
        &mut socket,
        tunnel_request::Payload::WsOpen(WsTunnelOpen {
            tunnel_id: "ws-1".to_owned(),
            port: local_port,
            path: "/ws".to_owned(),
            ..Default::default()
        }),
    )
    .await;
    let opened = next_response(&mut socket, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(matches!(
        opened.payload,
        Some(tunnel_response::Payload::WsOpened(_))
    ));
    let tunnel = client
        .ws_handler
        .as_ref()
        .unwrap()
        .lookup_tunnel("ws-1")
        .await
        .unwrap();
    let blocked_sink = tunnel.sink.lock().await;
    for _ in 0..3 {
        send_request(
            &mut socket,
            tunnel_request::Payload::WsMessage(WsTunnelMessage {
                tunnel_id: "ws-1".to_owned(),
                data: vec![b'w'; 300 * 1024],
                is_binary: true,
            }),
        )
        .await;
    }
    send_request(&mut socket, http_request("during", target_port, "/big")).await;
    let mut saw_headers = false;
    while let Some(response) = next_response(&mut socket, Duration::from_secs(1)).await {
        if let Some(tunnel_response::Payload::HttpHeaders(headers)) = response.payload {
            saw_headers |= headers.request_id == "during";
            break;
        }
    }
    assert!(
        saw_headers,
        "read loop blocked behind a stalled local websocket"
    );
    drop(blocked_sink);
    let mut delivered = 0;
    while delivered < 3 * 300 * 1024 {
        delivered += timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap()
            .len();
    }
    let mut granted = 0_u64;
    while let Some(response) = next_response(&mut socket, Duration::from_millis(500)).await {
        if let Some(tunnel_response::Payload::WindowUpdate(update)) = response.payload {
            assert_eq!(update.stream_id, "ws-1");
            granted += u64::from(update.bytes);
        }
    }
    assert!(granted >= u64::try_from(FLOW_CONTROL_WINDOW / 2).unwrap());
    cancellation.cancel();
    let _ = run.await;
    local.abort();
    target.abort();
    server.abort();
}

fn jumping_clock(offset: Arc<AtomicU64>) -> ClientOption {
    Box::new(move |client: &mut Client| {
        let offset = Arc::clone(&offset);
        client.wall_clock = Arc::new(move || {
            SystemTime::now() + Duration::from_secs(offset.load(Ordering::SeqCst))
        });
        client.freeze_tick = Duration::from_millis(50);
        client.freeze_probe_timeout = Duration::from_millis(300);
    })
}

#[tokio::test]
async fn test_clock_jump_without_inbound_frame_requests_immediate_reconnect() {
    let offset = Arc::new(AtomicU64::new(0));
    let (client, _silent_socket, _headers, server) =
        connected_client(true, vec![jumping_clock(Arc::clone(&offset))]).await;
    let (_cancellation, run) = run_client(&client);
    tokio::time::sleep(Duration::from_millis(150)).await;
    offset.store(60, Ordering::SeqCst);
    let result = timeout(Duration::from_secs(3), run).await.unwrap().unwrap();
    let error = result.unwrap_err().to_string();
    assert!(error.contains("process freeze"), "{error}");
    assert!(client.reconnect_immediately.load(Ordering::SeqCst));
    server.abort();
}

#[tokio::test]
async fn test_clock_jump_with_live_connection_keeps_running() {
    let offset = Arc::new(AtomicU64::new(0));
    let (client, mut socket, _headers, server) =
        connected_client(true, vec![jumping_clock(Arc::clone(&offset))]).await;
    let reader = tokio::spawn(async move { while let Some(Ok(_)) = socket.next().await {} });
    let (cancellation, run) = run_client(&client);
    tokio::time::sleep(Duration::from_millis(150)).await;
    offset.store(60, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        !run.is_finished(),
        "live connection was dropped after a clock jump"
    );
    assert!(!client.reconnect_immediately.load(Ordering::SeqCst));
    cancellation.cancel();
    let _ = run.await;
    reader.abort();
    server.abort();
}

#[test]
fn test_frame_cost_charges_fixed_overhead_for_every_frame() {
    assert_eq!(FLOW_CONTROL_FRAME_OVERHEAD, 64);
    assert_eq!(frame_cost(0), 64);
    assert_eq!(frame_cost(32 * 1024), 32 * 1024 + 64);
}

#[tokio::test]
async fn test_zero_length_frames_consume_credit() {
    let flow = Arc::new(FlowControl::default());
    flow.reset(true);
    flow.open("empty");
    let frames = FLOW_CONTROL_WINDOW / FLOW_CONTROL_FRAME_OVERHEAD;
    for _ in 0..frames {
        flow.acquire("empty", 0).await.unwrap();
    }
    let waiting = Arc::clone(&flow);
    let waiter = tokio::spawn(async move { waiting.acquire("empty", 0).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !waiter.is_finished(),
        "empty frames must exhaust the window"
    );
    flow.grant("empty", 64);
    timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

async fn open_local_websocket(
    write_timeout: Duration,
) -> (
    Arc<Client>,
    WebSocket,
    mpsc::UnboundedReceiver<Vec<u8>>,
    CancellationToken,
    Vec<JoinHandle<()>>,
) {
    let (local_port, received, local) = local_websocket_server().await;
    let option: ClientOption = Box::new(move |client: &mut Client| {
        client.local_ws_write_timeout = write_timeout;
    });
    let (client, mut socket, _headers, server) = connected_client(true, vec![option]).await;
    let (cancellation, run) = run_client(&client);
    tokio::spawn(async move {
        let _ = run.await;
    });
    send_request(
        &mut socket,
        tunnel_request::Payload::WsOpen(WsTunnelOpen {
            tunnel_id: "ws-local".to_owned(),
            port: local_port,
            path: "/ws".to_owned(),
            ..Default::default()
        }),
    )
    .await;
    let opened = next_response(&mut socket, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(matches!(
        opened.payload,
        Some(tunnel_response::Payload::WsOpened(_))
    ));
    (client, socket, received, cancellation, vec![local, server])
}

fn local_message(data: &[u8]) -> tunnel_request::Payload {
    tunnel_request::Payload::WsMessage(WsTunnelMessage {
        tunnel_id: "ws-local".to_owned(),
        data: data.to_vec(),
        is_binary: true,
    })
}

#[tokio::test]
async fn test_local_websocket_write_blocked_beyond_limit_closes_stream() {
    let (client, mut socket, mut received, cancellation, tasks) =
        open_local_websocket(Duration::from_millis(300)).await;
    let tunnel = client
        .ws_handler
        .as_ref()
        .unwrap()
        .lookup_tunnel("ws-local")
        .await
        .unwrap();
    let blocked_sink = tunnel.sink.lock().await;
    send_request(&mut socket, local_message(b"stuck")).await;
    let mut close = None;
    while let Some(response) = next_response(&mut socket, Duration::from_secs(2)).await {
        if let Some(tunnel_response::Payload::WsClose(message)) = response.payload {
            close = Some(message);
            break;
        }
    }
    let close = close.expect("server was not told the stream closed");
    assert_eq!(close.tunnel_id, "ws-local");
    assert_eq!(close.code, 1011);
    assert_eq!(close.reason, "local websocket write timed out");
    drop(blocked_sink);
    drop(tunnel);
    let closed = timeout(Duration::from_secs(2), received.recv())
        .await
        .expect("local websocket stayed open")
        .unwrap();
    assert_eq!(closed, LOCAL_CLOSED);
    cancellation.cancel();
    for task in tasks {
        task.abort();
    }
}

#[tokio::test]
async fn test_local_websocket_write_blocked_within_limit_delivers_in_order() {
    let (client, mut socket, mut received, cancellation, tasks) =
        open_local_websocket(Duration::from_secs(2)).await;
    let tunnel = client
        .ws_handler
        .as_ref()
        .unwrap()
        .lookup_tunnel("ws-local")
        .await
        .unwrap();
    let blocked_sink = tunnel.sink.lock().await;
    for index in 0..5_u8 {
        send_request(&mut socket, local_message(&[index; 16])).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(blocked_sink);
    for index in 0..5_u8 {
        let data = timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data, vec![index; 16]);
    }
    while let Some(response) = next_response(&mut socket, Duration::from_millis(300)).await {
        assert!(
            !matches!(
                response.payload,
                Some(tunnel_response::Payload::WsClose(_) | tunnel_response::Payload::WsError(_))
            ),
            "stream closed although the write recovered within the limit"
        );
    }
    drop(tunnel);
    cancellation.cancel();
    for task in tasks {
        task.abort();
    }
}
