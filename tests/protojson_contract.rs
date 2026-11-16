//! Tunnel ProtoJSON 的字段名、oneof、默认省略和 bytes Base64 契约测试。

use environment_manager::r#gen::proto::anthropic::sessions::tunnel::v1alpha::{
    Header, HttpTunnelRequest, TunnelRequest, TunnelResponse, TunnelWindowUpdate, tunnel_request,
    tunnel_response,
};

/// 验证 pbjson 输出使用标准 ProtoJSON lowerCamel 字段名和 Base64 bytes。
#[test]
fn tunnel_request_uses_lower_camel_and_base64_bytes() {
    let request = TunnelRequest {
        session_id: "session-1".to_owned(),
        payload: Some(tunnel_request::Payload::HttpRequest(HttpTunnelRequest {
            request_id: "req-1".to_owned(),
            port: 8080,
            method: "POST".to_owned(),
            path: "/upload".to_owned(),
            headers: vec![Header {
                name: "X-Test".to_owned(),
                value: "ok".to_owned(),
            }],
            body: vec![0xfb, 0xff],
        })),
    };

    assert_eq!(
        serde_json::to_string(&request).unwrap(),
        r#"{"sessionId":"session-1","httpRequest":{"requestId":"req-1","port":8080,"method":"POST","path":"/upload","headers":[{"name":"X-Test","value":"ok"}],"body":"+/8="}}"#
    );
}

/// 验证 ProtoJSON 同时接受 lowerCamel 和 proto 原始 snake_case 字段名。
#[test]
fn tunnel_request_accepts_both_protojson_field_spellings() {
    let lower_camel: TunnelRequest = serde_json::from_str(
        r#"{"sessionId":"session-1","httpRequest":{"requestId":"req-1","body":"b2s="}}"#,
    )
    .unwrap();
    let snake_case: TunnelRequest = serde_json::from_str(
        r#"{"session_id":"session-1","http_request":{"request_id":"req-1","body":"b2s="}}"#,
    )
    .unwrap();

    assert_eq!(lower_camel, snake_case);
    let Some(tunnel_request::Payload::HttpRequest(request)) = lower_camel.payload else {
        panic!("expected http request payload");
    };
    assert_eq!(request.body, b"ok");
}

/// 验证双向 `window_update` 的 ProtoJSON 形状与服务端手写 DTO 的 golden 一致。
#[test]
fn window_update_uses_lower_camel_in_both_directions() {
    let request = TunnelRequest {
        session_id: "session-1".to_owned(),
        payload: Some(tunnel_request::Payload::WindowUpdate(TunnelWindowUpdate {
            stream_id: "req-1".to_owned(),
            bytes: 524_288,
        })),
    };
    assert_eq!(
        serde_json::to_string(&request).unwrap(),
        r#"{"sessionId":"session-1","windowUpdate":{"streamId":"req-1","bytes":524288}}"#
    );
    let response = TunnelResponse {
        payload: Some(tunnel_response::Payload::WindowUpdate(TunnelWindowUpdate {
            stream_id: "ws-1".to_owned(),
            bytes: 524_288,
        })),
    };
    assert_eq!(
        serde_json::to_string(&response).unwrap(),
        r#"{"windowUpdate":{"streamId":"ws-1","bytes":524288}}"#
    );
    let snake: TunnelResponse =
        serde_json::from_str(r#"{"window_update":{"stream_id":"ws-1","bytes":524288}}"#).unwrap();
    assert_eq!(snake, response);
}
