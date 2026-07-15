package tunnelv1alpha

// 本文件通过 overlay 临时加入 Go 生成包，直接调用官方 protojson。它只读生成类型，
// 不修改 `tunnel.pb.go`，也不需要运行 protoc 或访问 staging。

import (
	"encoding/json"
	"os"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// protocolProtoJSONCase 保存一条实际 wire JSON。Wire 是原始字符串而不是解析后的
// object，因此 lowerCamel 字段名、oneof 键、字段顺序、默认省略和 Base64 字节都会
// 被逐字节比较。
type protocolProtoJSONCase struct {
	Name string `json:"name"`
	Wire string `json:"wire"`
}

// protocolProtoJSONTranscript 是 Tunnel ProtoJSON probe 的稳定输出格式。
type protocolProtoJSONTranscript struct {
	SchemaVersion int                     `json:"schema_version"`
	Protocol      string                  `json:"protocol"`
	Cases         []protocolProtoJSONCase `json:"cases"`
}

// TestProtocolDifferentialTranscript 覆盖 HTTP 请求/响应以及 WebSocket 的 open、
// message、close、error payload。用包含 0xfb/0xff 的 bytes 强制验证标准 padded Base64，
// 并用 false/空值验证 proto3 默认字段确实被省略。
func TestProtocolDifferentialTranscript(t *testing.T) {
	cases := []struct {
		name    string
		message proto.Message
	}{
		{
			name: "http_request",
			message: &TunnelRequest{
				SessionId: "session-1",
				Payload: &TunnelRequest_HttpRequest{HttpRequest: &HTTPTunnelRequest{
					RequestId: "request-1",
					Port:      8080,
					Method:    "POST",
					Path:      "/upload?mode=raw",
					Headers: []*Header{
						{Name: "X-First", Value: "one"},
						{Name: "X-First", Value: "two"},
					},
					Body: []byte{0xfb, 0xff},
				}},
			},
		},
		{
			name: "http_cancel",
			message: &TunnelRequest{
				SessionId: "session-1",
				Payload:   &TunnelRequest_HttpCancel{HttpCancel: &HTTPTunnelCancel{RequestId: "request-1"}},
			},
		},
		{
			name: "ws_open",
			message: &TunnelRequest{
				SessionId: "session-1",
				Payload: &TunnelRequest_WsOpen{WsOpen: &WSTunnelOpen{
					TunnelId: "tunnel-1",
					Port:     9090,
					Path:     "/socket",
					Headers:  []*Header{{Name: "Origin", Value: "https://example.test"}},
					Protocols: []string{"chat.v2", "chat.v1"},
				}},
			},
		},
		{
			name: "ws_binary_message",
			message: &TunnelRequest{
				SessionId: "session-1",
				Payload: &TunnelRequest_WsMessage{WsMessage: &WSTunnelMessage{
					TunnelId: "tunnel-1",
					Data:     []byte{0x00, 0xfb, 0xff},
					IsBinary: true,
				}},
			},
		},
		{
			name: "ws_text_message_default_bool",
			message: &TunnelRequest{
				SessionId: "session-1",
				Payload: &TunnelRequest_WsMessage{WsMessage: &WSTunnelMessage{
					TunnelId: "tunnel-1",
					Data:     []byte("hello"),
					IsBinary: false,
				}},
			},
		},
		{
			name: "ws_close",
			message: &TunnelRequest{
				SessionId: "session-1",
				Payload: &TunnelRequest_WsClose{WsClose: &WSTunnelClose{
					TunnelId: "tunnel-1",
					Code:     1000,
					Reason:   "done",
				}},
			},
		},
		{
			name: "http_response_headers",
			message: &TunnelResponse{Payload: &TunnelResponse_HttpHeaders{HttpHeaders: &HTTPTunnelResponseHeaders{
				RequestId:      "request-1",
				StatusCode:     201,
				StatusText:     "201 Created",
				Headers:        []*Header{{Name: "Content-Type", Value: "application/json"}},
				SequenceNumber: 7,
			}}},
		},
		{
			name: "http_response_chunk",
			message: &TunnelResponse{Payload: &TunnelResponse_HttpChunk{HttpChunk: &HTTPTunnelResponseChunk{
				RequestId:      "request-1",
				Data:           []byte{0xfb, 0xff},
				IsFinal:        true,
				SequenceNumber: 8,
			}}},
		},
		{
			name: "ws_response_error",
			message: &TunnelResponse{Payload: &TunnelResponse_WsError{WsError: &WSTunnelError{
				TunnelId:    "tunnel-1",
				ErrorMessage: "dial failed",
			}}},
		},
	}

	marshalOptions := protojson.MarshalOptions{UseProtoNames: false, EmitUnpopulated: false}
	transcriptCases := make([]protocolProtoJSONCase, 0, len(cases))
	for _, item := range cases {
		wire, err := marshalOptions.Marshal(item.message)
		if err != nil {
			t.Fatalf("%s: marshal ProtoJSON: %v", item.name, err)
		}
		transcriptCases = append(transcriptCases, protocolProtoJSONCase{Name: item.name, Wire: string(wire)})
	}

	writeProtoJSONProtocolTranscript(t, protocolProtoJSONTranscript{
		SchemaVersion: 1,
		Protocol:      "tunnel_protojson",
		Cases:         transcriptCases,
	})
}

// writeProtoJSONProtocolTranscript 只写测试框架提供的临时输出文件；缺少路径时立即
// 失败，避免开发者单独运行 probe 时在只读 Go package 旁留下产物。
func writeProtoJSONProtocolTranscript(t *testing.T, transcript protocolProtoJSONTranscript) {
	t.Helper()
	path := os.Getenv("PROTOCOL_TRANSCRIPT_OUTPUT")
	if path == "" {
		t.Fatal("PROTOCOL_TRANSCRIPT_OUTPUT is required")
	}
	encoded, err := json.Marshal(transcript)
	if err != nil {
		t.Fatalf("marshal ProtoJSON transcript: %v", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write ProtoJSON transcript: %v", err)
	}
}
