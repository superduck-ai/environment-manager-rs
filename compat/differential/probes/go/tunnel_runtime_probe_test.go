package tunnel

// 本文件通过 overlay 临时加入 Go `internal/tunnel` 包，调用真实 Handler 与 WSHandler。
// 所有服务都只监听 loopback；随机端口只用于连接，transcript 只记录端口无关的契约。

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// protocolTunnelHTTPRequest 保存 mock 实际收到的稳定请求字段。Host 不写随机端口，
// 而是验证它是否严格等于 Handler 构造的本次 `localhost:<port>` authority。
type protocolTunnelHTTPRequest struct {
	Observed                  bool   `json:"observed"`
	Method                    string `json:"method"`
	Target                    string `json:"target"`
	HostMatchesLoopbackTarget bool   `json:"host_matches_loopback_target"`
	UserAgent                 string `json:"user_agent"`
	InputHeader               string `json:"input_header"`
	Body                      string `json:"body"`
}

// protocolTunnelHTTPEvent 是 Handler 经 ResponseSender 发出的单个协议事件。所有字段
// 固定存在，使空值、sequence zero、final false 等默认行为也参与严格比较。
type protocolTunnelHTTPEvent struct {
	Kind           string `json:"kind"`
	RequestID      string `json:"request_id"`
	StatusCode     int32  `json:"status_code"`
	StatusText     string `json:"status_text"`
	ReplyHeader    string `json:"reply_header"`
	SequenceNumber uint32 `json:"sequence_number"`
	DataBase64     string `json:"data_base64"`
	IsFinal        bool   `json:"is_final"`
	ErrorMessage   string `json:"error_message"`
	ErrorClass     string `json:"error_class"`
}

// protocolTunnelHTTPCase 保存一次真实本地 HTTP 转发的输入观察、输出事件和函数错误。
type protocolTunnelHTTPCase struct {
	Name         string                    `json:"name"`
	Request      protocolTunnelHTTPRequest `json:"request"`
	Events       []protocolTunnelHTTPEvent `json:"events"`
	HandlerError string                    `json:"handler_error"`
}

// protocolTunnelHTTPTranscript 是 HTTP Tunnel runtime 的稳定输出文档。
type protocolTunnelHTTPTranscript struct {
	SchemaVersion int                      `json:"schema_version"`
	Protocol      string                   `json:"protocol"`
	Cases         []protocolTunnelHTTPCase `json:"cases"`
}

// protocolTunnelHandshake 保存本地 WebSocket 服务实际收到的握手字段。
type protocolTunnelHandshake struct {
	Observed         bool   `json:"observed"`
	Host             string `json:"host"`
	TunnelHeader     string `json:"tunnel_header"`
	OfferedProtocols string `json:"offered_protocols"`
}

// protocolTunnelWSWireMessage 保存 mock 看到的客户端 frame，二进制统一使用标准 Base64。
type protocolTunnelWSWireMessage struct {
	Kind       string `json:"kind"`
	DataBase64 string `json:"data_base64"`
}

// protocolTunnelWSClose 保存 mock 看到的客户端 close frame。
type protocolTunnelWSClose struct {
	Observed bool   `json:"observed"`
	Code     int    `json:"code"`
	Reason   string `json:"reason"`
}

// protocolTunnelWSEvent 是 WSHandler 通过 WSResponseSender 发出的稳定协议事件。
type protocolTunnelWSEvent struct {
	Kind         string `json:"kind"`
	TunnelID     string `json:"tunnel_id"`
	Protocol     string `json:"protocol"`
	DataBase64   string `json:"data_base64"`
	IsBinary     bool   `json:"is_binary"`
	Code         int32  `json:"code"`
	Reason       string `json:"reason"`
	ErrorMessage string `json:"error_message"`
}

// protocolTunnelWSCase 保存一次真实本地 WebSocket 生命周期。
type protocolTunnelWSCase struct {
	Name           string                        `json:"name"`
	Handshake      protocolTunnelHandshake       `json:"handshake"`
	ClientMessages []protocolTunnelWSWireMessage `json:"client_messages"`
	ClientClose    protocolTunnelWSClose         `json:"client_close"`
	Events         []protocolTunnelWSEvent       `json:"events"`
	HandlerError   string                        `json:"handler_error"`
}

// protocolTunnelWSTranscript 是本地 WebSocket Tunnel runtime 的稳定输出文档。
type protocolTunnelWSTranscript struct {
	SchemaVersion int                    `json:"schema_version"`
	Protocol      string                 `json:"protocol"`
	Cases         []protocolTunnelWSCase `json:"cases"`
}

// protocolRuntimeHTTPSender 记录 Handler 事件，并可在数据 chunk 到达时通知分段 mock。
// 通知用于强制服务端在发送下一次 flush 前等待客户端消费，避免 TCP 合并掩盖边界。
type protocolRuntimeHTTPSender struct {
	mu       sync.Mutex
	events   []protocolTunnelHTTPEvent
	chunkAck chan<- struct{}
}

// SendHeaders 记录状态、sequence 和受控响应头。
func (s *protocolRuntimeHTTPSender) SendHeaders(value interface{}) error {
	response := value.(*HTTPTunnelResponseHeaders)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, protocolTunnelHTTPEvent{
		Kind:           "headers",
		RequestID:      response.RequestId,
		StatusCode:     response.StatusCode,
		StatusText:     response.StatusText,
		ReplyHeader:    protocolRuntimeHeaderValue(response.Headers, "X-Reply"),
		SequenceNumber: response.SequenceNumber,
	})
	return nil
}

// SendChunk 无损记录 data，并在释放 mutex 后通知 raw server 可以写下一块。
func (s *protocolRuntimeHTTPSender) SendChunk(value interface{}) error {
	chunk := value.(*HTTPTunnelResponseChunk)
	s.mu.Lock()
	s.events = append(s.events, protocolTunnelHTTPEvent{
		Kind:           "chunk",
		RequestID:      chunk.RequestId,
		SequenceNumber: chunk.SequenceNumber,
		DataBase64:     base64.StdEncoding.EncodeToString(chunk.Data),
		IsFinal:        chunk.IsFinal,
	})
	s.mu.Unlock()
	if !chunk.IsFinal && s.chunkAck != nil {
		select {
		case s.chunkAck <- struct{}{}:
		default:
		}
	}
	return nil
}

// SendError 记录完整稳定错误文本；取消 case 会在严格验证后转为无端口分类。
func (s *protocolRuntimeHTTPSender) SendError(value interface{}) error {
	response := value.(*HTTPTunnelResponseError)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, protocolTunnelHTTPEvent{
		Kind:         "error",
		RequestID:    response.RequestId,
		ErrorMessage: response.ErrorMessage,
	})
	return nil
}

// snapshot 返回与网络任务解耦的事件切片。
func (s *protocolRuntimeHTTPSender) snapshot() []protocolTunnelHTTPEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocolTunnelHTTPEvent(nil), s.events...)
}

// protocolRuntimeWSSender 记录 WSHandler 上行事件，并提供条件等待避免依赖 sleep。
type protocolRuntimeWSSender struct {
	mu     sync.Mutex
	events []protocolTunnelWSEvent
	wake   chan struct{}
}

// newProtocolRuntimeWSSender 构造带有合并唤醒信号的 recorder。
func newProtocolRuntimeWSSender() *protocolRuntimeWSSender {
	return &protocolRuntimeWSSender{wake: make(chan struct{}, 1)}
}

// signal 通知等待方重新检查事件数量；满缓冲表示已有未消费通知。
func (s *protocolRuntimeWSSender) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// SendOpened 记录握手成功和选中子协议。
func (s *protocolRuntimeWSSender) SendOpened(value interface{}) error {
	response := value.(*WSTunnelOpened)
	s.mu.Lock()
	s.events = append(s.events, protocolTunnelWSEvent{
		Kind: "opened", TunnelID: response.TunnelId, Protocol: response.Protocol,
	})
	s.mu.Unlock()
	s.signal()
	return nil
}

// SendMessage 记录本地服务端发来的 text/binary frame。
func (s *protocolRuntimeWSSender) SendMessage(value interface{}) error {
	message := value.(*WSTunnelMessage)
	s.mu.Lock()
	s.events = append(s.events, protocolTunnelWSEvent{
		Kind: "message", TunnelID: message.TunnelId,
		DataBase64: base64.StdEncoding.EncodeToString(message.Data), IsBinary: message.IsBinary,
	})
	s.mu.Unlock()
	s.signal()
	return nil
}

// SendClose 记录本地服务端 close frame。
func (s *protocolRuntimeWSSender) SendClose(value interface{}) error {
	message := value.(*WSTunnelClose)
	s.mu.Lock()
	s.events = append(s.events, protocolTunnelWSEvent{
		Kind: "close", TunnelID: message.TunnelId, Code: message.Code, Reason: message.Reason,
	})
	s.mu.Unlock()
	s.signal()
	return nil
}

// SendError 记录握手或读循环的原始对外错误文本。
func (s *protocolRuntimeWSSender) SendError(value interface{}) error {
	response := value.(*WSTunnelError)
	s.mu.Lock()
	s.events = append(s.events, protocolTunnelWSEvent{
		Kind: "error", TunnelID: response.TunnelId, ErrorMessage: response.ErrorMessage,
	})
	s.mu.Unlock()
	s.signal()
	return nil
}

// snapshot 返回当前事件副本。
func (s *protocolRuntimeWSSender) snapshot() []protocolTunnelWSEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]protocolTunnelWSEvent(nil), s.events...)
}

// waitFor 等待事件满足 predicate；超时意味着真实转发链路未产生预期事件。
func (s *protocolRuntimeWSSender) waitFor(t *testing.T, predicate func([]protocolTunnelWSEvent) bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		if predicate(s.snapshot()) {
			return
		}
		select {
		case <-s.wake:
		case <-deadline.C:
			t.Fatalf("timed out waiting for websocket events: %#v", s.snapshot())
		}
	}
}

// TestProtocolTunnelRuntimeDifferentialTranscript 生成 HTTP 与本地 WebSocket 两份真实
// runtime transcript。它不访问 staging，也不修改 Go oracle 工作树。
func TestProtocolTunnelRuntimeDifferentialTranscript(t *testing.T) {
	httpTranscript := protocolTunnelHTTPTranscript{
		SchemaVersion: 1,
		Protocol:      "tunnel_http_runtime",
		Cases:         protocolCaptureTunnelHTTPCases(t),
	}
	wsTranscript := protocolTunnelWSTranscript{
		SchemaVersion: 1,
		Protocol:      "tunnel_ws_runtime",
		Cases:         protocolCaptureTunnelWSCases(t),
	}
	protocolWriteRuntimeTranscript(t, "PROTOCOL_HTTP_TUNNEL_OUTPUT", httpTranscript)
	protocolWriteRuntimeTranscript(t, "PROTOCOL_WS_TUNNEL_OUTPUT", wsTranscript)
}

// protocolCaptureTunnelHTTPCases 按 manifest 固定顺序执行 HTTP runtime 场景。
func protocolCaptureTunnelHTTPCases(t *testing.T) []protocolTunnelHTTPCase {
	t.Helper()
	return []protocolTunnelHTTPCase{
		protocolCaptureRawHTTPCase(t, "stream_flush_and_request", "req-stream", "201 Created", "stream", [][]byte{[]byte("alpha"), []byte("beta")}, &HTTPTunnelRequest{
			RequestId: "req-stream", Port: 0, Method: http.MethodPost, Path: "/stream?q=1",
			Headers: []*Header{
				{Name: "X-Input", Value: "yes"},
				{Name: "Host", Value: "ignored.example"},
			},
			Body: []byte("request-body"),
		}),
		protocolCaptureRawHTTPCase(t, "custom_reason_phrase", "req-custom", "299 Custom Tunnel Reason", "custom", nil, &HTTPTunnelRequest{
			RequestId: "req-custom", Method: http.MethodGet, Path: "/custom",
		}),
		protocolCaptureRawHTTPCase(t, "empty_reason_phrase", "req-empty", "298 ", "empty", nil, &HTTPTunnelRequest{
			RequestId: "req-empty", Method: http.MethodGet, Path: "/empty",
		}),
		protocolCaptureInvalidHTTPMethod(t),
		protocolCaptureInvalidHTTPHeader(t),
		protocolCaptureFragmentHeaderError(t),
		protocolCaptureHTTPConnectionRefused(t),
		protocolCaptureFragmentConnectionRefused(t),
		protocolCaptureRawQueryBytes(t),
		protocolCaptureSlowConnectOutsideHeaderTimeout(t),
		protocolCaptureResponseBeforeSlowBodyCompletes(t),
		protocolCaptureSlowResponseHeaderTimeout(t),
		protocolCaptureMultiMemberAutoGzip(t),
		protocolCaptureInvalidAutoGzip(t),
		protocolCaptureTruncatedAutoGzip(t),
		protocolCapturePartialTruncatedAutoGzip(t),
		protocolCaptureChecksumAutoGzip(t),
		protocolCaptureMalformedHTTPResponses(t, "early_eof_response", "req-early-eof", nil),
		protocolCaptureMalformedHTTPResponses(t, "malformed_status_response", "req-malformed-status", []byte("NOT HTTP\r\n\r\n")),
		protocolCaptureMalformedHTTPResponses(t, "invalid_header_response", "req-invalid-response-header", []byte("HTTP/1.1 200 OK\r\nBad\x00Header: x\r\n\r\n")),
		protocolCaptureMalformedHTTPResponses(t, "conflicting_content_length", "req-conflicting-length", []byte("HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx")),
		protocolCaptureMalformedChunk(t),
		protocolCaptureConnectionResetBeforeHeaders(t),
		protocolCaptureHTTPCancellation(t),
	}
}

// protocolCaptureRawHTTPCase 启动一次性 raw HTTP/1.1 mock，并调用真实 Handler。
// raw 响应用于保留 net/http 可见的 custom/empty reason phrase 和显式 flush 边界。
func protocolCaptureRawHTTPCase(
	t *testing.T,
	name string,
	requestID string,
	statusLine string,
	replyHeader string,
	chunks [][]byte,
	request *HTTPTunnelRequest,
) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	chunkAck := make(chan struct{}, len(chunks))
	sender := &protocolRuntimeHTTPSender{chunkAck: chunkAck}
	requestCapture := make(chan protocolTunnelHTTPRequest, 1)
	serverDone := make(chan error, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		inbound, err := http.ReadRequest(reader)
		if err != nil {
			serverDone <- err
			return
		}
		body, err := io.ReadAll(inbound.Body)
		if err != nil {
			serverDone <- err
			return
		}
		requestCapture <- protocolTunnelHTTPRequest{
			Observed:                  true,
			Method:                    inbound.Method,
			Target:                    inbound.URL.RequestURI(),
			HostMatchesLoopbackTarget: inbound.Host == fmt.Sprintf("localhost:%d", port),
			UserAgent:                 inbound.UserAgent(),
			InputHeader:               inbound.Header.Get("X-Input"),
			Body:                      string(body),
		}

		writer := bufio.NewWriter(conn)
		if _, err := fmt.Fprintf(writer,
			"HTTP/1.1 %s\r\nTransfer-Encoding: chunked\r\nX-Reply: %s\r\nConnection: close\r\n\r\n",
			statusLine, replyHeader,
		); err != nil {
			serverDone <- err
			return
		}
		if err := writer.Flush(); err != nil {
			serverDone <- err
			return
		}
		for _, chunk := range chunks {
			if _, err := fmt.Fprintf(writer, "%x\r\n", len(chunk)); err != nil {
				serverDone <- err
				return
			}
			if _, err := writer.Write(chunk); err != nil {
				serverDone <- err
				return
			}
			if _, err := writer.WriteString("\r\n"); err != nil {
				serverDone <- err
				return
			}
			if err := writer.Flush(); err != nil {
				serverDone <- err
				return
			}
			// 正确实现会在下一次 flush 前把当前小块交给 sender；超时后仍继续，
			// 让错误实现产生 chunk transcript 差异，而不是把门禁卡死。
			select {
			case <-chunkAck:
			case <-time.After(500 * time.Millisecond):
			}
		}
		if _, err := writer.WriteString("0\r\n\r\n"); err != nil {
			serverDone <- err
			return
		}
		if err := writer.Flush(); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	request.Port = int32(port)
	handler := &Handler{logger: slog.Default()}
	handlerError := handler.HandleRequest(context.Background(), request, sender)
	capture := protocolReceive(t, requestCapture, "HTTP request capture")
	if err := protocolReceive(t, serverDone, "HTTP mock completion"); err != nil {
		t.Fatalf("%s raw HTTP mock: %v", name, err)
	}
	_ = listener.Close()
	return protocolTunnelHTTPCase{
		Name: name, Request: capture, Events: sender.snapshot(),
		HandlerError: protocolRuntimeErrorText(handlerError),
	}
}

// protocolCaptureInvalidHTTPMethod 固定 http.NewRequest 的方法校验错误和 sender 顺序。
func protocolCaptureInvalidHTTPMethod(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-invalid-method", Port: 9, Method: "BAD METHOD", Path: "/invalid",
	}, sender)
	return protocolTunnelHTTPCase{
		Name: "invalid_method", Events: sender.snapshot(), HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureInvalidHTTPHeader 固定 Transport 对非法 header name 的可观察错误。
func protocolCaptureInvalidHTTPHeader(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-invalid-header", Port: 9, Method: http.MethodGet, Path: "/invalid",
		Headers: []*Header{{Name: "Bad Header", Value: "value"}},
	}, sender)
	return protocolTunnelHTTPCase{
		Name: "invalid_header", Events: sender.snapshot(), HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureFragmentHeaderError 固定 URL fragment 在 Client.Do header 校验错误中的
// EscapedFragment 文本。fragment 不写入 request-target，但必须保留在 url.Error 中。
func protocolCaptureFragmentHeaderError(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-fragment-header", Port: 9, Method: http.MethodGet,
		Path:    "/invalid?x=1#frag <\\é%2f'[]",
		Headers: []*Header{{Name: "Bad Header", Value: "value"}},
	}, sender)
	return protocolTunnelHTTPCase{
		Name: "fragment_header_error", Events: sender.snapshot(), HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureFragmentConnectionRefused 固定 fragment 在拨号失败 url.Error 中的保留
// 规则，并证明它没有进入实际拨号地址或 request-target。
func protocolCaptureFragmentConnectionRefused(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	if err := listener.Close(); err != nil {
		t.Fatalf("close fragment-refused listener: %v", err)
	}
	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-fragment-refused", Port: int32(port), Method: http.MethodGet,
		Path: "/refused?x=1#frag <\\é%2f'[]",
	}, sender)
	events := sender.snapshot()
	if len(events) != 1 || events[0].Kind != "error" {
		t.Fatalf("unexpected fragment connection-refused events: %#v", events)
	}
	portText := strconv.Itoa(port)
	if strings.Count(events[0].ErrorMessage, portText) < 2 {
		t.Fatalf("fragment connection-refused error missing ports: %q", events[0].ErrorMessage)
	}
	events[0].ErrorMessage = strings.ReplaceAll(events[0].ErrorMessage, portText, "{port}")
	return protocolTunnelHTTPCase{
		Name: "fragment_connection_refused", Events: events, HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureRawQueryBytes 逐字写出除 `#`（URL fragment 分隔符）外的全部可打印
// ASCII，并附加 UTF-8 和畸形 percent escape。Go RawQuery 不做 RFC URI 规范化，raw
// server 因而直接读取首行，不能用会拒绝空格的 http.ReadRequest。
func protocolCaptureRawQueryBytes(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	requestCapture := make(chan protocolTunnelHTTPRequest, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		line, err := reader.ReadString('\n')
		if err != nil {
			serverDone <- err
			return
		}
		trimmed := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		versionIndex := strings.LastIndex(trimmed, " HTTP/1.1")
		if !strings.HasPrefix(trimmed, "GET ") || versionIndex < len("GET ") {
			serverDone <- fmt.Errorf("unexpected raw request line %q", trimmed)
			return
		}
		hostMatches := false
		for {
			headerLine, readErr := reader.ReadString('\n')
			if readErr != nil {
				serverDone <- readErr
				return
			}
			if headerLine == "\r\n" {
				break
			}
			if strings.EqualFold(strings.TrimSpace(headerLine), fmt.Sprintf("Host: localhost:%d", port)) {
				hostMatches = true
			}
		}
		requestCapture <- protocolTunnelHTTPRequest{
			Observed: true, Method: "GET", Target: trimmed[len("GET "):versionIndex],
			HostMatchesLoopbackTarget: hostMatches,
		}
		_, err = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		serverDone <- err
	}()

	var printable strings.Builder
	for value := byte(0x20); value <= 0x7e; value++ {
		if value != '#' {
			printable.WriteByte(value)
		}
	}
	path := "/raw?bytes=" + printable.String() + "é%zz"
	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-raw-query", Port: int32(port), Method: http.MethodGet, Path: path,
	}, sender)
	capture := protocolReceive(t, requestCapture, "raw query request capture")
	if serverErr := protocolReceive(t, serverDone, "raw query mock completion"); serverErr != nil {
		t.Fatalf("raw query mock: %v", serverErr)
	}
	_ = listener.Close()
	return protocolTunnelHTTPCase{
		Name: "raw_query_printable_bytes", Request: capture, Events: sender.snapshot(),
		HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolHeaderTimeoutHandler 创建带短 ResponseHeaderTimeout 的真实 Go Transport。
// dialDelay 只由 slow-connect 场景使用，明确位于 Transport 的 header timer 之外。
func protocolHeaderTimeoutHandler(timeout, dialDelay time.Duration) (*Handler, *http.Transport) {
	transport := &http.Transport{ResponseHeaderTimeout: timeout}
	if dialDelay > 0 {
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(dialDelay):
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	}
	return &Handler{logger: slog.Default(), httpClient: &http.Client{Transport: transport}}, transport
}

// protocolCaptureSlowConnectOutsideHeaderTimeout 证明拨号延迟不消耗响应 header timeout。
func protocolCaptureSlowConnectOutsideHeaderTimeout(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		_, err = http.ReadRequest(bufio.NewReader(conn))
		if err == nil {
			_, err = io.WriteString(conn, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
		}
		serverDone <- err
	}()
	handler, transport := protocolHeaderTimeoutHandler(30*time.Millisecond, 120*time.Millisecond)
	defer transport.CloseIdleConnections()
	sender := &protocolRuntimeHTTPSender{}
	started := time.Now()
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-slow-connect", Port: int32(port), Method: http.MethodGet, Path: "/slow-connect",
	}, sender)
	if time.Since(started) < 100*time.Millisecond {
		t.Fatalf("slow connect delay was not exercised")
	}
	if serverErr := protocolReceive(t, serverDone, "slow connect mock completion"); serverErr != nil {
		t.Fatalf("slow connect mock: %v", serverErr)
	}
	_ = listener.Close()
	return protocolTunnelHTTPCase{
		Name: "slow_connect_outside_header_timeout", Events: sender.snapshot(),
		HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureResponseBeforeSlowBodyCompletes 固定 readLoop 先于 writeLoop 返回响应的
// 行为：服务端不读取 8 MiB body，响应仍不得被 header timeout 截断。
func protocolCaptureResponseBeforeSlowBodyCompletes(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		inbound, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			serverDone <- err
			return
		}
		if inbound.ContentLength != 8*1024*1024 {
			serverDone <- fmt.Errorf("unexpected slow body length %d", inbound.ContentLength)
			return
		}
		if _, err = io.WriteString(conn, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"); err != nil {
			serverDone <- err
			return
		}
		time.Sleep(100 * time.Millisecond)
		serverDone <- nil
	}()
	handler, transport := protocolHeaderTimeoutHandler(30*time.Millisecond, 0)
	defer transport.CloseIdleConnections()
	sender := &protocolRuntimeHTTPSender{}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-slow-body", Port: int32(port), Method: http.MethodPost, Path: "/slow-body",
		Body: bytes.Repeat([]byte("x"), 8*1024*1024),
	}, sender)
	if serverErr := protocolReceive(t, serverDone, "slow body mock completion"); serverErr != nil {
		t.Fatalf("slow body mock: %v", serverErr)
	}
	_ = listener.Close()
	return protocolTunnelHTTPCase{
		Name: "response_before_slow_body_complete", Events: sender.snapshot(),
		HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureSlowResponseHeaderTimeout 固定请求写完后才启动的 timer 和错误文本。
func protocolCaptureSlowResponseHeaderTimeout(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	serverDone := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			_, _ = http.ReadRequest(bufio.NewReader(conn))
			time.Sleep(120 * time.Millisecond)
			_, _ = io.WriteString(conn, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
		}
		serverDone <- struct{}{}
	}()
	handler, transport := protocolHeaderTimeoutHandler(30*time.Millisecond, 0)
	defer transport.CloseIdleConnections()
	sender := &protocolRuntimeHTTPSender{}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-slow-header", Port: int32(port), Method: http.MethodGet, Path: "/slow-header",
	}, sender)
	events := sender.snapshot()
	if len(events) != 1 || events[0].Kind != "error" {
		t.Fatalf("unexpected slow-header events: %#v", events)
	}
	events[0].ErrorMessage = strings.ReplaceAll(events[0].ErrorMessage, strconv.Itoa(port), "{port}")
	protocolReceive(t, serverDone, "slow header mock completion")
	_ = listener.Close()
	return protocolTunnelHTTPCase{
		Name: "slow_response_header_timeout", Events: events, HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureHTTPConnectionRefused 在刚关闭的 loopback 端口上固定 net/http 的
// 完整 dial error。动态端口只替换成单一占位符，其余地址族、标点和 OS 文本均逐字
// 写入 transcript，确保 Rust 不能只返回笼统的 connection refused。
func protocolCaptureHTTPConnectionRefused(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	if err := listener.Close(); err != nil {
		t.Fatalf("close refused listener: %v", err)
	}
	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-refused", Port: int32(port), Method: http.MethodGet, Path: "/refused",
	}, sender)
	events := sender.snapshot()
	if len(events) != 1 || events[0].Kind != "error" {
		t.Fatalf("unexpected connection-refused events: %#v", events)
	}
	portText := strconv.Itoa(port)
	if strings.Count(events[0].ErrorMessage, portText) < 2 {
		t.Fatalf("connection-refused error does not contain URL and dial ports: %q", events[0].ErrorMessage)
	}
	events[0].ErrorMessage = strings.ReplaceAll(events[0].ErrorMessage, portText, "{port}")
	return protocolTunnelHTTPCase{
		Name: "connection_refused", Events: events, HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolGzipMember 使用 Go 标准库生成一个确定 gzip member。
func protocolGzipMember(t *testing.T, payload []byte) []byte {
	t.Helper()
	var encoded bytes.Buffer
	writer := gzip.NewWriter(&encoded)
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("write gzip member: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip member: %v", err)
	}
	return encoded.Bytes()
}

// protocolCaptureMultiMemberAutoGzip 固定 Go gzip.Reader 默认 multistream 行为。
func protocolCaptureMultiMemberAutoGzip(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	body := append(protocolGzipMember(t, []byte("first-member|")), protocolGzipMember(t, []byte("second-member"))...)
	return protocolCaptureAutoGzipError(t, "multi_member_auto_gzip", "req-multi-gzip", body)
}

// protocolCaptureInvalidAutoGzip 固定 Transport 自动请求 gzip 后遇到非法 gzip header
// 的 headers/event/error 顺序与完整文本。
func protocolCaptureInvalidAutoGzip(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	return protocolCaptureAutoGzipError(t, "invalid_auto_gzip", "req-invalid-gzip", []byte("not-a-gzip-stream"))
}

// protocolCaptureTruncatedAutoGzip 固定只有 gzip magic、尚未完成 header 就 EOF 时的
// `unexpected EOF` 文本，防止 decoder 暴露第三方库自己的措辞。
func protocolCaptureTruncatedAutoGzip(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	return protocolCaptureAutoGzipError(t, "truncated_auto_gzip", "req-truncated-gzip", []byte{0x1f, 0x8b})
}

// protocolCapturePartialTruncatedAutoGzip 保留完整解压 payload 后截断 footer，固定“先
// chunk、后 unexpected EOF”的外部事件顺序。
func protocolCapturePartialTruncatedAutoGzip(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	body := protocolGzipMember(t, []byte("payload-before-truncated-footer"))
	body = body[:len(body)-4]
	return protocolCaptureAutoGzipError(t, "partial_truncated_auto_gzip", "req-partial-truncated-gzip", body)
}

// protocolCaptureChecksumAutoGzip 损坏 CRC32 footer，固定 Go `gzip: invalid checksum`
// 及已经解压的数据不可回滚的行为。
func protocolCaptureChecksumAutoGzip(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	body := protocolGzipMember(t, []byte("payload-before-bad-checksum"))
	body[len(body)-8] ^= 0xff
	return protocolCaptureAutoGzipError(t, "checksum_auto_gzip", "req-checksum-gzip", body)
}

// protocolCaptureAutoGzipError 启动一次性 raw server，写入指定损坏 gzip body，并
// 返回真实 Handler 的 request、headers、error event 及函数错误。
func protocolCaptureAutoGzipError(t *testing.T, name string, requestID string, body []byte) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	requestCapture := make(chan protocolTunnelHTTPRequest, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		inbound, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			serverDone <- err
			return
		}
		requestCapture <- protocolTunnelHTTPRequest{
			Observed:                  true,
			Method:                    inbound.Method,
			Target:                    inbound.URL.RequestURI(),
			HostMatchesLoopbackTarget: inbound.Host == fmt.Sprintf("localhost:%d", port),
			UserAgent:                 inbound.UserAgent(),
		}
		writer := bufio.NewWriter(conn)
		if _, err := fmt.Fprintf(writer,
			"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", len(body),
		); err != nil {
			serverDone <- err
			return
		}
		if _, err := writer.Write(body); err != nil {
			serverDone <- err
			return
		}
		serverDone <- writer.Flush()
	}()

	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: requestID, Port: int32(port), Method: http.MethodGet, Path: "/gzip-error",
	}, sender)
	capture := protocolReceive(t, requestCapture, "gzip request capture")
	if serverErr := protocolReceive(t, serverDone, "gzip mock completion"); serverErr != nil {
		t.Fatalf("%s raw HTTP mock: %v", name, serverErr)
	}
	_ = listener.Close()
	return protocolTunnelHTTPCase{
		Name: name, Request: capture, Events: sender.snapshot(),
		HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureMalformedHTTPResponses 把受控 raw response 直接交给 Go Transport，
// 固定首字节 EOF、非法状态行/header 和冲突 Content-Length 的 url.Error 文本。
func protocolCaptureMalformedHTTPResponses(t *testing.T, name, requestID string, response []byte) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	requestCapture := make(chan protocolTunnelHTTPRequest, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		inbound, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			serverDone <- err
			return
		}
		requestCapture <- protocolTunnelHTTPRequest{
			Observed: true, Method: inbound.Method, Target: inbound.URL.RequestURI(),
			HostMatchesLoopbackTarget: inbound.Host == fmt.Sprintf("localhost:%d", port),
			UserAgent:                 inbound.UserAgent(),
		}
		if len(response) > 0 {
			_, err = conn.Write(response)
		}
		serverDone <- err
	}()

	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: requestID, Port: int32(port), Method: http.MethodGet, Path: "/malformed",
	}, sender)
	capture := protocolReceive(t, requestCapture, "malformed response request capture")
	if serverErr := protocolReceive(t, serverDone, "malformed response mock completion"); serverErr != nil {
		t.Fatalf("%s mock: %v", name, serverErr)
	}
	_ = listener.Close()
	events := sender.snapshot()
	for index := range events {
		events[index].ErrorMessage = strings.ReplaceAll(events[index].ErrorMessage, strconv.Itoa(port), "{port}")
	}
	return protocolTunnelHTTPCase{
		Name: name, Request: capture, Events: events, HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureMalformedChunk 固定 header 已交付后，chunk size parser 在 body Read
// 阶段失败的事件顺序与 Go 文本。
func protocolCaptureMalformedChunk(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	return protocolCaptureMalformedHTTPResponses(t, "malformed_chunk", "req-malformed-chunk", []byte(
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nZ\r\n",
	))
}

// protocolCaptureConnectionResetBeforeHeaders 使用 SO_LINGER=0 制造 Linux TCP RST，
// 并仅替换已从真实 socket 地址取得的两个随机端口；地址族、箭头和 OS 文本保留。
func protocolCaptureConnectionResetBeforeHeaders(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	requestCapture := make(chan protocolTunnelHTTPRequest, 1)
	addressCapture := make(chan [2]string, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		tcpConn := conn.(*net.TCPConn)
		inbound, err := http.ReadRequest(bufio.NewReader(tcpConn))
		if err != nil {
			_ = tcpConn.Close()
			serverDone <- err
			return
		}
		requestCapture <- protocolTunnelHTTPRequest{
			Observed: true, Method: inbound.Method, Target: inbound.URL.RequestURI(),
			HostMatchesLoopbackTarget: inbound.Host == fmt.Sprintf("localhost:%d", port),
			UserAgent:                 inbound.UserAgent(),
		}
		addressCapture <- [2]string{tcpConn.RemoteAddr().String(), tcpConn.LocalAddr().String()}
		if err = tcpConn.SetLinger(0); err == nil {
			err = tcpConn.Close()
		}
		serverDone <- err
	}()

	sender := &protocolRuntimeHTTPSender{}
	handler := &Handler{logger: slog.Default()}
	err := handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
		RequestId: "req-reset", Port: int32(port), Method: http.MethodGet, Path: "/reset",
	}, sender)
	capture := protocolReceive(t, requestCapture, "reset request capture")
	addresses := protocolReceive(t, addressCapture, "reset addresses")
	if serverErr := protocolReceive(t, serverDone, "reset mock completion"); serverErr != nil {
		t.Fatalf("reset mock: %v", serverErr)
	}
	events := sender.snapshot()
	if len(events) != 1 || events[0].Kind != "error" {
		t.Fatalf("unexpected reset events: %#v", events)
	}
	expectedNetworkText := fmt.Sprintf("read tcp %s->%s: read: connection reset by peer", addresses[0], addresses[1])
	if !strings.Contains(events[0].ErrorMessage, expectedNetworkText) {
		t.Fatalf("reset error %q does not contain %q", events[0].ErrorMessage, expectedNetworkText)
	}
	events[0].ErrorMessage = strings.ReplaceAll(events[0].ErrorMessage, addresses[0], "127.0.0.1:{local_port}")
	events[0].ErrorMessage = strings.ReplaceAll(events[0].ErrorMessage, addresses[1], "127.0.0.1:{port}")
	events[0].ErrorMessage = strings.ReplaceAll(events[0].ErrorMessage, strconv.Itoa(port), "{port}")
	_ = listener.Close()
	return protocolTunnelHTTPCase{
		Name: "connection_reset_before_headers", Request: capture, Events: events,
		HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureHTTPCancellation 先等待 mock 收到真实请求，再通过 CancelRequest 取消。
// 原始 net/http 错误含随机端口；probe 必须先严格断言关键文本，再输出稳定分类，不能
// 使用比较器 normalizer 擦除任意错误内容。
func protocolCaptureHTTPCancellation(t *testing.T) protocolTunnelHTTPCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	requestCapture := make(chan protocolTunnelHTTPRequest, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		inbound, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			serverDone <- err
			return
		}
		body, err := io.ReadAll(inbound.Body)
		if err != nil {
			serverDone <- err
			return
		}
		requestCapture <- protocolTunnelHTTPRequest{
			Observed:                  true,
			Method:                    inbound.Method,
			Target:                    inbound.URL.RequestURI(),
			HostMatchesLoopbackTarget: inbound.Host == fmt.Sprintf("localhost:%d", port),
			UserAgent:                 inbound.UserAgent(),
			Body:                      string(body),
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, err = io.Copy(io.Discard, conn)
		if networkError, ok := err.(net.Error); ok && networkError.Timeout() {
			serverDone <- fmt.Errorf("client did not close cancelled request")
			return
		}
		serverDone <- nil
	}()

	handler := &Handler{logger: slog.Default()}
	sender := &protocolRuntimeHTTPSender{}
	done := make(chan error, 1)
	go func() {
		done <- handler.HandleRequest(context.Background(), &HTTPTunnelRequest{
			RequestId: "req-cancel", Port: int32(port), Method: http.MethodGet, Path: "/cancel",
		}, sender)
	}()
	capture := protocolReceive(t, requestCapture, "cancel request capture")
	handler.CancelRequest("req-cancel")
	handlerError := protocolReceive(t, done, "cancelled Handler completion")
	if err := protocolReceive(t, serverDone, "cancel mock completion"); err != nil {
		t.Fatalf("cancel mock: %v", err)
	}
	_ = listener.Close()
	events := sender.snapshot()
	if len(events) != 1 || events[0].Kind != "error" ||
		!strings.Contains(events[0].ErrorMessage, "request failed") {
		t.Fatalf("unexpected cancellation events: %#v", events)
	}
	if strings.Contains(events[0].ErrorMessage, "context canceled") {
		events[0].ErrorClass = "context_canceled"
	} else {
		events[0].ErrorClass = "request_failed_without_context_marker"
	}
	events[0].ErrorMessage = ""
	return protocolTunnelHTTPCase{
		Name: "cancel", Request: capture, Events: events,
		HandlerError: protocolRuntimeErrorText(handlerError),
	}
}

// protocolCaptureTunnelWSCases 按 manifest 固定顺序执行 WebSocket runtime 场景。
func protocolCaptureTunnelWSCases(t *testing.T) []protocolTunnelWSCase {
	t.Helper()
	return []protocolTunnelWSCase{
		protocolCaptureWSSuccess(t),
		protocolCaptureWSBadHandshake(t),
		protocolCaptureWSAbnormalEOF(t),
	}
}

// protocolRuntimeWSServerResult 汇总成功 mock 收到的两个客户端 frame 和 close。
type protocolRuntimeWSServerResult struct {
	messages []protocolTunnelWSWireMessage
	close    protocolTunnelWSClose
}

// protocolCaptureWSSuccess 覆盖握手 header、子协议、双向 text/binary 和主动 close。
func protocolCaptureWSSuccess(t *testing.T) protocolTunnelWSCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	handshakeChannel := make(chan protocolTunnelHandshake, 1)
	resultChannel := make(chan protocolRuntimeWSServerResult, 1)
	errorChannel := make(chan error, 1)
	upgrader := websocket.Upgrader{Subprotocols: []string{"proto1"}}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		handshakeChannel <- protocolTunnelHandshake{
			Observed: true, Host: request.Host, TunnelHeader: request.Header.Get("X-Tunnel"),
			OfferedProtocols: request.Header.Get("Sec-Websocket-Protocol"),
		}
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			errorChannel <- err
			return
		}
		defer connection.Close()
		result := protocolRuntimeWSServerResult{}
		messageType, data, err := connection.ReadMessage()
		if err != nil {
			errorChannel <- err
			return
		}
		result.messages = append(result.messages, protocolTunnelWSWireMessage{
			Kind: protocolRuntimeWSKind(messageType), DataBase64: base64.StdEncoding.EncodeToString(data),
		})
		if err := connection.WriteMessage(websocket.TextMessage, []byte("server-text")); err != nil {
			errorChannel <- err
			return
		}
		if err := connection.WriteMessage(websocket.BinaryMessage, []byte{0x00, 0xfb, 0xff}); err != nil {
			errorChannel <- err
			return
		}
		messageType, data, err = connection.ReadMessage()
		if err != nil {
			errorChannel <- err
			return
		}
		result.messages = append(result.messages, protocolTunnelWSWireMessage{
			Kind: protocolRuntimeWSKind(messageType), DataBase64: base64.StdEncoding.EncodeToString(data),
		})
		_, _, err = connection.ReadMessage()
		var closeError *websocket.CloseError
		_ = errors.As(err, &closeError)
		if closeError == nil {
			errorChannel <- fmt.Errorf("expected websocket close, got %v", err)
			return
		}
		result.close = protocolTunnelWSClose{
			Observed: true, Code: closeError.Code, Reason: closeError.Text,
		}
		resultChannel <- result
	})}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errorChannel <- err
		}
	}()

	handler := &WSHandler{logger: slog.Default()}
	sender := newProtocolRuntimeWSSender()
	err := handler.OpenTunnel(context.Background(), &WSTunnelOpen{
		TunnelId: "ws-success", Port: int32(port), Path: "/socket",
		Headers: []*Header{
			{Name: "X-Tunnel", Value: "yes"},
			{Name: "Host", Value: "fixed.example"},
		},
		Protocols: []string{"proto2", "proto1"},
	}, sender)
	if err != nil {
		t.Fatalf("open success websocket: %v", err)
	}
	if err := handler.SendMessage(context.Background(), &WSTunnelMessage{
		TunnelId: "ws-success", Data: []byte("client-text"), IsBinary: false,
	}); err != nil {
		t.Fatalf("send client text: %v", err)
	}
	sender.waitFor(t, func(events []protocolTunnelWSEvent) bool {
		messages := 0
		for _, event := range events {
			if event.Kind == "message" {
				messages++
			}
		}
		return messages == 2
	})
	if err := handler.SendMessage(context.Background(), &WSTunnelMessage{
		TunnelId: "ws-success", Data: []byte{0x01, 0x02, 0x03}, IsBinary: true,
	}); err != nil {
		t.Fatalf("send client binary: %v", err)
	}
	if err := handler.CloseTunnel(context.Background(), &WSTunnelClose{
		TunnelId: "ws-success", Code: websocket.CloseNormalClosure, Reason: "client-done",
	}); err != nil {
		t.Fatalf("close success websocket: %v", err)
	}

	handshake := protocolReceive(t, handshakeChannel, "websocket handshake")
	result := protocolReceiveWithError(t, resultChannel, errorChannel, "websocket server result")
	_ = server.Close()
	return protocolTunnelWSCase{
		Name: "success_frames_and_close", Handshake: handshake,
		ClientMessages: result.messages, ClientClose: result.close,
		Events: sender.snapshot(), HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureWSBadHandshake 固定 Gorilla 在带 response 的 403 握手错误格式。
func protocolCaptureWSBadHandshake(t *testing.T) protocolTunnelWSCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
	})}
	go func() { _ = server.Serve(listener) }()
	handler := &WSHandler{logger: slog.Default()}
	sender := newProtocolRuntimeWSSender()
	err := handler.OpenTunnel(context.Background(), &WSTunnelOpen{
		TunnelId: "ws-bad-handshake", Port: int32(port), Path: "/reject",
	}, sender)
	_ = server.Close()
	return protocolTunnelWSCase{
		Name: "bad_handshake", ClientMessages: []protocolTunnelWSWireMessage{},
		Events: sender.snapshot(), HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolCaptureWSAbnormalEOF 完成合法 101 后直接关闭 TCP，不发送 close frame。
// Gorilla 把异常 EOF 转成 1006 close 事件；code/reason 是后端可见协议字段，因此
// 保持原文逐字节比较。
func protocolCaptureWSAbnormalEOF(t *testing.T) protocolTunnelWSCase {
	t.Helper()
	listener, port := protocolRuntimeListen(t)
	handshakeChannel := make(chan protocolTunnelHandshake, 1)
	serverDone := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer connection.Close()
		request, err := http.ReadRequest(bufio.NewReader(connection))
		if err != nil {
			serverDone <- err
			return
		}
		handshakeChannel <- protocolTunnelHandshake{Observed: true}
		key := request.Header.Get("Sec-Websocket-Key")
		digest := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		accept := base64.StdEncoding.EncodeToString(digest[:])
		if _, err := fmt.Fprintf(connection,
			"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
			accept,
		); err != nil {
			serverDone <- err
			return
		}
		// 给 dialer 足够时间消费完整握手，然后以 FIN 结束而不发送 WebSocket close。
		time.Sleep(100 * time.Millisecond)
		serverDone <- nil
	}()

	handler := &WSHandler{logger: slog.Default()}
	sender := newProtocolRuntimeWSSender()
	err := handler.OpenTunnel(context.Background(), &WSTunnelOpen{
		TunnelId: "ws-abnormal-eof", Port: int32(port), Path: "/eof",
	}, sender)
	sender.waitFor(t, func(events []protocolTunnelWSEvent) bool {
		for _, event := range events {
			if event.Kind == "close" {
				return true
			}
		}
		return false
	})
	handshake := protocolReceive(t, handshakeChannel, "abnormal EOF handshake")
	if serverError := protocolReceive(t, serverDone, "abnormal EOF server"); serverError != nil {
		t.Fatalf("abnormal EOF server: %v", serverError)
	}
	_ = listener.Close()
	return protocolTunnelWSCase{
		Name: "abnormal_eof", Handshake: handshake, ClientMessages: []protocolTunnelWSWireMessage{},
		Events: sender.snapshot(), HandlerError: protocolRuntimeErrorText(err),
	}
}

// protocolRuntimeHeaderValue 在已排序 response header 中读取首个匹配值。
func protocolRuntimeHeaderValue(headers []*Header, name string) string {
	for _, header := range headers {
		if header != nil && strings.EqualFold(header.Name, name) {
			return header.Value
		}
	}
	return ""
}

// protocolRuntimeWSKind 把 Gorilla frame 常量映射为稳定 schema 名称。
func protocolRuntimeWSKind(messageType int) string {
	switch messageType {
	case websocket.TextMessage:
		return "text"
	case websocket.BinaryMessage:
		return "binary"
	default:
		return fmt.Sprintf("type-%d", messageType)
	}
}

// protocolRuntimeListen 只监听 loopback 并返回动态端口；端口永不写 transcript。
func protocolRuntimeListen(t *testing.T) (net.Listener, int) {
	t.Helper()
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		listener.Close()
		t.Fatalf("split listener address: %v", err)
	}
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		listener.Close()
		t.Fatalf("parse listener port: %v", err)
	}
	return listener, port
}

// protocolRuntimeErrorText 把 nil 映射为空串，其他错误保持完整原文。
func protocolRuntimeErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// protocolReceive 在三秒内接收 mock 结果，避免 probe 永久挂起。
func protocolReceive[T any](t *testing.T, channel <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
		var zero T
		t.Fatalf("timed out waiting for %s", description)
		return zero
	}
}

// protocolReceiveWithError 等待成功结果，同时优先报告 mock 的异步错误。
func protocolReceiveWithError[T any](t *testing.T, result <-chan T, failures <-chan error, description string) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case err := <-failures:
		var zero T
		t.Fatalf("%s: %v", description, err)
		return zero
	case <-time.After(3 * time.Second):
		var zero T
		t.Fatalf("timed out waiting for %s", description)
		return zero
	}
}

// protocolWriteRuntimeTranscript 写 UTF-8 JSON 和单个结尾换行；缺少 runner 路径即失败。
func protocolWriteRuntimeTranscript(t *testing.T, environmentName string, transcript interface{}) {
	t.Helper()
	path := os.Getenv(environmentName)
	if path == "" {
		t.Fatalf("%s is required", environmentName)
	}
	encoded, err := json.Marshal(transcript)
	if err != nil {
		t.Fatalf("marshal %s: %v", environmentName, err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write %s: %v", environmentName, err)
	}
}
