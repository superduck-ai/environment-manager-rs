package api

// 本文件不会复制到 Go 工作树。Linux ARM64 门禁通过 `go test -overlay` 把它临时
// 映射到只读 Go 基线的 `internal/api` 包中，从而能够观察未导出的兼容实现，同时
// 保证迁移 oracle 的源码、索引和工作树状态不发生变化。

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic/api-go/environment-manager/internal/util"
)

// protocolHTTPHeaders 记录 Environment Manager 及 Go Transport 共同决定的稳定请求
// 头。Date、Content-Length 仍不参与比较；默认 User-Agent 与自动 Accept-Encoding
// 会被后端观察并控制解压行为，因此属于迁移兼容契约。
type protocolHTTPHeaders struct {
	Authorization            string `json:"authorization"`
	AcceptEncoding           string `json:"accept_encoding"`
	ContentType              string `json:"content_type"`
	EnvironmentRunnerVersion string `json:"environment_runner_version"`
	ClaudeCodeVersion        string `json:"claude_code_version"`
	TestHeader               string `json:"test_header"`
	UserAgent                string `json:"user_agent"`
}

// protocolHTTPRequest 是 mock 后端实际收到的一次完整、确定性业务请求。Body 保持
// 原始字节转换后的字符串，不先反序列化，因而字段顺序、omitempty 和结尾换行差异
// 都会直接导致 Go/Rust 差分失败。
type protocolHTTPRequest struct {
	Method  string              `json:"method"`
	Target  string              `json:"target"`
	Headers protocolHTTPHeaders `json:"headers"`
	Body    string              `json:"body"`
}

// protocolHTTPOutcome 记录调用者可见的解析结果或精确错误文本。两个字段始终存在，
// 避免比较器把“字段缺失”和“空字符串”误当成同一种状态。
type protocolHTTPOutcome struct {
	Name   string `json:"name"`
	Result string `json:"result"`
	Error  string `json:"error"`
}

// protocolHTTPTranscript 是 API 协议 probe 的稳定输出格式。
type protocolHTTPTranscript struct {
	SchemaVersion int                   `json:"schema_version"`
	Protocol      string                `json:"protocol"`
	Requests      []protocolHTTPRequest `json:"requests"`
	Outcomes      []protocolHTTPOutcome `json:"outcomes"`
}

// TestProtocolDifferentialTranscript 从真实 Go HttpClient 发出三次通用 JSON 请求，
// 覆盖 HTML JSON 编码、默认传输 header、gzip 成功响应、501 sentinel 和普通非 200
// 错误。动态 ID 的 `.`/`..` 点段已由受控后端格式约束排除，不再作为差分输入。
// 服务端完全位于当前测试进程，不依赖 staging、DNS 或外部网络；随机监听端口只用于
// 连接，不写入 transcript。
func TestProtocolDifferentialTranscript(t *testing.T) {
	t.Helper()

	var requestsMu sync.Mutex
	requests := make([]protocolHTTPRequest, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read failure", http.StatusInternalServerError)
			return
		}

		requestsMu.Lock()
		requests = append(requests, protocolHTTPRequest{
			Method: r.Method,
			Target: r.URL.RequestURI(),
			Headers: protocolHTTPHeaders{
				Authorization:            r.Header.Get("Authorization"),
				AcceptEncoding:           r.Header.Get("Accept-Encoding"),
				ContentType:              r.Header.Get("Content-Type"),
				EnvironmentRunnerVersion: r.Header.Get("x-environment-runner-version"),
				ClaudeCodeVersion:        r.Header.Get("x-claude-code-version"),
				TestHeader:               r.Header.Get("x-test-header"),
				UserAgent:                r.Header.Get("User-Agent"),
			},
			Body: string(body),
		})
		requestsMu.Unlock()

		switch r.URL.Path {
		case "/ok":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Encoding", "gzip")
			compressed := gzip.NewWriter(w)
			_, _ = io.WriteString(compressed, `{"ok":true,"echo":"stable"}`)
			_ = compressed.Close()
		case "/not-implemented":
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = io.WriteString(w, "missing")
		default:
			w.WriteHeader(http.StatusTeapot)
			_, _ = io.WriteString(w, "teapot")
		}
	}))
	defer server.Close()

	// Go 主程序通常在启动时赋值这两个变量。Probe 显式固定它们，使测试包和 Rust
	// Cargo 包都发出 0.1.0，且不会意外继承验证机上安装的 Claude 版本。
	util.Version = "0.1.0"
	util.ClaudeCodeVersion = ""
	client := NewHttpClientWithOptions(server.URL, slog.New(slog.NewTextHandler(io.Discard, nil)), ClientOptions{
		Timeout: 2 * time.Second,
		RetryConfig: &RetryConfig{
			MaxRetries:     0,
			InitialBackoff: time.Nanosecond,
			MaxBackoff:     time.Nanosecond,
			BackoffFactor:  1,
		},
		DefaultHeaders: map[string]string{"x-test-header": "stable-value"},
	})
	client.httpClient = server.Client()

	payload := map[string]interface{}{"hello": "world", "html": "<\u2028&>", "sequence": 7}
	outcomes := make([]protocolHTTPOutcome, 0, 5)
	var response struct {
		OK   bool   `json:"ok"`
		Echo string `json:"echo"`
	}
	err := client.PostJSONWithResponse(context.Background(), server.URL+"/ok?mode=success", payload, "fixed-api-token", "protocol success", &response)
	result := ""
	if err == nil {
		encoded, marshalErr := json.Marshal(response)
		if marshalErr != nil {
			t.Fatalf("marshal successful response: %v", marshalErr)
		}
		result = string(encoded)
	}
	outcomes = append(outcomes, protocolHTTPOutcome{Name: "success", Result: result, Error: errorString(err)})

	err = client.PostJSON(context.Background(), server.URL+"/not-implemented", payload, "fixed-api-token", "protocol missing")
	outcomes = append(outcomes, protocolHTTPOutcome{Name: "not_implemented", Result: "", Error: errorString(err)})

	err = client.PostJSON(context.Background(), server.URL+"/failure", payload, "fixed-api-token", "protocol failure")
	outcomes = append(outcomes, protocolHTTPOutcome{Name: "non_ok", Result: "", Error: errorString(err)})

	// 最后一次（这里也是唯一一次）attempt 被取消时，Go 仍返回 exhausted 包装；只有
	// 尚可继续重试并进入 backoff 时才返回 retry-cancelled。固定 `.invalid` URL 不会
	// 实际访问 DNS（context 已预先取消），也不会把 mock server 随机端口写入 transcript。
	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	err = client.PostJSON(cancelledContext, "http://api.invalid/cancelled", payload, "fixed-api-token", "protocol cancelled")
	outcomes = append(outcomes, protocolHTTPOutcome{Name: "cancelled_no_retry", Result: "", Error: errorString(err)})

	// 取得后立即关闭随机 loopback 端口，触发真实 Linux connection-refused；仅在
	// transcript 内归一化本次端口，其他 url.Error/dial 文本仍逐字比较。
	refusedListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind refused API probe port: %v", err)
	}
	refusedAddress := refusedListener.Addr().String()
	if err := refusedListener.Close(); err != nil {
		t.Fatalf("close refused API probe listener: %v", err)
	}
	refusedEndpoint := "http://" + refusedAddress + "/refused"
	err = client.PostJSON(context.Background(), refusedEndpoint, payload, "fixed-api-token", "protocol refused")
	refusedError := strings.ReplaceAll(errorString(err), refusedAddress, "{address}")
	outcomes = append(outcomes, protocolHTTPOutcome{Name: "connection_refused", Result: "", Error: refusedError})

	requestsMu.Lock()
	captured := append([]protocolHTTPRequest(nil), requests...)
	requestsMu.Unlock()
	writeProtocolTranscript(t, protocolHTTPTranscript{
		SchemaVersion: 1,
		Protocol:      "http_api",
		Requests:      captured,
		Outcomes:      outcomes,
	})
}

// errorString 保留 Go `error.Error()` 的原始文本；nil 映射为空字符串，与 Rust probe
// 的 Result 成功分支采用同一显式表示。
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// writeProtocolTranscript 把 probe 输出写到门禁提供的临时目录。输出路径必须显式
// 注入，防止测试误写 Go 工作区；JSON 使用紧凑编码并以换行结尾，便于独立审计。
func writeProtocolTranscript(t *testing.T, transcript protocolHTTPTranscript) {
	t.Helper()
	path := os.Getenv("PROTOCOL_TRANSCRIPT_OUTPUT")
	if path == "" {
		t.Fatal("PROTOCOL_TRANSCRIPT_OUTPUT is required")
	}
	encoded, err := json.Marshal(transcript)
	if err != nil {
		t.Fatalf("marshal protocol transcript: %v", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write protocol transcript: %v", err)
	}
}
