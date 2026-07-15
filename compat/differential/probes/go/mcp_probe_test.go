package mcp

// Linux ARM64 门禁通过 Go overlay 临时把本文件放入只读 `internal/mcp` 包。Probe
// 启动真实的 mcp-go Streamable HTTP server，所有请求都走 loopback，不需要 staging。

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// protocolMCPCase 同时保存请求和原始响应 body。JSON-RPC body 不做反序列化或字段
// 重排，因此字段顺序、omitempty、错误文本以及结尾换行都属于差分范围。
type protocolMCPCase struct {
	Name                string `json:"name"`
	Authorized          bool   `json:"authorized"`
	RequestBody         string `json:"request_body"`
	Status              int    `json:"status"`
	ResponseContentType string `json:"response_content_type"`
	ResponseBody        string `json:"response_body"`
}

// protocolMCPTranscript 是 MCP probe 的稳定文档格式。
type protocolMCPTranscript struct {
	SchemaVersion int               `json:"schema_version"`
	Protocol      string            `json:"protocol"`
	Endpoint      string            `json:"endpoint"`
	BearerToken   string            `json:"bearer_token"`
	Cases         []protocolMCPCase `json:"cases"`
}

// TestProtocolDifferentialTranscript 覆盖 Bearer 拒绝、initialize、notification、
// tools/list、tools/call、未知工具、未知方法和 JSON parse error。固定 debug token 让
// token 本身也能精确比较；内核分配的端口只用于发请求，不进入 transcript。
func TestProtocolDifferentialTranscript(t *testing.T) {
	if !debugHardcodedToken {
		t.Fatal("MCP_DEBUG_TOKEN must be set before the Go test process starts")
	}

	server := &BaseServer{
		Name:        "protocol-probe",
		Version:     "1.2.3",
		BearerToken: "fixed-mcp-token",
	}
	config, err := server.Start([]ToolConfig{{
		Tool: mcpgo.Tool{
			Name:        "echo",
			Description: "Return a deterministic value",
			InputSchema: mcpgo.ToolInputSchema{
				Type: "object",
				Properties: map[string]interface{}{
					"message": map[string]interface{}{"type": "string"},
				},
				Required: []string{"message"},
			},
		},
		Handler: func(_ context.Context, request mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
			return mcpgo.NewToolResultText("echo:" + request.GetString("message", "")), nil
		},
	}})
	if err != nil {
		t.Fatalf("start MCP probe server: %v", err)
	}
	defer server.Stop()
	if config.BearerToken != "fixed-mcp-token" {
		t.Fatalf("debug bearer token changed: %q", config.BearerToken)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	endpoint := "http://127.0.0.1:" + jsonNumber(config.Port) + "/mcp"
	requests := []struct {
		name       string
		authorized bool
		body       string
	}{
		{
			name:       "unauthorized",
			authorized: false,
			body:       `{"jsonrpc":"2.0","id":0,"method":"tools/list"}`,
		},
		{
			name:       "initialize",
			authorized: true,
			body:       `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"1"}}}`,
		},
		{
			name:       "initialized_notification",
			authorized: true,
			body:       `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		},
		{
			name:       "tools_list",
			authorized: true,
			body:       `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		},
		{
			name:       "tools_call",
			authorized: true,
			body:       `{"jsonrpc":"2.0","id":"call-1","method":"tools/call","params":{"name":"echo","arguments":{"message":"hello"},"futureField":"kept"},"futureTopLevel":true}`,
		},
		{
			name:       "tool_not_found",
			authorized: true,
			body:       `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"missing","arguments":{}}}`,
		},
		{
			name:       "method_not_found",
			authorized: true,
			body:       `{"jsonrpc":"2.0","id":4,"method":"future/method"}`,
		},
		{
			name:       "parse_error",
			authorized: true,
			body:       `{"jsonrpc":"2.0",`,
		},
	}

	cases := make([]protocolMCPCase, 0, len(requests))
	for _, request := range requests {
		cases = append(cases, performMCPProbeRequest(t, client, endpoint, config.BearerToken, request.name, request.authorized, request.body))
	}

	writeMCPProtocolTranscript(t, protocolMCPTranscript{
		SchemaVersion: 1,
		Protocol:      "mcp",
		Endpoint:      "/mcp",
		BearerToken:   config.BearerToken,
		Cases:         cases,
	})
}

// performMCPProbeRequest 发出一条 MCP HTTP POST，并无损读取响应 body。请求固定声明
// JSON 和 event-stream 两种 Accept，兼容 mcp-go Streamable HTTP 的协商规则。
func performMCPProbeRequest(t *testing.T, client *http.Client, endpoint string, token string, name string, authorized bool, body string) protocolMCPCase {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("%s: construct request: %v", name, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if authorized {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s: execute request: %v", name, err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s: read response: %v", name, err)
	}
	return protocolMCPCase{
		Name:                name,
		Authorized:          authorized,
		RequestBody:         body,
		Status:              resp.StatusCode,
		ResponseContentType: resp.Header.Get("Content-Type"),
		ResponseBody:        string(responseBody),
	}
}

// jsonNumber 避免 fmt 对 probe 只用于端口拼接这一处增加额外依赖。
func jsonNumber(value int) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// writeMCPProtocolTranscript 只允许写门禁注入的临时路径，绝不修改 Go oracle。
func writeMCPProtocolTranscript(t *testing.T, transcript protocolMCPTranscript) {
	t.Helper()
	path := os.Getenv("PROTOCOL_TRANSCRIPT_OUTPUT")
	if path == "" {
		t.Fatal("PROTOCOL_TRANSCRIPT_OUTPUT is required")
	}
	encoded, err := json.Marshal(transcript)
	if err != nil {
		t.Fatalf("marshal MCP transcript: %v", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write MCP transcript: %v", err)
	}
}
