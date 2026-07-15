package input

// 本文件不会复制到 Go 工作树。Linux ARM64 门禁通过 `go test -overlay` 把它临时
// 映射到只读 Go 基线的 `internal/input` 包中。Probe 只连接进程内 loopback 后端，
// 不修改 oracle，也不需要 staging 凭据。

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/anthropics/anthropic/api-go/environment-manager/internal/api"
	"github.com/anthropics/anthropic/api-go/environment-manager/internal/auth"
	"github.com/anthropics/anthropic/api-go/environment-manager/internal/claude"
	"github.com/anthropics/anthropic/api-go/environment-manager/internal/config"
)

const (
	v0FullWire = `{"startup_context":{"api_base_url":"api.example.test/base","sources":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/demo","ref":"main","url":"https://github.example/anthropic/demo.git","host":"github.example","auth":{"type":"anthropic_api","token":"source-token"},"allow_unrestricted_git_push":true}}],"outcomes":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/demo","branches":["task-branch"]}},{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/skipped","branches":["one","two"]}},{"type":"other","git_info":{"type":"github","repo":"anthropic/ignored","branches":["ignored"]}}],"custom_system_prompt":"custom","append_system_prompt":"append","model":"claude-test","mcp_config":{"mcpServers":{"remote":{"type":"http","url":"http://localhost:4321/mcp","headers":{"X-Test":"one"},"tools":[{"name":"echo","enabled":true,"permission_policy":"allow"}]}}},"allowed_tools":["Read","Bash"],"disallowed_tools":["Write"],"enabled_tools":["Read"],"claude_code_args":{"model":"sonnet","verbose":"true"},"mcp_config_file":{"path":"/tmp/mcp.json","content":"eyJ6IjoxfQ==","mode":384},"use_sandbox_gateway_config":true,"entrypoint":"/bin/start","environment_variables":{"A":"B","EMPTY":""},"environment_sub_type":"baku","use_code_sessions":false},"environment":{"environment_type":"anthropic","cwd":"/workspace","allowed_tools":"Read","disallowed_tools":"Bash","unknown":{"nested":true}},"auth":[{"type":"session_ingress","token":"v0-ingress"},{"type":"anthropic_api","token":"api-token"},{"type":"anthropic_oauth","token":"oauth-token"},{"type":"vercel_deploy","token":"vercel-token"}]}`
	v0NullWire = `{"startup_context":{"api_base_url":"null.example.test","sources":null,"outcomes":null,"mcp_config":null,"allowed_tools":null,"disallowed_tools":null,"enabled_tools":null,"claude_code_args":null,"mcp_config_file":null,"environment_variables":null},"environment":{"environment_type":"byoc","cwd":null,"allowed_tools":null,"disallowed_tools":null,"unknown":null},"auth":null}`
	v0MissingAuthWire = `{"startup_context":{"api_base_url":"missing-auth.example.test","mcp_config_file":{"path":"/tmp/invalid-mcp.json","content":"%%%","mode":384}},"environment":{"environment_type":"byoc","cwd":"/fallback"}}`
	v0InvalidEnvironmentWire = `{"startup_context":{"api_base_url":"invalid-env.example.test"},"environment":null,"auth":null}`
	v0MissingStartupWire = `{}`
	v0NullStartupWire = `{"startup_context":null,"environment":{"environment_type":"byoc"},"auth":null}`

	v1CodeSessionSecret = `{"version":1,"session_ingress_token":"secret-ingress","api_base_url":"code.example.test","sources":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/code-session","ref":"feature","host":"github.com"}}],"auth":[{"type":"session_ingress","token":"old-ingress"},{"type":"anthropic_api","token":"code-api"}],"claude_code_args":{"model":"opus"},"mcp_config":{"path":"/tmp/code-mcp.json","content":"e30=","mode":384},"use_sandbox_gateway_config":true,"environment_variables":{"CODE":"1"},"use_code_sessions":true}`
	v1HealthcheckSecret = `{"version":1,"session_ingress_token":null,"api_base_url":"health.example.test","sources":null,"auth":null,"claude_code_args":null,"mcp_config":null,"use_sandbox_gateway_config":null,"environment_variables":null,"use_code_sessions":null}`
	v1BackendFullSecret = `{"version":1,"session_ingress_token":"backend-ingress","api_base_url":"http://127.0.0.1:41791","sources":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/backend","ref":null,"url":null,"host":"github.com","auth":null,"allow_unrestricted_git_push":null}}],"auth":[{"type":"session_ingress","token":"old-backend-ingress"},{"type":"anthropic_oauth","token":"backend-oauth"}],"claude_code_args":null,"mcp_config":null,"use_sandbox_gateway_config":false,"environment_variables":null,"use_code_sessions":false}`
	v1BackendZeroSecret = `{"version":1,"session_ingress_token":"backend-ingress","api_base_url":"http://127.0.0.1:41791","sources":null,"auth":null,"claude_code_args":null,"mcp_config":null,"environment_variables":null,"use_code_sessions":false}`
	inputBackendURL = "http://127.0.0.1:41791"
)

// protocolInputAttribute 保留 slog 参数的传入顺序；属性数组而非 map 能检测 Rust
// 是否在兼容层中重排了日志字段。
type protocolInputAttribute struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

// protocolInputLog 是一条去除事件时间与 source 后的 input/auth 阶段日志。原始
// parse_duration_ms 仍保留在属性数组中，只允许 comparator 按 manifest 审核规则处理。
type protocolInputLog struct {
	Level      string                   `json:"level"`
	Message    string                   `json:"message"`
	Attributes []protocolInputAttribute `json:"attributes"`
}

// protocolInputProviderRequest 记录 V1 parser 向 session-context provider 传递的稳定
// 参数。loopback 使用固定端口，token 与 session ID 均为测试假数据。
type protocolInputProviderRequest struct {
	APIBaseURL          string `json:"api_base_url"`
	SessionIngressToken string `json:"session_ingress_token"`
	SessionID            string `json:"session_id"`
}

// protocolInputStartupSummary 提取后续执行层可观察的 startup context 语义。nil slice
// 与空 slice 都按“零个条目”表达；可选 MCP 指针则显式保留 presence。
type protocolInputStartupSummary struct {
	Sources                 interface{} `json:"sources"`
	APIBaseURL              string      `json:"api_base_url"`
	OutcomeFields           interface{} `json:"outcome_fields"`
	CustomSystemPrompt      string      `json:"custom_system_prompt"`
	AppendSystemPrompt      string      `json:"append_system_prompt"`
	Model                   string      `json:"model"`
	McpConfigPresent        bool        `json:"mcp_config_present"`
	McpServers              interface{} `json:"mcp_servers"`
	McpExtra                interface{} `json:"mcp_extra"`
	AllowedTools            interface{} `json:"allowed_tools"`
	DisallowedTools         interface{} `json:"disallowed_tools"`
	EnabledTools            interface{} `json:"enabled_tools"`
	ClaudeCodeArgs          interface{} `json:"claude_code_args"`
	McpConfigFile           interface{} `json:"mcp_config_file"`
	UseSandboxGatewayConfig bool        `json:"use_sandbox_gateway_config"`
	Entrypoint              string      `json:"entrypoint"`
	EnvironmentVariables    interface{} `json:"environment_variables"`
	EnvironmentSubType      string      `json:"environment_sub_type"`
	UseCodeSessions         bool        `json:"use_code_sessions"`
}

// protocolInputEnvironmentSummary 保留环境类型、完整配置 object 和工具字符串。
type protocolInputEnvironmentSummary struct {
	EnvironmentType string      `json:"environment_type"`
	Config          interface{} `json:"config"`
	AllowedTools    string      `json:"allowed_tools"`
	DisallowedTools string      `json:"disallowed_tools"`
}

// protocolInputAuthSummary 逐项读取 AuthContext，防止 parser 仅接受 JSON 却丢失某个
// provider，或没有用 work secret 顶层 ingress token 覆盖 auth 数组旧值。
type protocolInputAuthSummary struct {
	SessionIngressToken     string `json:"session_ingress_token"`
	AnthropicAPIToken       string `json:"anthropic_api_token"`
	AnthropicOAuthToken     string `json:"anthropic_oauth_token"`
	VercelDeployToken       string `json:"vercel_deploy_token"`
	AntspaceControlPlaneURL string `json:"antspace_control_plane_url"`
	AntspaceAuthToken       string `json:"antspace_auth_token"`
	SupabaseProjectRef      string `json:"supabase_project_ref"`
	SupabaseAnonKey         string `json:"supabase_anon_key"`
	SupabaseDBPass          string `json:"supabase_db_pass"`
	SupabasePAT             string `json:"supabase_pat"`
	SessionID               string `json:"session_id"`
}

// protocolInputParsedSummary 是 V0/V1 成功结果的共同语义视图。
type protocolInputParsedSummary struct {
	Startup      *protocolInputStartupSummary     `json:"startup"`
	Environment  *protocolInputEnvironmentSummary `json:"environment"`
	Auth         *protocolInputAuthSummary        `json:"auth"`
	Outcomes     map[string][]string              `json:"outcomes"`
	SessionID    string                           `json:"session_id"`
	WorkID       string                           `json:"work_id"`
	EnvironmentID string                          `json:"environment_id"`
	IsHealthcheck bool                            `json:"is_healthcheck"`
}

// protocolInputCase 固定一份原始输入模板、解析结果、provider 调用和 logger transcript。
type protocolInputCase struct {
	Name                string                        `json:"name"`
	InputWireBase64     string                        `json:"input_wire_base64"`
	Result              *protocolInputParsedSummary   `json:"result"`
	Error               string                        `json:"error"`
	Panic               string                        `json:"panic"`
	ProviderRequest     *protocolInputProviderRequest `json:"provider_request"`
	Logs                []protocolInputLog            `json:"logs"`
}

// protocolInputTranscript 是第六类严格差分文档。
type protocolInputTranscript struct {
	SchemaVersion int                 `json:"schema_version"`
	Protocol      string              `json:"protocol"`
	Cases         []protocolInputCase `json:"cases"`
}

// inputLogCapture 实现 slog.Handler，并只保留 input parser 及其同步 auth 配置日志；
// SessionsClient 自身的 HTTP 日志属于已有 http_api 差分，不在这里重复比较。
type inputLogCapture struct {
	mu             *sync.Mutex
	records        *[]protocolInputLog
	boundAttrs     []slog.Attr
	group          string
}

// Enabled 为 probe 打开全部级别，确保 DEBUG 阶段不会因测试 logger 默认级别消失。
func (h *inputLogCapture) Enabled(context.Context, slog.Level) bool { return true }

// Handle 把一条允许的 input/auth 日志转换成稳定、保持属性顺序的 transcript 记录。
func (h *inputLogCapture) Handle(_ context.Context, record slog.Record) error {
	if !isInputParserLog(record.Message) {
		return nil
	}
	attributes := make([]protocolInputAttribute, 0, len(h.boundAttrs)+record.NumAttrs())
	appendAttr := func(attribute slog.Attr) bool {
		attribute.Value = attribute.Value.Resolve()
		value := slogValue(attribute.Value)
		name := attribute.Key
		if h.group != "" {
			name = h.group + "." + name
		}
		attributes = append(attributes, protocolInputAttribute{Name: name, Value: value})
		return true
	}
	for _, attribute := range h.boundAttrs {
		appendAttr(attribute)
	}
	record.Attrs(appendAttr)
	h.mu.Lock()
	*h.records = append(*h.records, protocolInputLog{
		Level:      record.Level.String(),
		Message:    record.Message,
		Attributes: attributes,
	})
	h.mu.Unlock()
	return nil
}

// WithAttrs 派生 handler 时复制属性，避免后续 append 改写父 logger 的 backing array。
func (h *inputLogCapture) WithAttrs(attributes []slog.Attr) slog.Handler {
	bound := append([]slog.Attr(nil), h.boundAttrs...)
	bound = append(bound, attributes...)
	clone := *h
	clone.boundAttrs = bound
	return &clone
}

// WithGroup 保留组名；input/auth 当前未使用 group，但 probe 不能通过丢弃它制造假象。
func (h *inputLogCapture) WithGroup(name string) slog.Handler {
	clone := *h
	if clone.group == "" {
		clone.group = name
	} else {
		clone.group += "." + name
	}
	return &clone
}

// slogValue 将 slog 基础类型转换为 encoding/json 可稳定编码的值。
func slogValue(value slog.Value) interface{} {
	switch value.Kind() {
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindString:
		return value.String()
	case slog.KindTime:
		return value.Time().Format("2006-01-02T15:04:05.999999999Z07:00")
	case slog.KindUint64:
		return value.Uint64()
	case slog.KindGroup:
		group := make(map[string]interface{})
		for _, attribute := range value.Group() {
			group[attribute.Key] = slogValue(attribute.Value.Resolve())
		}
		return group
	default:
		return value.Any()
	}
}

// isInputParserLog 明确列出本 probe 覆盖的业务阶段，防止 API transport 日志随实现
// 细节混入 parser transcript。
func isInputParserLog(message string) bool {
	switch message {
	case "Parsing V0 input format",
		"Parsed startup context from V0 input",
		"Received pre-computed args from sandbox-gateway",
		"Pre-computed MCP config file",
		"Parsed environment from V0 input",
		"Configured session ingress token",
		"Configured Anthropic API token",
		"Configured Anthropic OAuth token",
		"Configured Vercel deploy token",
		"Configured Antspace deploy credentials",
		"Configured Supabase credentials",
		"Parsed auth from V0 input",
		"Outcome skipped: exactly one branch must be specified",
		"Parsed outcomes from V0 input",
		"Repository outcome",
		"Completed parsing V0 input",
		"Parsing V1 input format",
		"Parsed V1 work response",
		"Processing healthcheck work type",
		"Completed parsing V1 healthcheck input",
		"Decoded work secret",
		"Parsed outcomes from V1 session context",
		"Completed parsing V1 input":
		return true
	default:
		return false
	}
}

// newInputLogger 为一个 case 创建隔离的记录器，并返回可在解析结束后读取的记录 slice。
func newInputLogger() (*slog.Logger, *[]protocolInputLog, *sync.Mutex) {
	records := &[]protocolInputLog{}
	mu := &sync.Mutex{}
	return slog.New(&inputLogCapture{mu: mu, records: records}), records, mu
}

// TestProtocolInputDifferentialTranscript 运行确定性的 V0/V1 成功、null/missing、Base64、
// session context、MCP、auth、environment 和 outcome 场景。
func TestProtocolInputDifferentialTranscript(t *testing.T) {
	backend := newProtocolInputBackend(t)
	defer backend.Close()

	cases := []protocolInputCase{
		runV0InputCase("v0_full", v0FullWire),
		runV0InputCase("v0_null_optional", v0NullWire),
		runV0InputCase("v0_missing_auth_invalid_mcp_base64", v0MissingAuthWire),
		runV0InputCase("v0_invalid_environment", v0InvalidEnvironmentWire),
		runV0InputCase("v0_missing_startup_context", v0MissingStartupWire),
		runV0InputCase("v0_null_startup_context", v0NullStartupWire),
	}

	codeWire := makeV1Wire("work-code", "env-code", "running", "session", "session-code", encodeWorkSecret(v1CodeSessionSecret))
	cases = append(cases, runV1InputCase("v1_code_session_full", codeWire, backend))
	healthWire := makeV1Wire("work-health", "env-health", "ready", "healthcheck", "", encodeWorkSecret(v1HealthcheckSecret))
	cases = append(cases, runV1InputCase("v1_healthcheck_null_optional", healthWire, backend))

	fullWire := makeV1Wire("work-backend-full", "env-backend", "running", "session", "session-full", encodeWorkSecret(v1BackendFullSecret))
	cases = append(cases, runV1InputCase("v1_session_context_full", fullWire, backend))

	missingWire := makeV1Wire("work-backend-missing", "env-backend", "running", "session", "session-missing", encodeWorkSecret(v1BackendZeroSecret))
	cases = append(cases, runV1InputCase("v1_session_context_missing", missingWire, backend))

	nullWire := makeV1Wire("work-backend-null", "env-backend", "running", "session", "session-null", encodeWorkSecret(v1BackendZeroSecret))
	cases = append(cases, runV1InputCase("v1_session_context_null", nullWire, backend))

	invalidBase64Wire := makeV1Wire("work-invalid-secret", "env-invalid", "running", "session", "session-invalid", "not_base64")
	cases = append(cases, runV1InputCase("v1_invalid_base64", invalidBase64Wire, backend))

	writeInputProtocolTranscript(t, protocolInputTranscript{SchemaVersion: 1, Protocol: "input", Cases: cases})
}

// runV0InputCase 通过真实 V0Parser 运行一条 wire，并捕获完整成功结果或原始错误文本。
func runV0InputCase(name, wire string) (result protocolInputCase) {
	logger, records, recordsMu := newInputLogger()
	result = protocolInputCase{
		Name:                name,
		InputWireBase64: base64.StdEncoding.EncodeToString([]byte(wire)),
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Panic = fmt.Sprint(recovered)
		}
		recordsMu.Lock()
		result.Logs = append([]protocolInputLog(nil), (*records)...)
		recordsMu.Unlock()
	}()
	parser := &V0Parser{Logger: logger, SessionID: "session-v0"}
	parsed, err := parser.Parse(context.Background(), []byte(wire))
	result.Result = summarizeParsedContext(parsed)
	result.Error = inputErrorString(err)
	return result
}

// runV1InputCase 通过真实 V1Parser 运行一条 work wire，并取回 mock 后端观察到的 provider 参数。
func runV1InputCase(name, wire string, backend *protocolInputBackend) protocolInputCase {
	logger, records, recordsMu := newInputLogger()
	parser := &V1Parser{Logger: logger, WorkingDirectory: "/fallback/workspace"}
	parsed, err := parser.Parse(context.Background(), []byte(wire))
	recordsMu.Lock()
	logs := append([]protocolInputLog(nil), (*records)...)
	recordsMu.Unlock()
	return protocolInputCase{
		Name:                name,
		InputWireBase64: base64.StdEncoding.EncodeToString([]byte(wire)),
		Result:              summarizeParsedContext(parsed),
		Error:               inputErrorString(err),
		ProviderRequest:     backend.takeRequest(parsedSessionID(parsed)),
		Logs:                logs,
	}
}

// parsedSessionID 在失败或 healthcheck 时返回空串，使后端记录查找保持无副作用。
func parsedSessionID(parsed *ParsedContext) string {
	if parsed == nil {
		return ""
	}
	return parsed.SessionID
}

// encodeWorkSecret 使用 Go 生产 DecodeWorkSecret 对应的 padded URL-safe Base64 编码。
func encodeWorkSecret(secret string) string {
	return base64.URLEncoding.EncodeToString([]byte(secret))
}

// makeV1Wire 用固定字段顺序构造 work envelope；fixture 字符串不含需额外 JSON escaping 的字符。
func makeV1Wire(workID, environmentID, state, dataType, sessionID, secret string) string {
	return fmt.Sprintf(`{"id":"%s","type":"work","environment_id":"%s","state":"%s","data":{"type":"%s","id":"%s"},"secret":"%s","created_at":"2026-01-02T03:04:05Z"}`,
		workID, environmentID, state, dataType, sessionID, secret)
}

// summarizeParsedContext 把成功结果转换为语言无关、可逐字段比较的语义视图。
func summarizeParsedContext(parsed *ParsedContext) *protocolInputParsedSummary {
	if parsed == nil {
		return nil
	}
	return &protocolInputParsedSummary{
		Startup:       summarizeStartupContext(parsed.StartupContext),
		Environment:   summarizeEnvironment(parsed.Environment),
		Auth:          summarizeAuth(parsed.AuthContext),
		Outcomes:      summarizeOutcomes(parsed.Outcomes),
		SessionID:     parsed.SessionID,
		WorkID:        parsed.WorkID,
		EnvironmentID: parsed.EnvironmentID,
		IsHealthcheck: parsed.IsHealthcheck,
	}
}

// summarizeStartupContext 展开后续执行层读取的全部 startup 字段。
func summarizeStartupContext(startup *config.StartupContext) *protocolInputStartupSummary {
	if startup == nil {
		return nil
	}
	mcpServers := interface{}(map[string]interface{}{})
	mcpExtra := interface{}(map[string]interface{}{})
	if startup.McpConfig != nil {
		mcpServers = jsonObjectOrEmpty(startup.McpConfig.McpServers)
		mcpExtra = jsonObjectOrEmpty(startup.McpConfig.Extra)
	}
	return &protocolInputStartupSummary{
		Sources:                 jsonArrayOrEmpty(startup.Sources),
		APIBaseURL:              startup.APIBaseURL,
		OutcomeFields:           jsonArrayOrEmpty(startup.Outcomes),
		CustomSystemPrompt:      startup.CustomSystemPrompt,
		AppendSystemPrompt:      startup.AppendSystemPrompt,
		Model:                   startup.Model,
		McpConfigPresent:        startup.McpConfig != nil,
		McpServers:              mcpServers,
		McpExtra:                mcpExtra,
		AllowedTools:            jsonArrayOrEmpty(startup.AllowedTools),
		DisallowedTools:         jsonArrayOrEmpty(startup.DisallowedTools),
		EnabledTools:            jsonArrayOrEmpty(startup.EnabledTools),
		ClaudeCodeArgs:          jsonObjectOrEmpty(startup.ClaudeCodeArgs),
		McpConfigFile:           jsonValue(startup.McpConfigFile),
		UseSandboxGatewayConfig: startup.UseSandboxGatewayConfig,
		Entrypoint:              startup.Entrypoint,
		EnvironmentVariables:    jsonObjectOrEmpty(startup.EnvironmentVariables),
		EnvironmentSubType:      startup.EnvironmentSubType,
		UseCodeSessions:         startup.UseCodeSessions,
	}
}

// summarizeEnvironment 解码 RawMessage 为 JSON 值，使 object 仅作为语义容器比较，
// 同时保留字段值和 null；原始 V0 wire 已由 input_template_base64 单独逐字节固定。
func summarizeEnvironment(environment *api.GetEnvironmentConfigResponse) *protocolInputEnvironmentSummary {
	if environment == nil {
		return nil
	}
	var configValue interface{}
	if len(environment.Config) != 0 {
		if err := json.Unmarshal(environment.Config, &configValue); err != nil {
			panic(err)
		}
	}
	return &protocolInputEnvironmentSummary{
		EnvironmentType: environment.EnvironmentType,
		Config:          configValue,
		AllowedTools:    environment.AllowedTools,
		DisallowedTools: environment.DisallowedTools,
	}
}

// summarizeAuth 读取所有 provider 字段；fixture token 均为固定假值，不含真实凭据。
func summarizeAuth(authContext *auth.AuthContext) *protocolInputAuthSummary {
	if authContext == nil {
		return nil
	}
	return &protocolInputAuthSummary{
		SessionIngressToken:     authContext.GetSessionIngressToken(),
		AnthropicAPIToken:       authContext.GetAnthropicAPIToken(),
		AnthropicOAuthToken:     authContext.GetAnthropicOAuthToken(),
		VercelDeployToken:       authContext.GetVercelDeployToken(),
		AntspaceControlPlaneURL: authContext.GetAntspaceControlPlaneURL(),
		AntspaceAuthToken:       authContext.GetAntspaceAuthToken(),
		SupabaseProjectRef:      authContext.GetSupabaseProjectRef(),
		SupabaseAnonKey:         authContext.GetSupabaseAnonKey(),
		SupabaseDBPass:          authContext.GetSupabaseDBPass(),
		SupabasePAT:             authContext.GetSupabasePAT(),
		SessionID:               authContext.GetSessionID(),
	}
}

// summarizeOutcomes 返回按仓库名稳定排序的 map；JSON object 的键顺序不属于 wire，
// 但排序可使人工查看生成文件时保持稳定。
func summarizeOutcomes(outcomes *claude.Outcomes) map[string][]string {
	result := make(map[string][]string)
	if outcomes == nil {
		return result
	}
	for repository, branches := range outcomes.AsMap() {
		result[repository] = append([]string(nil), branches...)
	}
	return result
}

// jsonValue 通过生产 encoding/json 规则把具体 DTO 转成 transcript 可编码值。
func jsonValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var decoded interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		panic(err)
	}
	return decoded
}

// jsonArrayOrEmpty 将 nil slice 的运行时零条目语义明确写为 []。
func jsonArrayOrEmpty(value interface{}) interface{} {
	decoded := jsonValue(value)
	if decoded == nil {
		return []interface{}{}
	}
	return decoded
}

// jsonObjectOrEmpty 将 nil map 的运行时零条目语义明确写为 {}。
func jsonObjectOrEmpty(value interface{}) interface{} {
	decoded := jsonValue(value)
	if decoded == nil {
		return map[string]interface{}{}
	}
	return decoded
}

// inputErrorString 把 nil/非 nil 错误统一成空串或原始 Error 文本。
func inputErrorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// protocolInputBackend 是 V1 非 code-session 场景使用的进程内 Sessions API。
type protocolInputBackend struct {
	*httptest.Server
	mu       sync.Mutex
	requests map[string]*protocolInputProviderRequest
}

// newProtocolInputBackend 为三个 session ID 返回 full、missing 和 explicit-null context。
func newProtocolInputBackend(t *testing.T) *protocolInputBackend {
	t.Helper()
	backend := &protocolInputBackend{requests: make(map[string]*protocolInputProviderRequest)}
	listener, err := net.Listen("tcp", "127.0.0.1:41791")
	if err != nil {
		t.Fatalf("listen fixed input probe backend: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		sessionID := strings.TrimPrefix(request.URL.Path, "/v2/sessions/")
		backend.mu.Lock()
		backend.requests[sessionID] = &protocolInputProviderRequest{
			APIBaseURL:          inputBackendURL,
			SessionIngressToken: strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "),
			SessionID:            sessionID,
		}
		backend.mu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		switch sessionID {
		case "session-full":
			_, _ = response.Write([]byte(`{"session_context":{"cwd":"/session/workspace","outcomes":[{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/backend","branches":["backend-branch"]}},{"type":"git_repository","git_info":{"type":"github","repo":"anthropic/backend-skipped","branches":["one","two"]}}],"custom_system_prompt":"session-custom","append_system_prompt":"session-append","model":"claude-session","mcp_config":{"mcpServers":{"session":{"type":"http","url":"http://localhost:7654/mcp","headers":{"X-Session":"yes"}}}}}}`))
		case "session-missing":
			_, _ = response.Write([]byte(`{"session_context":{}}`))
		case "session-null":
			_, _ = response.Write([]byte(`{"session_context":{"cwd":null,"outcomes":null,"custom_system_prompt":null,"append_system_prompt":null,"model":null,"mcp_config":null}}`))
		default:
			http.NotFound(response, request)
		}
	}))
	server.Listener = listener
	server.Start()
	backend.Server = server
	if backend.URL != inputBackendURL {
		t.Fatalf("fixed input backend URL = %q, want %q", backend.URL, inputBackendURL)
	}
	return backend
}

// takeRequest 取得指定 session 的 provider 调用；没有调用时返回 nil。
func (b *protocolInputBackend) takeRequest(sessionID string) *protocolInputProviderRequest {
	if sessionID == "" {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	request := b.requests[sessionID]
	delete(b.requests, sessionID)
	return request
}

// writeInputProtocolTranscript 将紧凑 JSON 和单个换行写到门禁指定路径。
func writeInputProtocolTranscript(t *testing.T, transcript protocolInputTranscript) {
	t.Helper()
	path := os.Getenv("PROTOCOL_TRANSCRIPT_OUTPUT")
	if path == "" {
		t.Fatal("PROTOCOL_TRANSCRIPT_OUTPUT is required")
	}
	encoded, err := json.Marshal(transcript)
	if err != nil {
		t.Fatalf("marshal input transcript: %v", err)
	}
	encoded = append(encoded, '\n')
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write input transcript: %v", err)
	}
}
