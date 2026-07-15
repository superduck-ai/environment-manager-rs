// logger-source-probe 在固定 Go 1.25.7 -trimpath Linux ARM64 构建中触发
// LogWriter 的两个生产日志调用点，并只输出可逐字节比较的 source 摘要。
//
// runner 会把本文件复制到 Go module 的临时副本；只读 Go oracle 从不被修改。
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"runtime"
	"strings"

	projectlogger "github.com/anthropics/anthropic/api-go/environment-manager/internal/logger"
)

type source struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

type transcriptEntry struct {
	Case   string `json:"case"`
	Source source `json:"source"`
}

type sourceHandler struct{}

func (sourceHandler) Enabled(context.Context, slog.Level) bool { return true }

func (sourceHandler) Handle(_ context.Context, record slog.Record) error {
	frame, _ := runtime.CallersFrames([]uintptr{record.PC}).Next()
	caseName := record.Message
	if strings.HasSuffix(record.Message, " [line truncated - no newline]") {
		caseName = "truncated"
	}
	return json.NewEncoder(os.Stdout).Encode(transcriptEntry{
		Case: caseName,
		Source: source{
			Function: frame.Function,
			File:     frame.File,
			Line:     frame.Line,
		},
	})
}

func (handler sourceHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }

func (handler sourceHandler) WithGroup(string) slog.Handler { return handler }

func main() {
	logger := slog.New(sourceHandler{})
	complete := projectlogger.NewLogWriter(
		logger,
		context.Background(),
		slog.LevelInfo,
		"",
	)
	if _, err := complete.Write([]byte("complete\n")); err != nil {
		panic(err)
	}

	truncated := projectlogger.NewLogWriter(
		logger,
		context.Background(),
		slog.LevelWarn,
		"",
	)
	payload := make([]byte, (1<<20)+1)
	for index := range payload {
		payload[index] = 'x'
	}
	if _, err := truncated.Write(payload); err != nil {
		panic(err)
	}
}
