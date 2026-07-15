// logger-probe 固化 Go 1.25.7 slog 与恢复后文件 handler 的可观察 wire 行为。
//
// 本文件只会复制到 Linux ARM64 上的临时 Go 基线副本中运行；它不是 Rust
// 发布产物的一部分，也绝不会写回只读 Go 工作树。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	projectlogger "github.com/anthropics/anthropic/api-go/environment-manager/internal/logger"
)

// emitFromNamedHelper 验证 slog 记录的 PC 指向业务 helper，而不是 Logger facade。
func emitFromNamedHelper(logger *slog.Logger) {
	logger.Error("named helper", "helper_attr", 7)
}

func main() {
	path := os.Getenv("LOGGER_PROBE_PATH")
	if path == "" {
		panic("LOGGER_PROBE_PATH is required")
	}

	inner := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})
	handler, err := projectlogger.NewFileLoggingHandler(inner, path, slog.LevelDebug)
	if err != nil {
		panic(err)
	}
	logger := slog.New(handler)

	logger.InfoContext(
		context.Background(),
		"html <>& \u2028 \u2029",
		"z", "last <",
		"a", "first &",
	)
	logger.With("bound_z", 2, "bound_a", 1).WithGroup("group").Warn(
		"grouped >",
		"record_z", true,
		"record_a", false,
	)
	emitFromNamedHelper(logger)

	firstClose := handler.Close()
	secondClose := handler.Close()
	fmt.Printf("first_close=%v\nsecond_close=%v\n", firstClose, secondClose)

	// 独立记录固定时间，便于检查 time 字段的 RFC3339Nano 裁剪和 file map 排序。
	fixedPath := path + ".fixed"
	fixed, err := projectlogger.NewFileLoggingHandler(
		slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{AddSource: true, Level: slog.LevelDebug}),
		fixedPath,
		slog.LevelDebug,
	)
	if err != nil {
		panic(err)
	}
	record := slog.NewRecord(time.Date(2026, 7, 14, 12, 34, 56, 120000000, time.UTC), slog.LevelInfo, "fixed <>&", 0)
	record.AddAttrs(slog.String("z", "last"), slog.String("a", "first"))
	if err := fixed.Handle(context.Background(), record); err != nil {
		panic(err)
	}
	if err := fixed.Close(); err != nil {
		panic(err)
	}
}
