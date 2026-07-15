// logger_source_catalog_probe.go 只会复制到固定 Go 工作树的临时只读 backing 副本。
// 它与原 main.go 一起构建，使 runtime 能用构建产物自身的 pclntab 精确解析任意
// slog CALL 的返回 PC；原 Go 工作树不会被写入或纳入新文件。
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	environmentsources "github.com/anthropics/anthropic/api-go/environment-manager/internal/sources"
)

func init() {
	// 主 CLI 当前不会触达 UpdateRemoteURLs，release linker 因而会删除其四个日志点。
	// catalog probe 显式保活 method expression，仅用于让固定编译器生成 pclntab 证据；
	// 不构造 manager、不执行网络/文件副作用，也不改变 Go 行为 oracle 二进制。
	if os.Getenv("LOGGER_SOURCE_FORCE_UPDATE_REMOTE_URLS") == "1" {
		var manager *environmentsources.SourceHandlerManager
		_ = manager.UpdateRemoteURLs(context.Background(), nil)
	}
	if os.Getenv("LOGGER_SOURCE_PC_PROBE") != "1" {
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		raw := strings.TrimSpace(scanner.Text())
		pc, err := strconv.ParseUint(strings.TrimPrefix(raw, "0x"), 16, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid PC %q: %v\n", raw, err)
			os.Exit(2)
		}

		// ARM64 的 CALL 指令固定为 4 字节。CallersFrames 接收的是 runtime.Callers
		// 返回的 return PC，因此必须传入 CALL 后一条指令，而不是 CALL 自身地址。
		frames := runtime.CallersFrames([]uintptr{uintptr(pc + 4)})
		index := 0
		for {
			frame, more := frames.Next()
			fmt.Printf("0x%x\t%d\t%s\t%s\t%d\n", pc, index, frame.Function, frame.File, frame.Line)
			index++
			if !more {
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read PCs: %v\n", err)
		os.Exit(2)
	}
	os.Exit(0)
}
