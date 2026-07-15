// process-edge 是 Go 1.25.7 Linux/ARM64 的低层进程兼容探针。
//
// 它只记录迁移需要复刻、而标准库文档没有完全承诺的可观察行为：LookPath/ErrDot、
// 非 UTF-8 路径错误字节、CombinedOutput 原始字节、direct child 的回收顺序，以及
// ExtraFiles 从 FD 3 开始的继承和 CLOEXEC 语义。该程序不是产品代码。
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// commandResult 保存异步 CombinedOutput 的单次返回值。
type commandResult struct {
	output []byte
	err    error
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "read-fds" {
		readInheritedFDs()
		return
	}

	fmt.Printf("go.version=%s\n", runtimeVersion())
	root, err := os.MkdirTemp("", "process-edge-")
	must(err)
	defer os.RemoveAll(root)
	// normalizer 只会替换这一行声明的精确临时根字节；它不会按 `/tmp/.*` 模糊
	// 擦除错误文本，因此路径后缀、非法字节和 errno 仍参加逐字节比较。
	fmt.Printf("dynamic.temp-root.hex=%x\n", []byte(root))

	probeLookPath(root)
	probeRawCombinedOutput()
	probeReaderFirstReap(root)
	probeCombinedOutputReap(root)
	probeCancelledReaderFirst(root)
	probeCancelledCombinedOutput(root)
	probeExtraFiles()
}

// runtimeVersion 通过 go env 固定容器内实际执行的工具链版本，避免探针结果误挂名。
func runtimeVersion() string {
	out, err := exec.Command("go", "env", "GOVERSION").Output()
	must(err)
	return strings.TrimSpace(string(out))
}

// probeLookPath 记录查找优先级、ErrDot 开关和任意 Unix 路径字节的错误表示。
func probeLookPath(root string) {
	originalPath, hadPath := os.LookupEnv("PATH")
	originalDebug, hadDebug := os.LookupEnv("GODEBUG")
	originalWorkingDirectory, err := os.Getwd()
	must(err)
	defer func() {
		restoreEnvironment("PATH", originalPath, hadPath)
		restoreEnvironment("GODEBUG", originalDebug, hadDebug)
		must(os.Chdir(originalWorkingDirectory))
	}()

	bin := filepath.Join(root, "bin")
	must(os.Mkdir(bin, 0o755))
	must(os.WriteFile(filepath.Join(bin, "nonexec"), []byte("#!/bin/sh\n"), 0o644))
	must(os.Setenv("PATH", bin))
	path, err := exec.LookPath("missing")
	printPathError("lookpath.missing", path, err)
	path, err = exec.LookPath("nonexec")
	printPathError("lookpath.nonexec", path, err)

	relative := filepath.Join(root, "relative")
	must(os.Mkdir(relative, 0o755))
	must(os.WriteFile(filepath.Join(relative, "relative-bin"), []byte("#!/bin/sh\nexit 0\n"), 0o755))
	must(os.Chdir(relative))
	must(os.Setenv("PATH", ":"))
	must(os.Unsetenv("GODEBUG"))
	path, err = exec.LookPath("relative-bin")
	printPathError("lookpath.errdot", path, err)
	must(os.Setenv("GODEBUG", "execerrdot=1,execerrdot=0"))
	path, err = exec.LookPath("relative-bin")
	printPathError("lookpath.errdot-last-zero", path, err)
	must(os.Setenv("GODEBUG", "execerrdot=0,execerrdot=1"))
	path, err = exec.LookPath("relative-bin")
	printPathError("lookpath.errdot-last-one", path, err)

	// Unix 文件名可包含任意非 NUL 字节；Go 的 PATH 搜索不能先转成 UTF-8。
	nonUTF8Directory := filepath.Join(root, string([]byte{'b', 'i', 'n', '-', 0xff}))
	must(os.Mkdir(nonUTF8Directory, 0o755))
	must(os.WriteFile(filepath.Join(nonUTF8Directory, "raw-bin"), []byte("#!/bin/sh\nexit 0\n"), 0o755))
	must(os.Setenv("PATH", nonUTF8Directory))
	path, err = exec.LookPath("raw-bin")
	printPathError("lookpath.nonutf8-directory", path, err)
	path, err = exec.LookPath(string([]byte{'r', 'a', 'w', '-', 0xff}))
	printPathError("lookpath.nonutf8-name", path, err)

	// 显式 slash 绕过 LookPath；下面直接观察 PathError.Error() 是否保留原始路径字节。
	rawMissing := filepath.Join(root, string([]byte{'m', 'i', 's', 's', '-', 0xff}))
	rawDenied := filepath.Join(root, string([]byte{'d', 'e', 'n', 'i', 'e', 'd', '-', 0xfe}))
	must(os.WriteFile(rawDenied, []byte("#!/bin/sh\nexit 0\n"), 0o644))
	printRunError("run.nonutf8-enoent", exec.Command(rawMissing).Run())
	printRunError("run.nonutf8-eacces", exec.Command(rawDenied).Run())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	printRunError("run.precancel-nonutf8-enoent", exec.CommandContext(ctx, rawMissing).Run())
	printRunError("run.precancel-missing-basename", exec.CommandContext(ctx, "missing-basename").Run())
}

// probeRawCombinedOutput 验证 Go error/string 可以携带非法 UTF-8 和 NUL 字节。
func probeRawCombinedOutput() {
	output, err := exec.Command("sh", "-c", "printf '\\377\\376\\000'; exit 5").CombinedOutput()
	wrapped := fmt.Errorf("npm install failed: %w, output: %s", err, string(output))
	fmt.Printf("combined.output.hex=%x\n", output)
	fmt.Printf("combined.error.bytes=%x\n", []byte(err.Error()))
	fmt.Printf("combined.wrapped.bytes=%x\n", []byte(wrapped.Error()))
}

// probeReaderFirstReap 复刻 internal/process.Execute 的“先等 reader，再 Wait”顺序。
func probeReaderFirstReap(root string) {
	directFile := filepath.Join(root, "reader-first-direct")
	holderFile := filepath.Join(root, "reader-first-holder")
	cmd := exec.Command("sh", "-c", "echo $$ > \"$1\"; sleep 30 & echo $! > \"$2\"; exit 0", "probe", directFile, holderFile)
	stdout, err := cmd.StdoutPipe()
	must(err)
	stderr, err := cmd.StderrPipe()
	must(err)
	must(cmd.Start())
	readersDone := copyBoth(stdout, stderr)
	direct := waitPIDFile(directFile)
	holder := waitPIDFile(holderFile)
	state := waitProcessState(direct, "Z", 3*time.Second)
	fmt.Printf("reader-first.before-release.state=%s readers-done=%t holder-alive=%t\n", state, channelReady(readersDone), processExists(holder))
	kill(holder)
	waitChannel(readersDone, 3*time.Second)
	waitErr := cmd.Wait()
	fmt.Printf("reader-first.after-release.wait=%q direct-exists=%t\n", errorText(waitErr), processExists(direct))
}

// probeCombinedOutputReap 记录标准库 CombinedOutput 已回收 direct child 后仍等待 pipe EOF。
func probeCombinedOutputReap(root string) {
	directFile := filepath.Join(root, "combined-direct")
	holderFile := filepath.Join(root, "combined-holder")
	cmd := exec.Command("sh", "-c", "echo $$ > \"$1\"; sleep 30 & echo $! > \"$2\"; exit 0", "probe", directFile, holderFile)
	finished := make(chan commandResult, 1)
	go func() {
		output, err := cmd.CombinedOutput()
		finished <- commandResult{output: output, err: err}
	}()
	direct := waitPIDFile(directFile)
	holder := waitPIDFile(holderFile)
	waitProcessGone(direct, 3*time.Second)
	fmt.Printf("combined.before-release.direct-exists=%t result-ready=%t holder-alive=%t\n", processExists(direct), channelReadyResult(finished), processExists(holder))
	kill(holder)
	result := waitResult(finished, 3*time.Second)
	fmt.Printf("combined.after-release.error=%q output.hex=%x\n", errorText(result.err), result.output)
}

// probeCancelledReaderFirst 验证 CommandContext 只杀 direct child，reader-first 使其暂为 zombie。
func probeCancelledReaderFirst(root string) {
	directFile := filepath.Join(root, "cancel-reader-direct")
	holderFile := filepath.Join(root, "cancel-reader-holder")
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "sh", "-c", "echo $$ > \"$1\"; sleep 30 & echo $! > \"$2\"; while :; do :; done", "probe", directFile, holderFile)
	stdout, err := cmd.StdoutPipe()
	must(err)
	stderr, err := cmd.StderrPipe()
	must(err)
	must(cmd.Start())
	readersDone := copyBoth(stdout, stderr)
	direct := waitPIDFile(directFile)
	holder := waitPIDFile(holderFile)
	cancel()
	state := waitProcessState(direct, "Z", 3*time.Second)
	fmt.Printf("cancel-reader.before-release.state=%s readers-done=%t holder-alive=%t\n", state, channelReady(readersDone), processExists(holder))
	kill(holder)
	waitChannel(readersDone, 3*time.Second)
	waitErr := cmd.Wait()
	fmt.Printf("cancel-reader.after-release.wait=%q direct-exists=%t\n", errorText(waitErr), processExists(direct))
}

// probeCancelledCombinedOutput 验证标准 CombinedOutput 在 pipe 被孙进程持有时的取消顺序。
func probeCancelledCombinedOutput(root string) {
	directFile := filepath.Join(root, "cancel-combined-direct")
	holderFile := filepath.Join(root, "cancel-combined-holder")
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "sh", "-c", "echo $$ > \"$1\"; sleep 30 & echo $! > \"$2\"; while :; do :; done", "probe", directFile, holderFile)
	finished := make(chan commandResult, 1)
	go func() {
		output, err := cmd.CombinedOutput()
		finished <- commandResult{output: output, err: err}
	}()
	direct := waitPIDFile(directFile)
	holder := waitPIDFile(holderFile)
	cancel()
	waitProcessGone(direct, 3*time.Second)
	fmt.Printf("cancel-combined.before-release.direct-exists=%t result-ready=%t holder-alive=%t\n", processExists(direct), channelReadyResult(finished), processExists(holder))
	kill(holder)
	result := waitResult(finished, 3*time.Second)
	fmt.Printf("cancel-combined.after-release.error=%q output.hex=%x\n", errorText(result.err), result.output)
}

// probeExtraFiles 验证父 source FD 的 CLOEXEC 与 child 3/4/5 连续映射。
func probeExtraFiles() {
	var padding []*os.File
	for index := 0; index < 80; index++ {
		file, err := os.Open("/dev/null")
		must(err)
		padding = append(padding, file)
	}
	defer func() {
		for _, file := range padding {
			_ = file.Close()
		}
	}()

	var readers []*os.File
	var sourceFDs []string
	var closeOnExec []string
	for _, payload := range []string{"alpha", "beta", "gamma"} {
		reader, writer, err := os.Pipe()
		must(err)
		_, err = writer.WriteString(payload)
		must(err)
		must(writer.Close())
		readers = append(readers, reader)
		sourceFDs = append(sourceFDs, strconv.Itoa(int(reader.Fd())))
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, reader.Fd(), syscall.F_GETFD, 0)
		mustErrno(errno)
		closeOnExec = append(closeOnExec, strconv.FormatBool(flags&syscall.FD_CLOEXEC != 0))
	}
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()

	executable, err := os.Executable()
	must(err)
	cmd := exec.Command(executable, "read-fds")
	cmd.ExtraFiles = readers
	cmd.Env = append(os.Environ(), "SOURCE_FDS="+strings.Join(sourceFDs, ","))
	output, err := cmd.CombinedOutput()
	must(err)
	fmt.Printf("extrafiles.parent.sources=%s cloexec=%s\n", strings.Join(sourceFDs, ","), strings.Join(closeOnExec, ","))
	fmt.Printf("extrafiles.child=%s", output)
}

// readInheritedFDs 是 probeExtraFiles exec 后的 child 模式。
func readInheritedFDs() {
	var payloads []string
	for fd := 3; fd <= 5; fd++ {
		file := os.NewFile(uintptr(fd), fmt.Sprintf("fd-%d", fd))
		data, err := io.ReadAll(file)
		must(err)
		payloads = append(payloads, string(data))
	}
	var visibleSources []string
	for _, source := range strings.Split(os.Getenv("SOURCE_FDS"), ",") {
		if source == "" || source == "3" || source == "4" || source == "5" {
			continue
		}
		if _, err := os.Stat("/proc/self/fd/" + source); err == nil {
			visibleSources = append(visibleSources, source)
		}
	}
	sort.Strings(visibleSources)
	fmt.Printf("payloads=%q source-visible=%q\n", payloads, visibleSources)
}

// copyBoth 并发排空两个 pipe，并在两者都 EOF 后关闭返回 channel。
func copyBoth(stdout io.Reader, stderr io.Reader) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		var wait sync.WaitGroup
		wait.Add(2)
		go func() { defer wait.Done(); _, _ = io.Copy(io.Discard, stdout) }()
		go func() { defer wait.Done(); _, _ = io.Copy(io.Discard, stderr) }()
		wait.Wait()
		close(done)
	}()
	return done
}

// waitPIDFile 等待 shell 原子写完十进制 PID。
func waitPIDFile(path string) int {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, conversionError := strconv.Atoi(strings.TrimSpace(string(data)))
			if conversionError == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("timed out waiting for pid file: " + path)
}

// waitProcessState 轮询 /proc，返回观察到的目标状态或最后状态。
func waitProcessState(pid int, expected string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	last := "missing"
	for time.Now().Before(deadline) {
		last = processState(pid)
		if last == expected {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	return last
}

// processState 从 /proc/PID/status 提取单字母状态。
func processState(pid int) string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return "missing"
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "State:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				return fields[1]
			}
		}
	}
	return "unknown"
}

// waitProcessGone 等待 direct child 已被 wait 回收。
func waitProcessGone(pid int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processExists(pid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// processExists 使用 signal 0 判断 PID 是否仍存在，包括 zombie。
func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// kill 清理探针创建的 pipe holder；ESRCH 表示它已经自然结束。
func kill(pid int) {
	err := syscall.Kill(pid, syscall.SIGKILL)
	if err != nil && !errors.Is(err, syscall.ESRCH) {
		panic(err)
	}
}

// channelReady 非阻塞检查 reader 汇合状态。
func channelReady(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

// channelReadyResult 非阻塞检查 CombinedOutput 返回状态且不消费结果。
func channelReadyResult(channel chan commandResult) bool {
	select {
	case result := <-channel:
		channel <- result
		return true
	default:
		return false
	}
}

// waitResult 带超时取得 CombinedOutput 结果。
func waitResult(channel <-chan commandResult, timeout time.Duration) commandResult {
	select {
	case result := <-channel:
		return result
	case <-time.After(timeout):
		panic("timed out waiting for CombinedOutput")
	}
}

// waitChannel 带超时等待 reader 汇合，避免失败探针永久挂住。
func waitChannel(channel <-chan struct{}, timeout time.Duration) {
	select {
	case <-channel:
	case <-time.After(timeout):
		panic("timed out waiting for pipe readers")
	}
}

// restoreEnvironment 恢复探针进入前的环境变量状态。
func restoreEnvironment(key string, value string, existed bool) {
	if existed {
		must(os.Setenv(key, value))
	} else {
		must(os.Unsetenv(key))
	}
}

// printPathError 以 hex 输出路径和错误，避免终端替换非法 UTF-8。
func printPathError(label string, path string, err error) {
	fmt.Printf("%s.path.hex=%x error.bytes=%x\n", label, []byte(path), errorBytes(err))
}

// printRunError 记录 Run 返回错误的动态类型和原始 Error 字节。
func printRunError(label string, err error) {
	fmt.Printf("%s.type=%T error.bytes=%x\n", label, err, errorBytes(err))
}

// errorBytes 将 nil 映射为空切片，其余保留 Go string 的原始字节。
func errorBytes(err error) []byte {
	if err == nil {
		return nil
	}
	return []byte(err.Error())
}

// errorText 只用于 ASCII wait 错误的稳定打印。
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// must 让基础设施错误立即终止探针，避免把无效环境误记为兼容行为。
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// mustErrno 是 syscall 探针的 errno 适配。
func mustErrno(errno syscall.Errno) {
	if errno != 0 {
		panic(errno)
	}
}
