#!/usr/bin/env python3
"""从固定 Go ARM64 oracle 捕获 V0 缺字段的 runtime panic transcript 资产。"""

from __future__ import annotations

import argparse
import os
import platform
import sys
from pathlib import Path

import differential_cli


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "src/cmd/cobra_assets/v0_missing_startup_panic.txt"
PANIC_PREFIX = "panic: runtime error: invalid memory address or nil pointer dereference\n"
REQUIRED_ORDERED_FRAGMENTS = (
    "[signal SIGSEGV: segmentation violation code=0x1 ",
    "goroutine 1 [running]:\n",
    "internal/input.(*V0Parser).Parse(",
    "\tgithub.com/anthropics/anthropic/api-go/environment-manager/internal/input/"
    "v0_parser.go:58 +0x188\n",
    "cmd.loadContextFromStdin(",
    "\tgithub.com/anthropics/anthropic/api-go/environment-manager/cmd/"
    "cmd_task_run.go:576 +0x2b4\n",
    "cmd.AddTaskRunCommand.func1(",
    "\tgithub.com/anthropics/anthropic/api-go/environment-manager/cmd/"
    "cmd_task_run.go:152 +0x378\n",
    "github.com/spf13/cobra.(*Command).execute(",
    "\tgithub.com/spf13/cobra@v1.9.1/command.go:1015 +0x7d4\n",
    "github.com/spf13/cobra.(*Command).ExecuteC(",
    "\tgithub.com/spf13/cobra@v1.9.1/command.go:1148 +0x350\n",
    "github.com/spf13/cobra.(*Command).Execute(...)\n",
    "\tgithub.com/spf13/cobra@v1.9.1/command.go:1071\n",
    "main.main()\n",
    "\t./main.go:44 +0x2bc\n",
)
CASE = {
    "name": "task_run_v0_missing_startup_context_panic",
    "argv0": "environment-runner",
    "args": [
        "task-run",
        "--session",
        "session-v0-missing-startup",
        "--upgrade-claude-code=false",
    ],
    "stdin": '{"environment":{"environment_type":"byoc"}}',
    "normalizers": ["rfc3339_timestamp", "go_panic_runtime_hex"],
}


class PanicAssetError(RuntimeError):
    """表示 Go oracle 没有产生受审核的 nil-dereference transcript。"""


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """解析 oracle 与输出路径，路径始终相对调用者当前目录正常解析。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--check",
        action="store_true",
        help="只验证已提交资产与当前固定 oracle 一致，不写文件",
    )
    return parser.parse_args(argv)


def panic_suffix(capture: dict[str, object]) -> str:
    """从完整 stderr 中提取 panic 起始处到 EOF，并验证进程级外观。"""
    if capture.get("exit_code") != 2:
        raise PanicAssetError(f"Go nil dereference exit={capture.get('exit_code')!r}，期望 2")
    if capture.get("stdout") != "":
        raise PanicAssetError("Go nil dereference 不应写 stdout")
    stderr = capture.get("stderr")
    if not isinstance(stderr, str):
        raise PanicAssetError("Go stderr capture 不是 UTF-8 文本")
    position = stderr.find(PANIC_PREFIX)
    if position < 0:
        raise PanicAssetError("Go stderr 中找不到固定 nil-dereference panic 前缀")
    transcript = stderr[position:]
    cursor = 0
    for fragment in REQUIRED_ORDERED_FRAGMENTS:
        position = transcript.find(fragment, cursor)
        if position < 0:
            raise PanicAssetError(f"Go panic transcript 缺少稳定 frame/标记: {fragment!r}")
        # 顺序也是外部协议：parser 必须先于 cmd/Cobra/main frame，不能只检查无序集合。
        cursor = position + len(fragment)
    if "thread 'main' panicked" in transcript:
        raise PanicAssetError("Go panic transcript 意外包含 Rust panic hook 文本")
    if not transcript.endswith("\n"):
        raise PanicAssetError("Go panic transcript 必须以换行结束")
    return transcript


def scrub_runtime_hex(transcript: str) -> str:
    """把仅能在差分中归一化的地址/寄存值写成无害的固定 `0x0`。"""
    return differential_cli.normalize_go_panic_runtime_hex(
        transcript,
        "0x0",
        require_all_frames=True,
    )


def capture_stable_asset(go_binary: Path) -> bytes:
    """运行 oracle 两次，证明除白名单十六进制值外 transcript 完全稳定。"""
    first = panic_suffix(differential_cli.capture(go_binary, CASE))
    second = panic_suffix(differential_cli.capture(go_binary, CASE))
    first_scrubbed = scrub_runtime_hex(first)
    second_scrubbed = scrub_runtime_hex(second)
    if first_scrubbed != second_scrubbed:
        raise PanicAssetError(
            "两次 Go panic 在地址/寄存值归一化后仍不一致，拒绝生成不稳定资产"
        )
    return first_scrubbed.encode("utf-8")


def write_if_changed(path: Path, content: bytes) -> bool:
    """原子更新 transcript；内容未变化时保留文件时间，便于 stale 门禁。"""
    if path.is_file() and path.read_bytes() == content:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(content)
    os.replace(temporary, path)
    return True


def check_current(path: Path, expected: bytes) -> None:
    """验证已提交资产逐字节等于本次 oracle 生成值，缺失或陈旧立即失败。"""
    try:
        actual = path.read_bytes()
    except OSError as error:
        raise PanicAssetError(f"无法读取已提交 panic 资产 {path}: {error}") from error
    if actual != expected:
        raise PanicAssetError(
            "V0 panic 资产已陈旧；请在固定 Linux ARM64 oracle 上重新运行本生成器"
        )


def main(argv: list[str]) -> int:
    """校验 Linux ARM64 平台，捕获稳定资产并报告是否发生更新。"""
    arguments = parse_arguments(argv)
    try:
        if platform.system() != "Linux" or platform.machine() not in {"aarch64", "arm64"}:
            raise PanicAssetError("panic 资产只允许由 Linux ARM64 Go oracle 生成")
        go_binary = arguments.go_binary.resolve()
        if not go_binary.is_file() or not os.access(go_binary, os.X_OK):
            raise PanicAssetError(f"Go oracle 不存在或不可执行: {go_binary}")
        content = capture_stable_asset(go_binary)
        if arguments.check:
            check_current(arguments.output, content)
            print(f"v0 panic asset: current {arguments.output}")
            return 0
        changed = write_if_changed(arguments.output, content)
        print(f"v0 panic asset: {'updated' if changed else 'unchanged'} {arguments.output}")
        return 0
    except (OSError, PanicAssetError, differential_cli.DifferentialError) as error:
        print(f"v0 panic asset generation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
