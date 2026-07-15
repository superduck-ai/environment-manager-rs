#!/usr/bin/env python3
"""从固定 Go oracle 生成 Rust 兼容层使用的 Cobra 文本资产。"""

from __future__ import annotations

import argparse
import os
import platform
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "src/cmd/cobra_assets"
HOME_SENTINEL = "/__ENVIRONMENT_MANAGER_HOME__"
WORKING_DIRECTORY_MARKER = "{{WORKING_DIRECTORY}}"


def capture(binary: Path, arguments: list[str]) -> str:
    """以稳定 HOME 执行 Go oracle，并要求帮助/补全命令正常退出。"""
    environment = dict(os.environ)
    environment.update(
        {
            "HOME": HOME_SENTINEL,
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "TZ": "UTC",
        }
    )
    completed = subprocess.run(
        ["environment-runner", *arguments],
        executable=binary,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0 or completed.stderr:
        raise RuntimeError(
            f"Go oracle command failed args={arguments!r} code={completed.returncode} "
            f"stderr={completed.stderr.decode('utf-8', errors='replace')!r}"
        )
    return completed.stdout.decode("utf-8").replace(
        HOME_SENTINEL, WORKING_DIRECTORY_MARKER
    )


def main() -> int:
    """生成所有公开 help 与四种 shell 的有/无说明补全脚本。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-binary", type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in {"aarch64", "arm64"}:
        parser.error("Cobra 资产只允许由 Linux ARM64 Go oracle 生成")
    if not args.go_binary.is_file():
        parser.error(f"Go oracle 不存在: {args.go_binary}")

    help_commands = {
        "root_help.txt": ["--help"],
        "completion_help.txt": ["completion", "--help"],
        "orchestrator_help.txt": ["orchestrator", "--help"],
        "task_run_help.txt": ["task-run", "--help"],
        "poll_help.txt": ["poll", "--help"],
        "setup_help.txt": ["setup", "--help"],
        "print_sandbox_settings_help.txt": ["print-sandbox-settings", "--help"],
    }
    for shell in ("bash", "fish", "powershell", "zsh"):
        help_commands[f"completion_{shell}_help.txt"] = [
            "completion",
            shell,
            "--help",
        ]
        help_commands[f"completion_{shell}.txt"] = ["completion", shell]
        help_commands[f"completion_{shell}_no_descriptions.txt"] = [
            "completion",
            shell,
            "--no-descriptions",
        ]

    OUTPUT.mkdir(parents=True, exist_ok=True)
    for filename, command in help_commands.items():
        (OUTPUT / filename).write_text(capture(args.go_binary, command), encoding="utf-8")
    print(f"generated {len(help_commands)} Cobra assets under {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
