#!/usr/bin/env python3
"""在 Linux ARM64 上逐字节比较 Go oracle、golden 与 Rust CLI。"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CASES = ROOT / "compat/differential/cli_cases.json"
DEFAULT_GOLDENS = ROOT / "compat/goldens/cli.json"
DEFAULT_REPORT = ROOT / "target/differential/cli-report.json"

# normalizer 必须逐用例显式启用。这里没有路径、错误文本等宽泛替换，避免比较器
# 悄悄吞掉真实兼容差异；替换值也包含类型，便于 transcript 审核。
NORMALIZERS: dict[str, tuple[re.Pattern[str], str]] = {
    "uuid": (
        re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"),
        "<UUID>",
    ),
    "loopback_port": (
        re.compile(r"\b(127\.0\.0\.1|\[::1\]|localhost):[0-9]{1,5}\b"),
        "<LOOPBACK_HOST:PORT>",
    ),
    "rfc3339_timestamp": (
        re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\b"),
        "<RFC3339_TIMESTAMP>",
    ),
    # tempfile 会给每次 Go/Rust 执行分配不同目录；仅替换本比较器自己创建的固定
    # `env-manager-<case>-<8 chars>` 前缀，绝不归一化用户路径或一般 `/tmp` 路径。
    "temporary_directory": (
        re.compile(r"/tmp/env-manager-[a-z0-9_]+-[a-z0-9_]{8}"),
        "<DIFFERENTIAL_TEMP_DIR>",
    ),
}

# 独立 Go 1.25.7 ARM64 进程捕获证明，只有下列固定 frame 参数槽会随运行改变。
# 除 receiver、heap slice/object 和 argv 指针外，`workingDirectory` 的字符串 data
# 指针也会在 `0x400003c005`/`0x400003e005` 等地址间变化。signal 的 addr=0xc0、
# pc=0x61a128，context itab/data、静态字符串指针、slice/string 长度与 capacity 以及
# 源码 `+0xNN` 偏移都保持稳定，必须逐字节比较，不能被“地址”宽泛吞掉。
GO_PANIC_DYNAMIC_HEX_SLOTS: tuple[tuple[str, tuple[int, ...], int], ...] = (
    (
        "github.com/anthropics/anthropic/api-go/environment-manager/internal/input."
        "(*V0Parser).Parse(",
        (0, 3),
        6,
    ),
    (
        "github.com/anthropics/anthropic/api-go/environment-manager/cmd."
        "loadContextFromStdin(",
        (2, 3, 7),
        9,
    ),
    (
        "github.com/anthropics/anthropic/api-go/environment-manager/cmd."
        "AddTaskRunCommand.func1(",
        (0,),
        4,
    ),
    ("github.com/spf13/cobra.(*Command).execute(", (0, 1), 4),
    ("github.com/spf13/cobra.(*Command).ExecuteC(", (0,), 1),
)
CUSTOM_NORMALIZERS = {
    "go_panic_runtime_hex",
    "structured_log_read_duration_ms",
}
# 读取 stdin 的耗时取决于 Linux 调度，只有真实触发 Go nil dereference 的这个 CLI
# 场景需要忽略它。把限制写进 manifest 校验而不只依赖评审约定，避免后续用例误用。
CASE_SCOPED_NORMALIZERS: dict[str, frozenset[str]] = {
    "structured_log_read_duration_ms": frozenset(
        {"task_run_v0_missing_startup_context_panic"}
    ),
}
HEX_TOKEN = re.compile(r"0x[0-9a-f]+")
JSON_NUMBER_TOKEN = re.compile(
    r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z"
)

# 凭证和会改变后端分支的环境变量全部从差分进程移除。PATH 被保留是为了动态
# loader/受控子命令可工作；当前首批用例不会调用网络或安装程序。
REMOVED_ENVIRONMENT = {
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ENVIRONMENT_SERVICE_KEY",
    "ENVRUNNER_SKIP_ACK",
    "ENVRUNNER_SKIP_CLAUDE_CODE",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_HEADERS",
    # Go runtime panic 的 frame 范围由 GOTRACEBACK/GODEBUG 控制；canonical case 必须
    # 使用 Go 1.25.7 默认用户栈。Rust backtrace 开关也移除，确保意外回归到 Rust
    # hook 时得到稳定且明确不兼容的默认输出，而不是继承调用者机器配置。
    "GOTRACEBACK",
    "GODEBUG",
    "RUST_BACKTRACE",
}


class DifferentialError(RuntimeError):
    """表示平台、manifest 或进程输出不满足差分测试前置条件。"""


def load_json(path: Path) -> dict[str, Any]:
    """读取 UTF-8 JSON 对象，并在顶层不是对象时给出可定位错误。"""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DifferentialError(f"无法读取 JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise DifferentialError(f"JSON 顶层必须是对象: {path}")
    return value


def validate_manifest(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """验证用例名称、参数和 normalizer 白名单，防止静默跳过或过度归一化。"""
    if manifest.get("schema_version") != 1:
        raise DifferentialError("只支持 schema_version=1 的 CLI 差分 manifest")
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases:
        raise DifferentialError("CLI 差分 manifest 必须包含非空 cases 数组")
    names: set[str] = set()
    for case in cases:
        if not isinstance(case, dict):
            raise DifferentialError("每个 CLI 差分用例必须是对象")
        name = case.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise DifferentialError(f"用例名称为空或重复: {name!r}")
        names.add(name)
        if not isinstance(case.get("argv0"), str) or not case["argv0"]:
            raise DifferentialError(f"{name}: argv0 必须是非空字符串")
        if not isinstance(case.get("args"), list) or not all(
            isinstance(argument, str) for argument in case["args"]
        ):
            raise DifferentialError(f"{name}: args 必须是字符串数组")
        if not isinstance(case.get("stdin", ""), str):
            raise DifferentialError(f"{name}: stdin 必须是 UTF-8 字符串")
        normalizers = case.get("normalizers", [])
        allowed_normalizers = set(NORMALIZERS) | CUSTOM_NORMALIZERS
        if not isinstance(normalizers, list) or not all(
            normalizer in allowed_normalizers for normalizer in normalizers
        ):
            raise DifferentialError(
                f"{name}: normalizers 只能取 {sorted(allowed_normalizers)}"
            )
        for normalizer in normalizers:
            allowed_cases = CASE_SCOPED_NORMALIZERS.get(normalizer)
            if allowed_cases is not None and name not in allowed_cases:
                raise DifferentialError(
                    f"{name}: normalizer {normalizer!r} 只允许用于 "
                    f"{sorted(allowed_cases)}"
                )
    return cases


def normalize_text(text: str, normalizers: list[str]) -> str:
    """按 manifest 声明顺序归一化真正不可预测的值，其他字节保持不变。"""
    normalized = text
    for normalizer in normalizers:
        if normalizer == "go_panic_runtime_hex":
            normalized = normalize_go_panic_runtime_hex(
                normalized, "<GO_RUNTIME_HEX>"
            )
            continue
        if normalizer == "structured_log_read_duration_ms":
            normalized = normalize_structured_log_read_duration_ms(normalized)
            continue
        pattern, replacement = NORMALIZERS[normalizer]
        normalized = pattern.sub(replacement, normalized)
    return normalized


def _top_level_json_number_span(
    line: str,
    field_name: str,
    *,
    required_string_field: tuple[str, str] | None = None,
) -> tuple[int, int] | None:
    """定位单行 JSON 对象中唯一的顶层数值字段。

    这里不重新序列化整行，因为 key 顺序、空白和 escaping 都是 CLI
    兼容合约。JSON decoder 只用于跨过完整的 key/value，返回的 span
    仍指向原始字节文本。重复 key、嵌套同名 key、字符串与非标准数值
    都不会被误替换。
    """

    decoder = json.JSONDecoder()
    length = len(line)

    def skip_whitespace(index: int) -> int:
        """跳过 JSON 允许的四种 ASCII 空白，不把其他 Unicode 字符当空白。"""
        while index < length and line[index] in " \t\r\n":
            index += 1
        return index

    cursor = skip_whitespace(0)
    if cursor >= length or line[cursor] != "{":
        return None
    cursor += 1
    spans: list[tuple[int, int]] = []
    required_values: list[Any] = []

    cursor = skip_whitespace(cursor)
    if cursor < length and line[cursor] == "}":
        cursor += 1
    else:
        while True:
            cursor = skip_whitespace(cursor)
            try:
                key, key_end = decoder.raw_decode(line, cursor)
            except json.JSONDecodeError:
                return None
            if not isinstance(key, str):
                return None

            cursor = skip_whitespace(key_end)
            if cursor >= length or line[cursor] != ":":
                return None
            value_start = skip_whitespace(cursor + 1)
            try:
                value, value_end = decoder.raw_decode(line, value_start)
            except json.JSONDecodeError:
                return None

            raw_value = line[value_start:value_end]
            if required_string_field is not None and key == required_string_field[0]:
                required_values.append(value)
            if (
                key == field_name
                and isinstance(value, (int, float))
                and not isinstance(value, bool)
                and JSON_NUMBER_TOKEN.fullmatch(raw_value) is not None
            ):
                spans.append((value_start, value_end))

            cursor = skip_whitespace(value_end)
            if cursor >= length:
                return None
            if line[cursor] == ",":
                cursor += 1
                continue
            if line[cursor] == "}":
                cursor += 1
                break
            return None

    if skip_whitespace(cursor) != length or len(spans) != 1:
        return None
    if required_string_field is not None and required_values != [
        required_string_field[1]
    ]:
        return None
    return spans[0]


def normalize_structured_log_read_duration_ms(text: str) -> str:
    """只归一化单行结构化日志顶层的 `read_duration_ms` 数值。

    该字段是读取 stdin 的真实耗时，会受 Linux 调度影响。比较副本
    使用显式字符串占位符；原始 capture 和摘要仍保留真实数值。
    """

    normalized_lines: list[str] = []
    for line in text.splitlines(keepends=True):
        span = _top_level_json_number_span(
            line,
            "read_duration_ms",
            required_string_field=("msg", "Read stdin data"),
        )
        if span is None:
            normalized_lines.append(line)
            continue
        start, end = span
        normalized_lines.append(
            f'{line[:start]}"<READ_DURATION_MS>"{line[end:]}'
        )
    return "".join(normalized_lines)


def normalize_go_panic_runtime_hex(
    text: str, replacement: str, *, require_all_frames: bool = False
) -> str:
    """只替换固定 Go frame 中经多进程证明确实变化的十六进制参数槽。"""
    lines = text.splitlines(keepends=True)
    matched_prefixes: set[str] = set()
    for line_index, line in enumerate(lines):
        for prefix, dynamic_slots, expected_tokens in GO_PANIC_DYNAMIC_HEX_SLOTS:
            if not line.startswith(prefix):
                continue
            tokens = list(HEX_TOKEN.finditer(line))
            if len(tokens) != expected_tokens:
                # 比较器保留异常 frame 原文让 Go/Rust 比较失败；资产生成器则在下面
                # 通过 require_all_frames 把形状漂移提升为明确配置错误。
                continue
            pieces: list[str] = []
            cursor = 0
            for token_index, token in enumerate(tokens):
                pieces.append(line[cursor : token.start()])
                pieces.append(replacement if token_index in dynamic_slots else token.group(0))
                cursor = token.end()
            pieces.append(line[cursor:])
            lines[line_index] = "".join(pieces)
            matched_prefixes.add(prefix)
            break
    if require_all_frames:
        missing = [
            prefix
            for prefix, _slots, _count in GO_PANIC_DYNAMIC_HEX_SLOTS
            if prefix not in matched_prefixes
        ]
        if missing:
            raise DifferentialError(f"Go panic 缺少可定位的动态参数 frame: {missing!r}")
    return "".join(lines)


def normalize_capture(capture: dict[str, Any], normalizers: list[str]) -> dict[str, Any]:
    """只归一化 stdout/stderr；退出码和超时状态始终精确比较。"""
    return {
        "exit_code": capture["exit_code"],
        "stdout": normalize_text(capture["stdout"], normalizers),
        "stderr": normalize_text(capture["stderr"], normalizers),
        "timed_out": capture.get("timed_out", False),
    }


def _isolated_environment(home: Path, overrides: dict[str, str]) -> dict[str, str]:
    """构造无凭证、固定 locale/timezone 且副作用被临时 HOME 吸收的环境。"""
    environment = dict(os.environ)
    for name in REMOVED_ENVIRONMENT:
        environment.pop(name, None)
    environment.update(
        {
            "HOME": str(home),
            "XDG_CACHE_HOME": str(home / ".cache"),
            "XDG_CONFIG_HOME": str(home / ".config"),
            "XDG_STATE_HOME": str(home / ".local/state"),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "TZ": "UTC",
        }
    )
    environment.update(overrides)
    return environment


def capture(binary: Path, case: dict[str, Any]) -> dict[str, Any]:
    """以指定 argv0 执行二进制并分别捕获退出码、stdout、stderr。

    `executable=` 让 `args[0]` 可独立于真实文件名，因而能够真实覆盖 Go main 的
    `argv[0] == code-sign` 分派。每次执行使用独立临时 HOME/cwd，避免日志、配置或
    上一用例的文件影响下一次比较。
    """
    # 子进程 cwd 会切到隔离临时目录，因此调用者传入的相对二进制路径必须先相对
    # 当前工作目录绝对化，不能让 Popen 在临时目录下重新解释。
    binary = binary.resolve()
    timeout_seconds = float(case.get("timeout_seconds", 10))
    overrides = case.get("env", {})
    if not isinstance(overrides, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in overrides.items()
    ):
        raise DifferentialError(f"{case['name']}: env 必须是字符串到字符串的对象")

    with tempfile.TemporaryDirectory(prefix=f"env-manager-{case['name']}-") as directory:
        temporary = Path(directory)
        home = temporary / "home"
        home.mkdir()
        process = subprocess.Popen(
            [case["argv0"], *case["args"]],
            executable=str(binary),
            cwd=temporary,
            env=_isolated_environment(home, overrides),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        timed_out = False
        try:
            stdout_bytes, stderr_bytes = process.communicate(
                case.get("stdin", "").encode("utf-8"), timeout=timeout_seconds
            )
        except subprocess.TimeoutExpired:
            timed_out = True
            # 差分用例不得把挂起误当作普通退出。这里只终止直接测试进程，符合迁移
            # 约束且不会擅自改变被测程序的进程组语义。
            process.kill()
            stdout_bytes, stderr_bytes = process.communicate()

    try:
        stdout = stdout_bytes.decode("utf-8")
        stderr = stderr_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise DifferentialError(
            f"{case['name']}: CLI transcript 不是 UTF-8，需扩展无损字节编码: {error}"
        ) from error
    return {
        "exit_code": process.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "timed_out": timed_out,
        "stdout_sha256": hashlib.sha256(stdout_bytes).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr_bytes).hexdigest(),
    }


def compared_fields(
    expected: dict[str, Any], actual: dict[str, Any], normalizers: list[str]
) -> list[str]:
    """返回所有不一致字段；空列表才表示 transcript 完全兼容。"""
    normalized_expected = normalize_capture(expected, normalizers)
    normalized_actual = normalize_capture(actual, normalizers)
    return [
        field
        for field in ("exit_code", "stdout", "stderr", "timed_out")
        if normalized_expected[field] != normalized_actual[field]
    ]


def text_diff(label: str, expected: str, actual: str) -> str:
    """生成限制长度的 unified diff，让 CI 日志可读而完整内容留在 JSON 报告。"""
    lines = list(
        difflib.unified_diff(
            expected.splitlines(keepends=True),
            actual.splitlines(keepends=True),
            fromfile=f"go/{label}",
            tofile=f"rust/{label}",
        )
    )
    return "".join(lines[:120])


def golden_document(
    manifest: dict[str, Any], captures: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """构造可重复的 golden 文档；不写生成时间或临时路径等随机元数据。"""
    return {
        "schema_version": 1,
        "baseline": manifest["baseline"],
        "cases": captures,
    }


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """解析差分工具参数，默认路径均相对仓库根目录而非当前工作目录。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-binary", type=Path, required=True)
    parser.add_argument("--rust-binary", type=Path, required=True)
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    parser.add_argument("--goldens", type=Path, default=DEFAULT_GOLDENS)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument(
        "--update-goldens",
        action="store_true",
        help="以已校验 Go oracle 重写 golden；Rust 差异仍会导致失败",
    )
    parser.add_argument(
        "--show-diffs",
        action="store_true",
        help="在终端打印最多 120 行 unified diff；完整内容始终写入 JSON 报告",
    )
    parser.add_argument("--list", action="store_true", help="仅列出用例名称")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """执行 Go/golden/Rust 三方门禁并写出完整机器可读报告。"""
    arguments = parse_arguments(argv)
    try:
        manifest = load_json(arguments.cases)
        cases = validate_manifest(manifest)
        if arguments.list:
            for case in cases:
                print(case["name"])
            return 0
        if platform.system() != "Linux" or platform.machine() not in {"aarch64", "arm64"}:
            raise DifferentialError("CLI 差分测试只允许在 Linux ARM64 上运行")
        for label, binary in (
            ("Go oracle", arguments.go_binary),
            ("Rust", arguments.rust_binary),
        ):
            if not binary.is_file() or not os.access(binary, os.X_OK):
                raise DifferentialError(f"{label} 二进制不存在或不可执行: {binary}")

        go_captures = {case["name"]: capture(arguments.go_binary, case) for case in cases}
        if arguments.update_goldens:
            arguments.goldens.parent.mkdir(parents=True, exist_ok=True)
            arguments.goldens.write_text(
                json.dumps(
                    golden_document(manifest, go_captures),
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        goldens = load_json(arguments.goldens)
        if goldens.get("schema_version") != 1 or goldens.get("baseline") != manifest.get(
            "baseline"
        ):
            raise DifferentialError("golden schema 或 Go 基线元数据与 manifest 不一致")
        golden_cases = goldens.get("cases")
        if not isinstance(golden_cases, dict) or set(golden_cases) != {
            case["name"] for case in cases
        }:
            raise DifferentialError("golden 用例集合与 manifest 不一致")

        rust_captures = {
            case["name"]: capture(arguments.rust_binary, case) for case in cases
        }
        results: list[dict[str, Any]] = []
        failed = 0
        for case in cases:
            name = case["name"]
            normalizers = case.get("normalizers", [])
            golden_fields = compared_fields(golden_cases[name], go_captures[name], normalizers)
            rust_fields = compared_fields(go_captures[name], rust_captures[name], normalizers)
            passed = not golden_fields and not rust_fields
            failed += int(not passed)
            results.append(
                {
                    "name": name,
                    "argv0": case["argv0"],
                    "args": case["args"],
                    "normalizers": normalizers,
                    "passed": passed,
                    "go_golden_mismatch_fields": golden_fields,
                    "go_rust_mismatch_fields": rust_fields,
                    "go": go_captures[name],
                    "rust": rust_captures[name],
                }
            )
            status = "PASS" if passed else "FAIL"
            details = []
            if golden_fields:
                details.append(f"Go/golden={','.join(golden_fields)}")
            if rust_fields:
                details.append(f"Go/Rust={','.join(rust_fields)}")
            print(f"[{status}] {name}" + (f" ({'; '.join(details)})" if details else ""))
            if arguments.show_diffs and "stdout" in rust_fields:
                print(text_diff("stdout", go_captures[name]["stdout"], rust_captures[name]["stdout"]))
            if arguments.show_diffs and "stderr" in rust_fields:
                print(text_diff("stderr", go_captures[name]["stderr"], rust_captures[name]["stderr"]))

        report = {
            "schema_version": 1,
            "platform": {"system": platform.system(), "machine": platform.machine()},
            "baseline": manifest["baseline"],
            "summary": {
                "total": len(results),
                "passed": len(results) - failed,
                "failed": failed,
            },
            "results": results,
        }
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        arguments.report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(
            f"CLI differential: {len(results) - failed}/{len(results)} passed; "
            f"report={arguments.report}"
        )
        return 1 if failed else 0
    except DifferentialError as error:
        print(f"differential configuration error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
