#!/usr/bin/env python3
"""把 Go/Rust process-edge 原始记录转换为严格、可比较的 JSON transcript。"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


CASE_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("lookpath_missing", ("lookpath.missing.path.hex=",)),
    ("lookpath_non_executable", ("lookpath.nonexec.path.hex=",)),
    ("lookpath_errdot", ("lookpath.errdot.path.hex=",)),
    ("lookpath_errdot_last_zero", ("lookpath.errdot-last-zero.path.hex=",)),
    ("lookpath_errdot_last_one", ("lookpath.errdot-last-one.path.hex=",)),
    ("lookpath_non_utf8_directory", ("lookpath.nonutf8-directory.path.hex=",)),
    ("lookpath_non_utf8_name", ("lookpath.nonutf8-name.path.hex=",)),
    ("run_non_utf8_enoent", ("run.nonutf8-enoent.type=",)),
    ("run_non_utf8_eacces", ("run.nonutf8-eacces.type=",)),
    ("run_pre_cancel_non_utf8_enoent", ("run.precancel-nonutf8-enoent.type=",)),
    ("run_pre_cancel_missing_basename", ("run.precancel-missing-basename.type=",)),
    (
        "combined_raw_bytes",
        (
            "combined.output.hex=",
            "combined.error.bytes=",
            "combined.wrapped.bytes=",
        ),
    ),
    (
        "reader_first_reap",
        ("reader-first.before-release.state=", "reader-first.after-release.wait="),
    ),
    (
        "combined_output_reap",
        ("combined.before-release.direct-exists=", "combined.after-release.error="),
    ),
    (
        "cancel_reader_first",
        ("cancel-reader.before-release.state=", "cancel-reader.after-release.wait="),
    ),
    (
        "cancel_combined_output",
        (
            "cancel-combined.before-release.direct-exists=",
            "cancel-combined.after-release.error=",
        ),
    ),
    (
        "extra_files_fd3",
        ("extrafiles.parent.sources=", "extrafiles.child=payloads="),
    ),
)

EXPECTED_TOOLCHAINS = {
    "go": "go.version=go1.25.7",
    "rust": "rust.version=1.97.0",
}
TEMP_ROOT_PREFIX = "dynamic.temp-root.hex="
TEMP_ROOT_MARKER_HEX = b"{temp_root}".hex()
# 临时根只允许出现在这三个已审核的 semantic line：非 UTF-8 PATH 命中、显式
# slash ENOENT 和 EACCES。使用固定位置而非全局计数，避免未来新增字段误借此擦除值。
TEMP_ROOT_LINE_INDEXES = frozenset((5, 7, 8))
FD_LINE = re.compile(
    r"^extrafiles\.parent\.sources=(\d+),(\d+),(\d+) "
    r"cloexec=(true|false),(true|false),(true|false)$"
)


class ProcessEdgeTranscriptError(RuntimeError):
    """表示原始 probe 缺项、越权归一化或动态值不满足约束。"""


def normalize_temporary_root(lines: list[str], root_hex: str) -> list[str]:
    """只替换 probe 显式声明的精确临时根字节，不匹配其他路径或错误文本。"""
    if not root_hex or len(root_hex) % 2 != 0 or not re.fullmatch(r"[0-9a-f]+", root_hex):
        raise ProcessEdgeTranscriptError("dynamic temp root 必须是非空小写十六进制")
    try:
        root = bytes.fromhex(root_hex)
    except ValueError as error:
        raise ProcessEdgeTranscriptError("dynamic temp root 不是有效十六进制") from error
    if not root.startswith(b"/") or b"\x00" in root:
        raise ProcessEdgeTranscriptError("dynamic temp root 必须是无 NUL 的绝对 Unix 路径")

    occurrences = [line.count(root_hex) for line in lines]
    expected_occurrences = [
        1 if index in TEMP_ROOT_LINE_INDEXES else 0 for index in range(len(lines))
    ]
    # 不只检查总数，还固定出现位置：否则删掉一个 raw-path case、再让另一个字段重复
    # 三次，也可能被宽泛的全局 replace 掩盖。
    if occurrences != expected_occurrences:
        raise ProcessEdgeTranscriptError(
            "dynamic temp root 只能在 semantic line 6/8/9 各出现一次，"
            f"实际分布 {occurrences}"
        )
    return [line.replace(root_hex, TEMP_ROOT_MARKER_HEX) for line in lines]


def normalize_source_fds(lines: list[str]) -> list[str]:
    """把三个不可预测 source FD 按 ExtraFiles 顺序改名，同时严格验证 CLOEXEC。"""
    output = list(lines)
    matching = [index for index, line in enumerate(output) if FD_LINE.fullmatch(line)]
    if len(matching) != 1:
        raise ProcessEdgeTranscriptError(
            f"必须且只能有一条 ExtraFiles parent FD 记录，实际 {len(matching)} 条"
        )
    index = matching[0]
    match = FD_LINE.fullmatch(output[index])
    assert match is not None
    descriptors = [int(value) for value in match.groups()[:3]]
    close_on_exec = list(match.groups()[3:])
    if len(set(descriptors)) != 3 or any(descriptor < 64 for descriptor in descriptors):
        raise ProcessEdgeTranscriptError(
            f"source FD 必须是三个不同且 >=64 的值，实际 {descriptors}"
        )
    if close_on_exec != ["true", "true", "true"]:
        raise ProcessEdgeTranscriptError(
            f"三个 parent source FD 都必须设置 CLOEXEC，实际 {close_on_exec}"
        )
    output[index] = (
        "extrafiles.parent.sources={source_fd_0},{source_fd_1},{source_fd_2} "
        "cloexec=true,true,true"
    )
    return output


def convert_raw(source: str, raw: bytes) -> dict[str, object]:
    """验证完整原始行集合，执行两项最小归一化并构造固定 case 文档。"""
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as error:
        raise ProcessEdgeTranscriptError("process-edge raw transcript 必须只含 ASCII") from error
    if "\r" in text:
        raise ProcessEdgeTranscriptError("process-edge raw transcript 不允许 CR 换行")
    lines = text.splitlines()
    if not text.endswith("\n"):
        raise ProcessEdgeTranscriptError("process-edge raw transcript 必须以换行结束")
    if len(lines) < 2 or lines[0] != EXPECTED_TOOLCHAINS[source]:
        raise ProcessEdgeTranscriptError(
            f"{source} probe 工具链标识不符: {lines[0] if lines else '<missing>'}"
        )
    if not lines[1].startswith(TEMP_ROOT_PREFIX):
        raise ProcessEdgeTranscriptError("缺少 dynamic.temp-root.hex 声明")
    root_hex = lines[1][len(TEMP_ROOT_PREFIX) :]
    semantic_lines = normalize_temporary_root(lines[2:], root_hex)
    semantic_lines = normalize_source_fds(semantic_lines)

    expected_prefixes = [prefix for _, prefixes in CASE_GROUPS for prefix in prefixes]
    if len(semantic_lines) != len(expected_prefixes):
        raise ProcessEdgeTranscriptError(
            f"process-edge 行数 {len(semantic_lines)} != manifest 行数 {len(expected_prefixes)}"
        )
    for index, (line, prefix) in enumerate(zip(semantic_lines, expected_prefixes, strict=True)):
        if not line.startswith(prefix):
            raise ProcessEdgeTranscriptError(
                f"process-edge 第 {index + 1} 行前缀 {line!r} != {prefix!r}"
            )

    cases: list[dict[str, object]] = []
    cursor = 0
    for name, prefixes in CASE_GROUPS:
        case_lines = semantic_lines[cursor : cursor + len(prefixes)]
        cases.append({"name": name, "lines": case_lines})
        cursor += len(prefixes)
    return {
        "schema_version": 1,
        "protocol": "process_edge",
        "normalization": {
            "temporary_root": "exact_declared_bytes_to_{temp_root}",
            "source_fds": "ordered_values_to_{source_fd_N}",
        },
        "cases": cases,
    }


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """解析实现类型、原始记录和 JSON 输出路径。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=sorted(EXPECTED_TOOLCHAINS), required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """转换一份 transcript；验证或写入失败均由入口返回非零。"""
    arguments = parse_arguments(sys.argv[1:] if argv is None else argv)
    try:
        raw = arguments.raw.read_bytes()
    except OSError as error:
        raise ProcessEdgeTranscriptError(f"无法读取 raw transcript: {error}") from error
    document = convert_raw(arguments.source, raw)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProcessEdgeTranscriptError as error:
        print(f"process-edge transcript 前置检查失败: {error}", file=sys.stderr)
        raise SystemExit(2) from error
