#!/usr/bin/env python3
"""严格比较固定 Go oracle 与 Rust 的协议及 Linux process-edge transcript。"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import platform
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = ROOT / "compat/differential/protocol_cases.json"
DEFAULT_REPORT = ROOT / "target/differential/protocol-report.json"
PROTOCOL_NAMES = (
    "http_api",
    "input",
    "mcp",
    "process_edge",
    "tunnel_protojson",
    "tunnel_http_runtime",
    "tunnel_ws_runtime",
)

# input parser 的墙钟耗时无法逐值复现，但 producer 必须保留原始整数。比较器仅接受
# 这一条审核过的路径规则，并在替换前验证每个命中值都是非负 JSON integer。
INPUT_DURATION_NORMALIZER = {
    "protocol": "input",
    "path": "$.cases[*].logs[*].attributes[name=parse_duration_ms].value",
    "kind": "nonnegative_integer",
}


class ProtocolDifferentialError(RuntimeError):
    """表示平台、manifest、transcript 或精确差分不满足门禁。"""


@dataclass(frozen=True)
class Difference:
    """一处结构路径上的 Go/Rust 差异。"""

    path: str
    go: Any
    rust: Any


def load_json(path: Path) -> dict[str, Any]:
    """读取 UTF-8 JSON object，拒绝缺失、半写入和非 object 顶层。"""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProtocolDifferentialError(f"无法读取 JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ProtocolDifferentialError(f"JSON 顶层必须是 object: {path}")
    return value


def validate_manifest(manifest: dict[str, Any]) -> None:
    """固定协议覆盖集合，并明确拒绝任何未审核 normalizer。"""
    if manifest.get("schema_version") != 1:
        raise ProtocolDifferentialError("只支持 schema_version=1 的协议差分 manifest")
    # 只允许 input parse_duration_ms 这一条不可预测墙钟值。producer 仍输出原始值，
    # comparator 在 normalize_document 中逐个验证类型/范围后才替换比较副本。
    if manifest.get("normalizers") != [INPUT_DURATION_NORMALIZER]:
        raise ProtocolDifferentialError(
            "协议差分 normalizers 必须精确为审核过的 input parse_duration_ms 规则"
        )
    protocols = manifest.get("protocols")
    if not isinstance(protocols, dict) or set(protocols) != set(PROTOCOL_NAMES):
        raise ProtocolDifferentialError(
            f"manifest protocols 必须精确为 {list(PROTOCOL_NAMES)}"
        )


def _string_list(value: Any, label: str) -> list[str]:
    """验证 manifest/transcript 中的名称数组，拒绝空值和重复项。"""
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
        or len(set(value)) != len(value)
    ):
        raise ProtocolDifferentialError(f"{label} 必须是非空且不重复的字符串数组")
    return value


def validate_transcript(
    protocol: str, document: dict[str, Any], manifest: dict[str, Any], source: str
) -> None:
    """验证 probe 没有通过同时删除 Go/Rust 用例来制造假阳性。"""
    if document.get("schema_version") != 1 or document.get("protocol") != protocol:
        raise ProtocolDifferentialError(f"{source}/{protocol}: schema 或 protocol 标识不符")
    specification = manifest["protocols"][protocol]
    if protocol == "http_api":
        requests = document.get("requests")
        outcomes = document.get("outcomes")
        if not isinstance(requests, list) or not all(
            isinstance(item, dict) for item in requests
        ):
            raise ProtocolDifferentialError(f"{source}/{protocol}: requests 必须是 object 数组")
        if not isinstance(outcomes, list) or not all(
            isinstance(item, dict) for item in outcomes
        ):
            raise ProtocolDifferentialError(f"{source}/{protocol}: outcomes 必须是 object 数组")
        actual_targets = [item.get("target") for item in requests]
        actual_outcomes = [item.get("name") for item in outcomes]
        expected_targets = _string_list(
            specification.get("request_targets"), "http_api.request_targets"
        )
        expected_outcomes = _string_list(
            specification.get("outcome_names"), "http_api.outcome_names"
        )
        if actual_targets != expected_targets:
            raise ProtocolDifferentialError(
                f"{source}/{protocol}: request targets {actual_targets!r} != {expected_targets!r}"
            )
        if actual_outcomes != expected_outcomes:
            raise ProtocolDifferentialError(
                f"{source}/{protocol}: outcome names {actual_outcomes!r} != {expected_outcomes!r}"
            )
        return

    cases = document.get("cases")
    if not isinstance(cases, list) or not all(isinstance(item, dict) for item in cases):
        raise ProtocolDifferentialError(f"{source}/{protocol}: cases 必须是 object 数组")
    actual_names = [item.get("name") for item in cases]
    expected_names = _string_list(
        specification.get("case_names"), f"{protocol}.case_names"
    )
    if actual_names != expected_names:
        raise ProtocolDifferentialError(
            f"{source}/{protocol}: case names {actual_names!r} != {expected_names!r}"
        )


def normalize_document(
    protocol: str,
    document: dict[str, Any],
    manifest: dict[str, Any],
    source: str,
) -> dict[str, Any]:
    """按 manifest 审核规则复制并处理不可预测字段，绝不改写原 transcript。

    当前唯一规则只匹配 input 日志属性数组中名称恰为 parse_duration_ms 的 value；
    其他整数、字符串、错误、Base64 和业务字段都没有归一化入口。
    """
    normalized = copy.deepcopy(document)
    if protocol != "input":
        return normalized
    if manifest.get("normalizers") != [INPUT_DURATION_NORMALIZER]:
        raise ProtocolDifferentialError("input duration normalizer 未通过 manifest 审核")

    matched = 0
    for case_index, case in enumerate(normalized.get("cases", [])):
        for log_index, record in enumerate(case.get("logs", [])):
            for attribute_index, attribute in enumerate(record.get("attributes", [])):
                if attribute.get("name") != "parse_duration_ms":
                    continue
                value = attribute.get("value")
                if (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or value < 0
                ):
                    raise ProtocolDifferentialError(
                        f"{source}/input: cases[{case_index}].logs[{log_index}]"
                        f".attributes[{attribute_index}].value 必须是非负整数"
                    )
                attribute["value"] = "<nonnegative_integer>"
                matched += 1
    if matched == 0:
        raise ProtocolDifferentialError(
            f"{source}/input: manifest duration normalizer 没有匹配任何属性"
        )
    return normalized


def collect_differences(
    go: Any, rust: Any, path: str = "$", limit: int = 100
) -> list[Difference]:
    """递归精确比较 JSON 数据；wire body 字符串保持逐字节语义。

    transcript 外层 JSON object 的 key 顺序不是线协议，只用于组织 probe 结果，所以
    object 按字段名配对；`request_body`、`response_body`、`body` 和 `wire` 均是字符串，
    这里不会 parse、trim、重排或归一化其中任何字节。
    """
    differences: list[Difference] = []

    def visit(left: Any, right: Any, location: str) -> None:
        if len(differences) >= limit:
            return
        if type(left) is not type(right):
            differences.append(Difference(location, left, right))
            return
        if isinstance(left, dict):
            for key in sorted(set(left) | set(right)):
                child = f"{location}.{key}"
                if key not in left:
                    differences.append(Difference(child, "<missing>", right[key]))
                elif key not in right:
                    differences.append(Difference(child, left[key], "<missing>"))
                else:
                    visit(left[key], right[key], child)
                if len(differences) >= limit:
                    return
            return
        if isinstance(left, list):
            if len(left) != len(right):
                differences.append(
                    Difference(f"{location}.length", len(left), len(right))
                )
                if len(differences) >= limit:
                    return
            for index, (left_item, right_item) in enumerate(zip(left, right)):
                visit(left_item, right_item, f"{location}[{index}]")
                if len(differences) >= limit:
                    return
            return
        if left != right:
            differences.append(Difference(location, left, right))

    visit(go, rust, path)
    return differences


def canonical_sha256(document: dict[str, Any]) -> str:
    """计算 transcript 结构的稳定摘要，供报告和 CI 产物核对。"""
    encoded = json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _report_value(value: Any, maximum: int = 800) -> Any:
    """限制报告中的巨大 wire 文本，但比较本身始终使用完整值。"""
    if isinstance(value, str) and len(value) > maximum:
        return value[:maximum] + f"…<{len(value) - maximum} bytes omitted from report>"
    return value


def compare_directories(
    go_dir: Path, rust_dir: Path, manifest: dict[str, Any]
) -> tuple[bool, dict[str, Any]]:
    """加载七个 transcript 文档，验证覆盖集合并生成可审计差分报告。"""
    protocol_reports: dict[str, Any] = {}
    passed = True
    for protocol in PROTOCOL_NAMES:
        go_document = load_json(go_dir / f"{protocol}.json")
        rust_document = load_json(rust_dir / f"{protocol}.json")
        validate_transcript(protocol, go_document, manifest, "go")
        validate_transcript(protocol, rust_document, manifest, "rust")
        go_comparison = normalize_document(protocol, go_document, manifest, "go")
        rust_comparison = normalize_document(protocol, rust_document, manifest, "rust")
        differences = collect_differences(go_comparison, rust_comparison)
        if differences:
            passed = False
        protocol_reports[protocol] = {
            "passed": not differences,
            "go_sha256": canonical_sha256(go_document),
            "rust_sha256": canonical_sha256(rust_document),
            "differences": [
                {
                    "path": difference.path,
                    "go": _report_value(difference.go),
                    "rust": _report_value(difference.rust),
                }
                for difference in differences
            ],
        }
    return passed, {
        "schema_version": 1,
        "baseline": manifest.get("baseline", {}),
        "passed": passed,
        "protocols": protocol_reports,
    }


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    """解析 transcript 目录与报告路径。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-dir", type=Path, required=True)
    parser.add_argument("--rust-dir", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """执行 Linux ARM64 协议门禁；任何差异都返回非零。"""
    arguments = parse_arguments(sys.argv[1:] if argv is None else argv)
    if platform.system() != "Linux" or platform.machine() != "aarch64":
        raise ProtocolDifferentialError("协议差分只允许在 Linux ARM64 上运行")
    manifest = load_json(arguments.manifest)
    validate_manifest(manifest)
    passed, report = compare_directories(
        arguments.go_dir.resolve(), arguments.rust_dir.resolve(), manifest
    )
    arguments.report.parent.mkdir(parents=True, exist_ok=True)
    arguments.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    if passed:
        print(
            "差分通过（HTTP API、input、MCP、process-edge、Tunnel ProtoJSON、HTTP/WS runtime）："
            f"{arguments.report}"
        )
        return 0
    print(f"协议差分失败，完整差异见：{arguments.report}", file=sys.stderr)
    for protocol, result in report["protocols"].items():
        for difference in result["differences"][:10]:
            print(
                f"- {protocol}{difference['path'][1:]}: "
                f"Go={difference['go']!r} Rust={difference['rust']!r}",
                file=sys.stderr,
            )
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProtocolDifferentialError as error:
        print(f"协议差分前置检查失败: {error}", file=sys.stderr)
        raise SystemExit(2) from error
