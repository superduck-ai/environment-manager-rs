"""协议差分比较器的无网络单元测试。"""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/compat/differential_protocol.py"
SPEC = importlib.util.spec_from_file_location("differential_protocol", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
differential_protocol = importlib.util.module_from_spec(SPEC)
# dataclass 在创建类时会查询模块命名空间；显式登记可模拟正常 import 语义。
sys.modules[SPEC.name] = differential_protocol
SPEC.loader.exec_module(differential_protocol)


class DifferentialProtocolTest(unittest.TestCase):
    """验证覆盖门禁和 raw wire 精确比较不会产生假阳性。"""

    def test_raw_json_wire_order_is_not_normalized(self) -> None:
        """body 内字段相同但字节顺序不同仍必须报告差异。"""
        go = {"cases": [{"name": "wire", "response_body": '{"a":1,"b":2}\n'}]}
        rust = {
            "cases": [{"name": "wire", "response_body": '{"b":2,"a":1}\n'}]
        }
        differences = differential_protocol.collect_differences(go, rust)
        self.assertEqual(len(differences), 1)
        self.assertEqual(differences[0].path, "$.cases[0].response_body")

    def test_outer_object_key_order_is_not_wire_data(self) -> None:
        """仅用于组织 transcript 的外层 object 顺序不应制造差异。"""
        left = {"protocol": "mcp", "schema_version": 1}
        right = {"schema_version": 1, "protocol": "mcp"}
        self.assertEqual(
            differential_protocol.collect_differences(left, right), []
        )

    def test_manifest_accepts_only_reviewed_input_duration_normalizer(self) -> None:
        """仅允许 input parse_duration_ms 的已审核路径规则。"""
        manifest = {
            "schema_version": 1,
            "normalizers": [
                dict(differential_protocol.INPUT_DURATION_NORMALIZER)
            ],
            "protocols": {
                "http_api": {},
                "input": {},
                "mcp": {},
                "process_edge": {},
                "tunnel_protojson": {},
                "tunnel_http_runtime": {},
                "tunnel_ws_runtime": {},
            },
        }
        differential_protocol.validate_manifest(manifest)
        manifest["normalizers"][0]["kind"] = "erase_errors"
        with self.assertRaises(differential_protocol.ProtocolDifferentialError):
            differential_protocol.validate_manifest(manifest)

    def test_input_duration_normalizer_preserves_every_other_value(self) -> None:
        """duration 可不同，但相邻业务整数和原始输入必须继续逐值比较。"""
        manifest = {
            "normalizers": [
                dict(differential_protocol.INPUT_DURATION_NORMALIZER)
            ]
        }
        document = {
            "cases": [
                {
                    "input_wire_base64": "e30=",
                    "logs": [
                        {
                            "attributes": [
                                {"name": "data_size_bytes", "value": 2},
                                {"name": "parse_duration_ms", "value": 7},
                            ]
                        }
                    ],
                }
            ]
        }
        normalized = differential_protocol.normalize_document(
            "input", document, manifest, "go"
        )
        self.assertEqual(document["cases"][0]["logs"][0]["attributes"][1]["value"], 7)
        self.assertEqual(normalized["cases"][0]["logs"][0]["attributes"][0]["value"], 2)
        self.assertEqual(
            normalized["cases"][0]["logs"][0]["attributes"][1]["value"],
            "<nonnegative_integer>",
        )

    def test_input_duration_normalizer_rejects_negative_or_boolean(self) -> None:
        """非负 JSON integer 以外的 duration 不能借 normalizer 混过门禁。"""
        manifest = {
            "normalizers": [
                dict(differential_protocol.INPUT_DURATION_NORMALIZER)
            ]
        }
        for invalid in (-1, True, "0"):
            document = {
                "cases": [
                    {
                        "logs": [
                            {
                                "attributes": [
                                    {"name": "parse_duration_ms", "value": invalid}
                                ]
                            }
                        ]
                    }
                ]
            }
            with self.assertRaises(differential_protocol.ProtocolDifferentialError):
                differential_protocol.normalize_document(
                    "input", document, manifest, "rust"
                )

    def test_validate_transcript_rejects_deleted_case(self) -> None:
        """Go/Rust 同时漏掉用例也不能绕过 manifest 固定的覆盖集合。"""
        manifest = {
            "protocols": {"mcp": {"case_names": ["initialize", "tools_call"]}}
        }
        document = {
            "schema_version": 1,
            "protocol": "mcp",
            "cases": [{"name": "initialize"}],
        }
        with self.assertRaises(differential_protocol.ProtocolDifferentialError):
            differential_protocol.validate_transcript(
                "mcp", document, manifest, "go"
            )

    def test_validate_transcript_rejects_cleaned_dot_segments(self) -> None:
        """URL 库若清理 `.`/`..`，即使请求仍到达 mock 也必须在门禁失败。"""
        manifest = {
            "protocols": {
                "http_api": {
                    "request_targets": [
                        "/v1/environments/./work/../ack"
                    ],
                    "outcome_names": ["work_ack_dot_segments"],
                }
            }
        }
        document = {
            "schema_version": 1,
            "protocol": "http_api",
            "requests": [
                {
                    # 这是常见 URL parser 清理后的错误结果；比较器必须逐字节拒绝，
                    # 不能把它归一化回 Go 基线的原始 request-target。
                    "target": "/v1/environments/ack",
                }
            ],
            "outcomes": [{"name": "work_ack_dot_segments"}],
        }
        with self.assertRaises(differential_protocol.ProtocolDifferentialError):
            differential_protocol.validate_transcript(
                "http_api", document, manifest, "rust"
            )

    def test_validate_runtime_transcript_rejects_reordered_case(self) -> None:
        """真实 Tunnel runtime 场景即使双方一起重排也必须被 manifest 拒绝。"""
        manifest = {
            "protocols": {
                "tunnel_http_runtime": {
                    "case_names": ["stream_flush_and_request", "cancel"]
                }
            }
        }
        document = {
            "schema_version": 1,
            "protocol": "tunnel_http_runtime",
            "cases": [
                {"name": "cancel"},
                {"name": "stream_flush_and_request"},
            ],
        }
        with self.assertRaises(differential_protocol.ProtocolDifferentialError):
            differential_protocol.validate_transcript(
                "tunnel_http_runtime", document, manifest, "go"
            )

    def test_compare_directories_reports_exact_path(self) -> None:
        """目录级比较应指出协议、case 下标和原始 response body 路径。"""
        manifest = {
            "schema_version": 1,
            "normalizers": [
                dict(differential_protocol.INPUT_DURATION_NORMALIZER)
            ],
            "baseline": {},
            "protocols": {
                "http_api": {
                    "request_targets": ["/ok"],
                    "outcome_names": ["success"],
                },
                "input": {"case_names": ["v0_full"]},
                "mcp": {"case_names": ["initialize"]},
                "process_edge": {"case_names": ["lookpath_missing"]},
                "tunnel_protojson": {"case_names": ["ws_open"]},
                "tunnel_http_runtime": {
                    "case_names": ["stream_flush_and_request"]
                },
                "tunnel_ws_runtime": {
                    "case_names": ["success_frames_and_close"]
                },
            },
        }
        documents = {
            "http_api": {
                "schema_version": 1,
                "protocol": "http_api",
                "requests": [{"target": "/ok", "body": "{}"}],
                "outcomes": [{"name": "success", "error": ""}],
            },
            "input": {
                "schema_version": 1,
                "protocol": "input",
                "cases": [
                    {
                        "name": "v0_full",
                        "input_wire_base64": "e30=",
                        "logs": [
                            {
                                "attributes": [
                                    {"name": "parse_duration_ms", "value": 0}
                                ]
                            }
                        ],
                    }
                ],
            },
            "mcp": {
                "schema_version": 1,
                "protocol": "mcp",
                "cases": [{"name": "initialize", "response_body": "go"}],
            },
            "process_edge": {
                "schema_version": 1,
                "protocol": "process_edge",
                "cases": [{"name": "lookpath_missing", "lines": ["exact"]}],
            },
            "tunnel_protojson": {
                "schema_version": 1,
                "protocol": "tunnel_protojson",
                "cases": [{"name": "ws_open", "wire": "{}"}],
            },
            "tunnel_http_runtime": {
                "schema_version": 1,
                "protocol": "tunnel_http_runtime",
                "cases": [{"name": "stream_flush_and_request", "events": []}],
            },
            "tunnel_ws_runtime": {
                "schema_version": 1,
                "protocol": "tunnel_ws_runtime",
                "cases": [{"name": "success_frames_and_close", "events": []}],
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            go_dir = root / "go"
            rust_dir = root / "rust"
            go_dir.mkdir()
            rust_dir.mkdir()
            for protocol, document in documents.items():
                (go_dir / f"{protocol}.json").write_text(
                    json.dumps(document), encoding="utf-8"
                )
                rust_document = json.loads(json.dumps(document))
                if protocol == "mcp":
                    rust_document["cases"][0]["response_body"] = "rust"
                (rust_dir / f"{protocol}.json").write_text(
                    json.dumps(rust_document), encoding="utf-8"
                )

            passed, report = differential_protocol.compare_directories(
                go_dir, rust_dir, manifest
            )
        self.assertFalse(passed)
        self.assertEqual(
            report["protocols"]["mcp"]["differences"][0]["path"],
            "$.cases[0].response_body",
        )


if __name__ == "__main__":
    unittest.main()
