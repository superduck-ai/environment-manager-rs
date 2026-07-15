"""process-edge raw transcript 转换器的无进程单元测试。"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/compat/process_edge_transcript.py"
SPEC = importlib.util.spec_from_file_location("process_edge_transcript", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
process_edge_transcript = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = process_edge_transcript
SPEC.loader.exec_module(process_edge_transcript)


class ProcessEdgeTranscriptTest(unittest.TestCase):
    """确保动态归一化范围不会扩大到错误、状态或任意路径。"""

    def test_temp_root_replaces_only_declared_exact_hex_three_times(self) -> None:
        """只替换声明根的完整 hex；相邻但不同的路径必须保留。"""
        root = b"/tmp/probe-123".hex()
        other = b"/tmp/probe-124".hex()
        lines = [f"line-{index}={other}" for index in range(9)]
        lines[5] = f"a={root}2fff"
        lines[7] = f"b=00{root}"
        lines[8] = f"c={root}"
        normalized = process_edge_transcript.normalize_temporary_root(lines, root)
        marker = process_edge_transcript.TEMP_ROOT_MARKER_HEX
        self.assertEqual(normalized[5], f"a={marker}2fff")
        self.assertEqual(normalized[7], f"b=00{marker}")
        self.assertEqual(normalized[8], f"c={marker}")
        self.assertEqual(normalized[0], f"line-0={other}")

    def test_temp_root_rejects_missing_covered_edge(self) -> None:
        """少一个 raw-path case 时不能借 normalizer 让残缺 probe 通过。"""
        root = b"/tmp/probe".hex()
        lines = [f"line-{index}=stable" for index in range(9)]
        lines[5] = f"a={root}"
        lines[7] = f"b={root}"
        with self.assertRaises(process_edge_transcript.ProcessEdgeTranscriptError):
            process_edge_transcript.normalize_temporary_root(lines, root)

    def test_temp_root_rejects_occurrence_in_unreviewed_line(self) -> None:
        """总数即使仍为三次，只要位置越界也必须拒绝。"""
        root = b"/tmp/probe".hex()
        lines = [f"line-{index}=stable" for index in range(9)]
        lines[5] = f"a={root}"
        lines[7] = f"b={root}"
        lines[6] = f"unreviewed.error={root}"
        with self.assertRaises(process_edge_transcript.ProcessEdgeTranscriptError):
            process_edge_transcript.normalize_temporary_root(lines, root)

    def test_source_fd_normalizer_requires_three_cloexec_high_fds(self) -> None:
        """仅允许三个不同高位 FD；CLOEXEC false 或低位 FD 必须拒绝。"""
        valid = [
            "extrafiles.parent.sources=64,65,66 cloexec=true,true,true",
            "unchanged=signal: killed",
        ]
        self.assertEqual(
            process_edge_transcript.normalize_source_fds(valid),
            [
                "extrafiles.parent.sources={source_fd_0},{source_fd_1},{source_fd_2} "
                "cloexec=true,true,true",
                "unchanged=signal: killed",
            ],
        )
        for invalid in (
            "extrafiles.parent.sources=3,4,5 cloexec=true,true,true",
            "extrafiles.parent.sources=64,64,66 cloexec=true,true,true",
            "extrafiles.parent.sources=64,65,66 cloexec=true,false,true",
        ):
            with self.subTest(line=invalid):
                with self.assertRaises(process_edge_transcript.ProcessEdgeTranscriptError):
                    process_edge_transcript.normalize_source_fds([invalid])

    def test_converter_fixes_case_order_and_removes_toolchain_identity(self) -> None:
        """Go/Rust toolchain头不同，但完整语义行必须生成同一固定 case 文档。"""
        root = b"/tmp/process-edge-dynamic".hex()
        semantic_lines: list[str] = []
        for _, prefixes in process_edge_transcript.CASE_GROUPS:
            for prefix in prefixes:
                if prefix == "extrafiles.parent.sources=":
                    semantic_lines.append(
                        "extrafiles.parent.sources=80,81,82 cloexec=true,true,true"
                    )
                else:
                    semantic_lines.append(prefix + "stable")
        # 转换器要求三个固定 raw-path case 都实际引用声明根；这里只替换值部分，
        # 不改变 manifest 审核的行前缀。
        for index in (5, 7, 8):
            semantic_lines[index] += root

        def raw(toolchain: str) -> bytes:
            return (
                "\n".join(
                    [toolchain, f"dynamic.temp-root.hex={root}", *semantic_lines]
                )
                + "\n"
            ).encode("ascii")

        go = process_edge_transcript.convert_raw("go", raw("go.version=go1.25.7"))
        rust = process_edge_transcript.convert_raw("rust", raw("rust.version=1.97.0"))
        self.assertEqual(go, rust)
        self.assertEqual(
            [case["name"] for case in go["cases"]],
            [name for name, _ in process_edge_transcript.CASE_GROUPS],
        )


if __name__ == "__main__":
    unittest.main()
