"""CLI 差分比较器的无后端单元测试。"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/compat/differential_cli.py"
SPEC = importlib.util.spec_from_file_location("differential_cli", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
differential_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(differential_cli)


class DifferentialCliTest(unittest.TestCase):
    """验证精确比较与显式随机值归一化不会互相越界。"""

    def test_compared_fields_checks_exit_stdout_stderr_and_timeout(self) -> None:
        """任何一个外部可见字段变化都必须单独报告。"""
        expected = {
            "exit_code": 0,
            "stdout": "out\n",
            "stderr": "",
            "timed_out": False,
        }
        actual = {
            "exit_code": 1,
            "stdout": "other\n",
            "stderr": "error\n",
            "timed_out": True,
        }
        self.assertEqual(
            differential_cli.compared_fields(expected, actual, []),
            ["exit_code", "stdout", "stderr", "timed_out"],
        )

    def test_uuid_normalizer_only_changes_uuid(self) -> None:
        """UUID 白名单不应顺便吞掉普通数字、端口或错误文本。"""
        text = "id=123e4567-e89b-42d3-a456-426614174000 port=127.0.0.1:1234 code=17"
        self.assertEqual(
            differential_cli.normalize_text(text, ["uuid"]),
            "id=<UUID> port=127.0.0.1:1234 code=17",
        )

    def test_normalizer_must_be_declared_per_case(self) -> None:
        """未声明 normalizer 时，两个随机 UUID 仍然必须被判为差异。"""
        left = {
            "exit_code": 0,
            "stdout": "123e4567-e89b-42d3-a456-426614174000\n",
            "stderr": "",
            "timed_out": False,
        }
        right = {
            "exit_code": 0,
            "stdout": "123e4567-e89b-42d3-a456-426614174001\n",
            "stderr": "",
            "timed_out": False,
        }
        self.assertEqual(
            differential_cli.compared_fields(left, right, []), ["stdout"]
        )
        self.assertEqual(
            differential_cli.compared_fields(left, right, ["uuid"]), []
        )

    def test_temporary_directory_normalizer_is_narrow(self) -> None:
        """临时目录归一化只覆盖比较器自己的前缀，不覆盖普通用户路径。"""
        text = (
            "/tmp/env-manager-task_run_help-a1b2c3d4/home "
            "/tmp/user-session-a1b2c3d4 /home/arthur"
        )
        self.assertEqual(
            differential_cli.normalize_text(text, ["temporary_directory"]),
            "<DIFFERENTIAL_TEMP_DIR>/home /tmp/user-session-a1b2c3d4 /home/arthur",
        )

    def test_go_panic_hex_normalizer_preserves_stable_stack_bytes(self) -> None:
        """Go panic 归一化只能改地址/参数寄存值，不能吞 signal code 或 frame 元数据。"""
        text = (
            "[signal SIGSEGV: segmentation violation code=0x1 addr=0xc0 pc=0x61a128]\n"
            "github.com/anthropics/anthropic/api-go/environment-manager/internal/input."
            "(*V0Parser).Parse(0x400001, {0xa01f08, 0xfc8540}, "
            "{0x400002, 0x2b, 0x200})\n"
            "\tgithub.com/anthropics/anthropic/api-go/environment-manager/internal/input/"
            "v0_parser.go:58 +0x188\n"
            "github.com/anthropics/anthropic/api-go/environment-manager/cmd."
            "loadContextFromStdin({0xa01f08, 0xfc8540}, 0x400003, "
            "{0x400004, 0x1a}, {0x8a8721, 0x2}, {0x400005, 0x48})\n"
            "business=0xfeed\n"
        )
        self.assertEqual(
            differential_cli.normalize_text(text, ["go_panic_runtime_hex"]),
            "[signal SIGSEGV: segmentation violation code=0x1 "
            "addr=0xc0 pc=0x61a128]\n"
            "github.com/anthropics/anthropic/api-go/environment-manager/internal/input."
            "(*V0Parser).Parse(<GO_RUNTIME_HEX>, {0xa01f08, 0xfc8540}, "
            "{<GO_RUNTIME_HEX>, 0x2b, 0x200})\n"
            "\tgithub.com/anthropics/anthropic/api-go/environment-manager/internal/input/"
            "v0_parser.go:58 +0x188\n"
            "github.com/anthropics/anthropic/api-go/environment-manager/cmd."
            "loadContextFromStdin({0xa01f08, 0xfc8540}, <GO_RUNTIME_HEX>, "
            "{<GO_RUNTIME_HEX>, 0x1a}, {0x8a8721, 0x2}, "
            "{<GO_RUNTIME_HEX>, 0x48})\n"
            "business=0xfeed\n",
        )

    def test_go_panic_hex_normalizer_requires_exact_frame_shape_for_assets(self) -> None:
        """资产生成不能在 frame 参数数量漂移时退回宽泛十六进制替换。"""
        with self.assertRaises(differential_cli.DifferentialError):
            differential_cli.normalize_go_panic_runtime_hex(
                "github.com/anthropics/anthropic/api-go/environment-manager/internal/input."
                "(*V0Parser).Parse(0x1)\n",
                "0x0",
                require_all_frames=True,
            )

    def test_read_duration_normalizer_only_changes_top_level_json_number(self) -> None:
        """耗时归一化不能修改其他数字、嵌套字段、字符串或非 JSON 行。"""
        text = (
            '{"time":"fixed","msg":"Read stdin data",'
            '"read_duration_ms":17,"bytes_read":42,'
            '"nested":{"read_duration_ms":99},'
            '"text":"\\"read_duration_ms\\":123"}\n'
            '{"msg":"Other event","read_duration_ms":77,"code":6}\n'
            'not-json read_duration_ms=18 code=7\n'
            '{"read_duration_ms":"19","code":8}\n'
            '[{"read_duration_ms":20,"code":9}]\n'
        )
        self.assertEqual(
            differential_cli.normalize_text(
                text, ["structured_log_read_duration_ms"]
            ),
            '{"time":"fixed","msg":"Read stdin data",'
            '"read_duration_ms":"<READ_DURATION_MS>",'
            '"bytes_read":42,"nested":{"read_duration_ms":99},'
            '"text":"\\"read_duration_ms\\":123"}\n'
            '{"msg":"Other event","read_duration_ms":77,"code":6}\n'
            'not-json read_duration_ms=18 code=7\n'
            '{"read_duration_ms":"19","code":8}\n'
            '[{"read_duration_ms":20,"code":9}]\n',
        )

    def test_read_duration_normalizer_is_explicitly_case_scoped(self) -> None:
        """耗时归一化只能由 v0 missing startup panic 用例启用。"""
        manifest = differential_cli.load_json(differential_cli.DEFAULT_CASES)
        enabled_cases = [
            case["name"]
            for case in manifest["cases"]
            if "structured_log_read_duration_ms" in case.get("normalizers", [])
        ]
        self.assertEqual(
            enabled_cases, ["task_run_v0_missing_startup_context_panic"]
        )
        differential_cli.validate_manifest(manifest)

        invalid_manifest = {
            "schema_version": 1,
            "cases": [
                {
                    "name": "unrelated_case",
                    "argv0": "environment-runner",
                    "args": [],
                    "stdin": "",
                    "normalizers": ["structured_log_read_duration_ms"],
                }
            ],
        }
        with self.assertRaises(differential_cli.DifferentialError):
            differential_cli.validate_manifest(invalid_manifest)

    def test_manifest_rejects_unknown_normalizer(self) -> None:
        """宽泛或拼错的 normalizer 不能被静默忽略。"""
        manifest = {
            "schema_version": 1,
            "cases": [
                {
                    "name": "bad",
                    "argv0": "environment-runner",
                    "args": [],
                    "stdin": "",
                    "normalizers": ["erase-all-errors"],
                }
            ],
        }
        with self.assertRaises(differential_cli.DifferentialError):
            differential_cli.validate_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
