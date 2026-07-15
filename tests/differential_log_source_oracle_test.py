"""Go logger source canonical override 的纯比较器单元测试。"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIRECTORY = ROOT / "scripts/compat"
sys.path.insert(0, str(SCRIPT_DIRECTORY))
SCRIPT = SCRIPT_DIRECTORY / "generate_go_log_source_oracle.py"
SPEC = importlib.util.spec_from_file_location("generate_go_log_source_oracle", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
oracle = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = oracle
SPEC.loader.exec_module(oracle)


class CanonicalLogSourceOverrideTest(unittest.TestCase):
    """验证 canonical CLI 证据只能精确修正已审核的 probe 行号。"""

    def test_committed_overrides_are_exactly_the_two_observed_sites(self) -> None:
        """目录必须只包含当前 canonical CLI 已实际触发的两个调用点。"""
        overrides = oracle.parse_canonical_overrides(
            oracle.DEFAULT_CANONICAL_OVERRIDES
        )
        self.assertEqual(
            set(overrides),
            {
                ("cmd/cmd_task_run.go", 555),
                ("internal/util/lockfile.go", 135),
            },
        )

    def test_override_changes_only_reviewed_destinations(self) -> None:
        """无关目录项保持原值，两个已审核项按 canonical 行号替换。"""
        overrides = oracle.parse_canonical_overrides(
            oracle.DEFAULT_CANONICAL_OVERRIDES
        )
        values = {key: value.probe for key, value in overrides.items()}
        unrelated = ("internal/example.go", 10)
        values[unrelated] = ("example.Function", "module/internal/example.go", 10)
        sites = [
            SimpleNamespace(path=path, line=line, end_line=line + 2)
            for path, line in overrides
        ]
        sites.append(SimpleNamespace(path=unrelated[0], line=10, end_line=10))

        actual = oracle.apply_canonical_overrides(values, sites, overrides)

        self.assertEqual(actual[unrelated], values[unrelated])
        for key, override in overrides.items():
            self.assertEqual(actual[key], override.canonical)

    def test_probe_drift_fails_closed(self) -> None:
        """probe 近似值变化后不得继续套用可能已经失效的例外。"""
        overrides = oracle.parse_canonical_overrides(
            oracle.DEFAULT_CANONICAL_OVERRIDES
        )
        key = ("cmd/cmd_task_run.go", 555)
        values = {key: ("unexpected.Function", "unexpected.go", 999)}
        sites = [SimpleNamespace(path=key[0], line=key[1], end_line=557)]
        with self.assertRaisesRegex(ValueError, "probe value drifted"):
            oracle.apply_canonical_overrides(
                values, sites, {key: overrides[key]}
            )


if __name__ == "__main__":
    unittest.main()
