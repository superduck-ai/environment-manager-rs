#!/usr/bin/env python3
"""从固定 Go ARM64 证据生成每个 slog 调用的 runtime source 目录。"""

from __future__ import annotations

import argparse
import os
import platform
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# 防止 Linux 门禁运行此脚本时把 Python 缓存写进 rsync 源码目录。
sys.dont_write_bytecode = True

from generate_log_source_map import MODULE_PATH, GoSite, collect_go_sites


HEADER = "origin_path\torigin_line\tgo_function\tgo_file\tgo_line\tevidence"
EVIDENCE = "go1.25.7-trimpath-linux-arm64"
OVERRIDE_HEADER = (
    "origin_path\torigin_line\tprobe_function\tprobe_file\tprobe_line\t"
    "canonical_function\tcanonical_file\tcanonical_line\tevidence"
)
DEFAULT_CANONICAL_OVERRIDES = (
    Path(__file__).resolve().parents[2]
    / "compat/go-log-source-canonical-overrides.tsv"
)


@dataclass(frozen=True)
class CanonicalSourceOverride:
    """一条由 canonical Go CLI 实际输出确认的 source 覆盖证据。"""

    probe: tuple[str, str, int]
    canonical: tuple[str, str, int]
    evidence: str


def parse_canonical_overrides(
    path: Path,
) -> dict[tuple[str, int], CanonicalSourceOverride]:
    """读取并严格校验 canonical CLI source 覆盖目录。

    catalog probe 需要向 root package 注入一个不可达的 init，且用调用表达式内的
    候选 PC 近似真实 ``slog.Record.PC``。极少数多行调用会因此落到最后一个属性行，
    而未注入文件的 canonical CLI 会稳定报告调用起始行。这里只允许逐项记录这种
    差异；每项同时钉住 probe 原值，防止工具链或生成算法漂移后继续盲目覆盖。
    """

    if not path.is_file():
        raise ValueError(f"canonical Go source override catalog does not exist: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != OVERRIDE_HEADER:
        raise ValueError(f"invalid canonical Go source override header: {path}")
    result: dict[tuple[str, int], CanonicalSourceOverride] = {}
    for number, value in enumerate(lines[1:], start=2):
        columns = value.split("\t")
        if len(columns) != 9 or not columns[8]:
            raise ValueError(
                f"invalid canonical Go source override row {path}:{number}"
            )
        try:
            origin_line = int(columns[1])
            probe_line = int(columns[4])
            canonical_line = int(columns[7])
        except ValueError as error:
            raise ValueError(
                f"invalid canonical Go source override line number {path}:{number}"
            ) from error
        if min(origin_line, probe_line, canonical_line) <= 0:
            raise ValueError(
                f"non-positive canonical Go source override line {path}:{number}"
            )
        probe = (columns[2], columns[3], probe_line)
        canonical = (columns[5], columns[6], canonical_line)
        # 已观察到的编译差异只允许改变行号。函数或文件变化代表关联错位，必须先
        # 修复 probe/映射算法，不能靠 override 隐藏。
        if probe[:2] != canonical[:2]:
            raise ValueError(
                f"canonical override may only change line number {path}:{number}"
            )
        key = (columns[0], origin_line)
        override = CanonicalSourceOverride(probe, canonical, columns[8])
        previous = result.setdefault(key, override)
        if previous != override:
            raise ValueError(f"ambiguous canonical Go source override {path}:{number}")
    return result


def apply_canonical_overrides(
    values: dict[tuple[str, int], tuple[str, str, int]],
    sites: Iterable[GoSite],
    overrides: dict[tuple[str, int], CanonicalSourceOverride],
) -> dict[tuple[str, int], tuple[str, str, int]]:
    """让可执行的 canonical CLI 证据优先于注入式 catalog probe 近似值。"""

    site_index: dict[tuple[str, int], GoSite] = {}
    for site in sites:
        key = (site.path, site.line)
        if key in site_index:
            raise ValueError(f"ambiguous Go site for canonical source override: {key}")
        site_index[key] = site
    result = dict(values)
    for key, override in overrides.items():
        site = site_index.get(key)
        if site is None:
            raise ValueError(f"canonical source override no longer identifies a Go site: {key}")
        actual_probe = result.get(key)
        if actual_probe != override.probe:
            raise ValueError(
                "canonical source override probe value drifted: "
                f"{key}: expected {override.probe}, got {actual_probe}"
            )
        canonical_line = override.canonical[2]
        if not site.line <= canonical_line <= site.end_line:
            raise ValueError(
                "canonical source override is outside its Go call expression: "
                f"{key}: {canonical_line} not in {site.line}..{site.end_line}"
            )
        result[key] = override.canonical
    return result


def normalized_go_path(value: str) -> str | None:
    """把 objdump 行表中的 trimpath 文件名转换为 Go 工作树相对路径。"""

    marker = f"{MODULE_PATH}/"
    if marker in value:
        return value.split(marker, 1)[1]
    if value.startswith("/src/"):
        return value.removeprefix("/src/")
    if value.startswith("./"):
        return value.removeprefix("./")
    return value if not value.startswith("/") else None


def runtime_go_file(value: str, relative: str) -> str:
    """返回 `slog.Source.File` 应使用的固定 trimpath 文件名。"""

    if value.startswith(f"{MODULE_PATH}/"):
        return value
    return f"{MODULE_PATH}/{relative}"


def canonical_function(value: str) -> str:
    """清理 objdump 只用于 ABI 展示、不会出现在 runtime.Func 名称中的后缀。"""

    return re.sub(r"\.abi[0-9]+$", "", value).replace("·", ".")


def parse_runtime_frames(path: Path) -> dict[int, tuple[str, str, int]]:
    """读取同一固定 ELF 自身 runtime 解析出的 project caller frame。"""

    result: dict[int, tuple[str, str, int]] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        columns = raw.split("\t")
        if len(columns) != 5 or columns[1] != "0":
            raise ValueError(f"invalid runtime frame row {path}:{number}")
        pc = int(columns[0], 16)
        destination = (columns[2], columns[3], int(columns[4]))
        previous = result.setdefault(pc, destination)
        if previous != destination:
            raise ValueError(f"ambiguous runtime frame row {path}:{number}")
    return result


def candidate_source_pcs(lines: Iterable[str], sites: list[GoSite]) -> list[int]:
    """提取位于任一 Go slog 调用表达式内的 ARM64 指令地址。

    先输出候选集再运行 probe，可避免用静态函数名猜测 func literal 编号。候选集
    允许包含同基名文件的额外 PC；正式关联阶段仍会按 TEXT package 唯一化。
    """

    ranges: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for site in sites:
        ranges[Path(site.path).name].append((site.line, site.end_line))
    location_pattern = re.compile(r"(?P<file>\S+\.go):(?P<line>[0-9]+)")
    pc_pattern = re.compile(r"\s(?P<pc>0x[0-9a-f]+)\s")
    result: set[int] = set()
    for line in lines:
        location = location_pattern.search(line)
        pc = pc_pattern.search(line)
        if location is None or pc is None:
            continue
        source_line = int(location.group("line"))
        if any(
            start <= source_line <= end
            for start, end in ranges.get(Path(location.group("file")).name, [])
        ):
            result.add(int(pc.group("pc"), 16))
    return sorted(result)


def parse_objdump_lines(
    lines: Iterable[str],
    sites: list[GoSite],
    runtime_frames: dict[int, tuple[str, str, int]],
) -> list[tuple[str, int, str, str]]:
    """解析 objdump，并把内联 slog 的最终 CALL 关联回项目调用点。

    Go 1.25 会把 ``DebugContext`` 等公开方法内联进业务函数，所以最终指令的
    行表是 ``log/slog/logger.go``，不能直接作为 ``slog.Source``。同一内联区段
    开头仍保留项目文件/调用行；这里跟踪 CALL 前最后一个落入静态调用表达式范围
    的项目位置，等价于 runtime 跳过 slog inline frame 后使用的 caller。
    """

    current_function = ""
    current_site: GoSite | None = None
    current_site_pc: int | None = None
    calls: list[tuple[str, int, str, str]] = []
    text_pattern = re.compile(r"^TEXT\s+(.+?)\(SB\)")
    location_pattern = re.compile(r"(?P<file>\S+\.go):(?P<line>[0-9]+)")
    pc_pattern = re.compile(r"\s(?P<pc>0x[0-9a-f]+)\s")
    final_slog_call = re.compile(
        r"CALL log/slog\.\(\*Logger\)\.(?:log|logAttrs)\(SB\)"
    )
    by_basename: dict[str, list[GoSite]] = defaultdict(list)
    for site in sites:
        by_basename[Path(site.path).name].append(site)

    for line in lines:
        text_match = text_pattern.search(line)
        if text_match:
            current_function = canonical_function(text_match.group(1))
            current_site = None
            current_site_pc = None
            continue
        if not current_function:
            continue
        location = location_pattern.search(line)
        if location is not None:
            basename = Path(location.group("file")).name
            source_line = int(location.group("line"))
            candidates = [
                site
                for site in by_basename.get(basename, [])
                if site.line <= source_line <= site.end_line
            ]
            # action.go 等基名会在多个包重复；当前 TEXT 的完整 package 名可以
            # 消除这种歧义，同时允许业务 helper 被内联到同包调用者。
            package_candidates = [
                site
                for site in candidates
                if current_function.startswith(
                    f"{MODULE_PATH}/{Path(site.path).parent.as_posix()}."
                )
            ]
            if len(package_candidates) == 1:
                current_site = package_candidates[0]
                pc_match = pc_pattern.search(line)
                current_site_pc = int(pc_match.group("pc"), 16) if pc_match else None
            elif len(candidates) == 1:
                current_site = candidates[0]
                pc_match = pc_pattern.search(line)
                current_site_pc = int(pc_match.group("pc"), 16) if pc_match else None

        if final_slog_call.search(line):
            site = current_site
            site_pc = current_site_pc
            # 一个最终 slog CALL 只能消费一个项目调用点；不清空会让紧随其后的
            # 无参数简单调用错误复用前一条 location，形成伪重复证据。
            current_site = None
            current_site_pc = None
            if site is None or site_pc is None:
                calls.append(("<unmatched>", 0, current_function, line.rstrip("\n")))
                continue
            destination = runtime_frames.get(site_pc)
            if destination is None:
                calls.append(
                    (
                        "<unmatched>",
                        0,
                        current_function,
                        f"missing runtime frame for PC {site_pc:#x}: {line.rstrip()}",
                    )
                )
                continue
            function, file, source_line = destination
            calls.append(
                (
                    file,
                    source_line,
                    function,
                    (
                        f"{line.rstrip()} [TEXT {current_function}; "
                        f"SOURCE_PC {site_pc:#x}]"
                    ),
                )
            )
    return calls


def objdump_slog_calls(
    go_tool: str,
    binary: Path,
    sites: list[GoSite],
    runtime_frames: dict[int, tuple[str, str, int]],
) -> list[tuple[str, int, str, str]]:
    """流式调用固定 Go objdump，避免把完整反汇编一次性读进内存。"""

    expression = re.escape(MODULE_PATH) + r"/.*"
    process = subprocess.Popen(
        [go_tool, "tool", "objdump", "-s", expression, os.fspath(binary)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    assert process.stdout is not None
    calls = parse_objdump_lines(process.stdout, sites, runtime_frames)
    stderr = ""
    if process.stderr is not None:
        stderr = process.stderr.read()
    status = process.wait()
    if status != 0:
        raise RuntimeError(f"go tool objdump failed ({status}): {stderr.strip()}")
    return calls


def match_sites(
    sites: list[GoSite], calls: list[tuple[str, int, str, str]]
) -> tuple[dict[tuple[str, int], tuple[str, str, int]], list[str]]:
    """把每条 slog CALL 指令关联回唯一源码调用表达式，并报告所有证据缺口。"""

    by_path: dict[str, list[GoSite]] = defaultdict(list)
    for site in sites:
        by_path[site.path].append(site)
    destinations: dict[tuple[str, int], set[tuple[str, str, int]]] = defaultdict(set)
    unmatched_calls: list[str] = []
    unmatched_by_function: dict[str, list[str]] = defaultdict(list)
    for file, line, function, evidence_line in calls:
        if file == "<unmatched>":
            unmatched_by_function[function].append(evidence_line)
            continue
        relative = normalized_go_path(file)
        if relative is None:
            unmatched_calls.append(f"无法归一化 objdump 文件: {evidence_line}")
            continue
        candidates = [
            site
            for site in by_path.get(relative, [])
            if site.line <= line <= site.end_line
        ]
        if len(candidates) != 1:
            unmatched_calls.append(
                f"objdump CALL 未匹配唯一源码调用 ({relative}:{line}): {evidence_line}"
            )
            continue
        site = candidates[0]
        destinations[(site.path, site.line)].add(
            (function, runtime_go_file(file, relative), line)
        )

    # 无属性的单行 slog 调用有时从第一条指令起就全部属于内联 logger.go，objdump
    # 不留下 project source PC。此时固定 ELF 仍留下一个最终 CALL 和精确 TEXT 名；
    # 仅当该函数内“未消费 Go site 数量 == 无 location CALL 数量”时按源码顺序配对。
    # 任一数量不等都保留为 blocker，禁止猜测或跨函数借用证据。
    for function, evidence_lines in unmatched_by_function.items():
        missing_sites = sorted(
            (
                site
                for site in sites
                if site.function == function
                and not destinations.get((site.path, site.line))
            ),
            key=lambda site: (site.path, site.line),
        )
        if len(missing_sites) != len(evidence_lines):
            unmatched_calls.extend(
                f"无 project PC 的 slog CALL 无法唯一配对 ({function}): {line}"
                for line in evidence_lines
            )
            continue
        for site, _evidence_line in zip(missing_sites, evidence_lines, strict=True):
            destinations[(site.path, site.line)].add(
                (function, f"{MODULE_PATH}/{site.path}", site.line)
            )

    result: dict[tuple[str, int], tuple[str, str, int]] = {}
    gaps = list(unmatched_calls)
    for site in sites:
        key = (site.path, site.line)
        values = destinations.get(key, set())
        if len(values) == 1:
            result[key] = next(iter(values))
        elif not values:
            gaps.append(f"ELF 中没有 slog CALL 证据: {site.path}:{site.line}")
        else:
            gaps.append(f"ELF source 歧义 {site.path}:{site.line}: {sorted(values)}")
    return result, gaps


def render(values: dict[tuple[str, int], tuple[str, str, int]]) -> str:
    """按 Go 源路径和调用行稳定输出 oracle TSV。"""

    lines = [HEADER]
    for (path, origin_line), (function, file, line) in sorted(values.items()):
        lines.append(
            f"{path}\t{origin_line}\t{function}\t{file}\t{line}\t{EVIDENCE}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    """校验平台/工具链，生成目录，并在任一生产调用缺证据时失败。"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--go-root", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--go-tool", default="go")
    parser.add_argument(
        "--objdump",
        type=Path,
        help="由固定 digest Go 1.25.7 容器预先产生的 objdump；提供后宿主无需安装 Go",
    )
    parser.add_argument(
        "--verified-go-version",
        help="runner 已核对的精确 `go version` 文本；使用 --objdump 时必须提供",
    )
    parser.add_argument(
        "--runtime-frames",
        type=Path,
        help="由同一固定 probe ELF 通过 runtime.CallersFrames 解析的 source PC TSV",
    )
    parser.add_argument(
        "--source-pcs-output",
        type=Path,
        help="只从 --objdump 提取候选 project caller PC，供固定 probe 生成 runtime frames",
    )
    parser.add_argument(
        "--canonical-overrides",
        type=Path,
        default=DEFAULT_CANONICAL_OVERRIDES,
        help="canonical Go CLI 实际 source 优先证据目录",
    )
    parser.add_argument("--allow-incomplete", action="store_true")
    arguments = parser.parse_args()

    if platform.system() != "Linux" or platform.machine() != "aarch64":
        parser.error("Go log source oracle 只允许在 Linux ARM64 上生成")
    if arguments.objdump is not None:
        version = arguments.verified_go_version or ""
    else:
        version = subprocess.run(
            [arguments.go_tool, "version"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.strip()
    if version != "go version go1.25.7 linux/arm64":
        parser.error(f"固定 Go 工具链不符: {version}")
    if not (arguments.go_root / "go.mod").is_file():
        parser.error(f"Go 基线不可用: {arguments.go_root}")
    if not arguments.binary.is_file():
        parser.error(f"Go ARM64 ELF 不存在: {arguments.binary}")

    sites = collect_go_sites(arguments.go_root.resolve())
    if arguments.source_pcs_output is not None:
        if arguments.objdump is None or not arguments.objdump.is_file():
            parser.error("--source-pcs-output requires an existing --objdump")
        with arguments.objdump.open(encoding="utf-8") as dump:
            pcs = candidate_source_pcs(dump, sites)
        arguments.source_pcs_output.parent.mkdir(parents=True, exist_ok=True)
        arguments.source_pcs_output.write_text(
            "".join(f"{pc:#x}\n" for pc in pcs), encoding="utf-8"
        )
        print(f"Go log source candidate PCs: {len(pcs)}", file=sys.stderr)
        return 0
    if arguments.runtime_frames is None:
        parser.error("--runtime-frames is required when generating the oracle")
    try:
        runtime_frames = parse_runtime_frames(arguments.runtime_frames)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    if arguments.objdump is not None:
        if not arguments.objdump.is_file():
            parser.error(f"objdump 证据不存在: {arguments.objdump}")
        with arguments.objdump.open(encoding="utf-8") as dump:
            calls = parse_objdump_lines(dump, sites, runtime_frames)
    else:
        calls = objdump_slog_calls(
            arguments.go_tool,
            arguments.binary.resolve(),
            sites,
            runtime_frames,
        )
    values, gaps = match_sites(sites, calls)
    try:
        overrides = parse_canonical_overrides(arguments.canonical_overrides)
        values = apply_canonical_overrides(values, sites, overrides)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    for gap in gaps:
        print(f"blocker: {gap}", file=sys.stderr)
    print(
        f"Go log source oracle: {len(values)}/{len(sites)} calls mapped; "
        f"{len(gaps)} evidence gaps; {len(overrides)} canonical overrides",
        file=sys.stderr,
    )
    if gaps and not arguments.allow_incomplete:
        return 2

    content = render(values)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=arguments.output.parent,
        delete=False,
    ) as output:
        output.write(content)
        temporary = Path(output.name)
    temporary.replace(arguments.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
