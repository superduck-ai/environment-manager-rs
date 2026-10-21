#!/usr/bin/env python3
"""构建当前 Rust 服务，对照现成 x86-64 Go 二进制，输出完整 MCP 功能报告。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent


def command(args):
    """执行参数数组，任何构建或容器错误都使验收失败。"""
    print('+', ' '.join(map(str, args)), flush=True)
    subprocess.run(list(map(str, args)), check=True)


def compare(go, rust):
    """比较逐步响应、副作用和独立断言；缺失步骤不能被忽略。"""
    failures = []
    if go['requests'] != rust['requests']:
        failures.append({'reason': 'different MCP request counts', 'go': go['requests'], 'rust': rust['requests']})
    for dependency in ('bash', 'ripgrep'):
        if go['runtime'][dependency] != rust['runtime'][dependency]:
            failures.append({'reason': f'different {dependency} versions', 'go': go['runtime'][dependency], 'rust': rust['runtime'][dependency]})
    for label, report in [('go', go), ('rust', rust)]:
        for failure in report['assertion_failures']:
            failures.append({'implementation': label, **failure})
    left = {step['name']: step for step in go['steps']}
    right = {step['name']: step for step in rust['steps']}
    if len(left) != len(go['steps']) or len(right) != len(rust['steps']):
        failures.append({'reason': 'duplicate case names'})
    for name in dict.fromkeys([*left, *right]):
        a, b = left.get(name), right.get(name)
        if json.dumps(a, sort_keys=True, ensure_ascii=False) != json.dumps(b, sort_keys=True, ensure_ascii=False):
            failures.append({'name': name, 'go': a, 'rust': b})
    return failures


def main():
    """使用隔离容器运行两份相同测试，始终记录二进制 SHA256。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go-binary', type=Path, required=True)
    parser.add_argument('--rust-build-image', default=os.environ.get('MCP_RUST_BUILD_IMAGE', 'rust:1.97.0-bookworm'))
    parser.add_argument('--profile', choices=('release', 'debug'), default='release')
    parser.add_argument('--output', type=Path, default=ROOT / 'target/mcp-binary-e2e')
    args = parser.parse_args()
    source = args.go_binary.resolve(strict=True)
    header = source.read_bytes()[:20]
    if header[:4] != b'\x7fELF' or header[4:6] != b'\x02\x01' or header[18:20] != b'\x3e\x00':
        parser.error('--go-binary must be an ELF64 little-endian x86-64 executable')
    arch = subprocess.check_output(['docker', 'info', '--format', '{{.Architecture}}'], text=True).strip()
    platforms = {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64', 'amd64': 'amd64'}
    native = platforms.get(arch)
    if native is None:
        parser.error(f'unsupported Docker architecture: {arch}')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    for platform in sorted({'amd64', native}):
        command(['docker', 'build', '--platform', f'linux/{platform}', '-t', f'mcp-binary-e2e:{platform}', HERE])
    # 仅编译 Rust；不挂载 Go 源码，也不调用 Go 工具链。
    command(['docker', 'run', '--rm', '--platform', f'linux/{native}', '--entrypoint', 'bash',
             '-v', f'{ROOT}:/workspace', '-v', 'mcp-rust-cargo-cache:/cargo-cache',
             '-w', '/workspace', args.rust_build_image, '-lc',
             'CARGO_HOME=/cargo-cache CARGO_TARGET_DIR=/workspace/target/binary-e2e-linux cargo build --locked -p mcp-server'
             + (' --release' if args.profile == 'release' else '')])
    rust = ROOT / f'target/binary-e2e-linux/{args.profile}/mcp-server'
    reports = {}
    for label, binary, platform in [('go', source, 'amd64'), ('rust', rust, native)]:
        # 只读挂载输入，chmod 只作用于即将销毁的容器副本。
        command(['docker', 'run', '--rm', '--platform', f'linux/{platform}', '--init',
                 '-v', f'{binary}:/input/mcp-server:ro', '-v', f'{HERE}:/suite:ro',
                 '-v', f'{ROOT}/crates/mcp-server/tests/fixtures:/image-fixtures:ro',
                 '-v', f'{output}:/reports', f'mcp-binary-e2e:{platform}', 'bash', '-lc',
                 f'cp /input/mcp-server /tmp/mcp-server && chmod 755 /tmp/mcp-server && '
                 f'python3 /suite/suite.py /tmp/mcp-server /reports/{label}.json'])
        reports[label] = json.loads((output / f'{label}.json').read_text())
    failures = compare(reports['go'], reports['rust'])
    summary = {'go_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
               'rust_sha256': hashlib.sha256(rust.read_bytes()).hexdigest(),
               'go_platform': 'linux/amd64', 'rust_platform': f'linux/{native}',
               'rust_profile': args.profile,
               'steps_go': len(reports['go']['steps']), 'steps_rust': len(reports['rust']['steps']),
               'mcp_requests_go': reports['go']['requests'], 'mcp_requests_rust': reports['rust']['requests'],
               'normalization': ['tools/list enumeration order only', 'generated bash PID/output filenames only; output bytes compared separately'],
               'failures': failures}
    (output / 'comparison.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: v for k, v in summary.items() if k != 'failures'}, ensure_ascii=False))
    print(f'FAILURES {len(failures)}; report: {output / "comparison.json"}')
    for failure in failures[:10]:
        print(json.dumps(failure, ensure_ascii=False)[:2500])
    return bool(failures)


if __name__ == '__main__':
    sys.exit(main())
