#!/usr/bin/env bash
# 在 Linux ARM64 发布链中拒绝会破坏既定网络与 Git 实现边界的 Cargo 依赖。
set -euo pipefail

readonly workspace_root="${1:-.}"

if (($# > 1)); then
  echo "用法: $0 [rust-workspace-root]" >&2
  exit 1
fi
if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "Cargo.lock 禁止依赖检查只能在 Linux ARM64 上执行" >&2
  exit 1
fi
if [[ ! -d "${workspace_root}" ]]; then
  echo "Rust 工作区不存在: ${workspace_root}" >&2
  exit 1
fi

readonly root="$(realpath -e "${workspace_root}")"
readonly cargo_toml="${root}/Cargo.toml"
readonly cargo_lock="${root}/Cargo.lock"

# 发布门禁必须读取 Cargo 实际解析后的 lockfile，而不能只扫描 Cargo.toml。这样即使
# 禁止 crate 被重命名为别的依赖键，或作为间接依赖引入，仍会按真实 package name
# 被拒绝。符号链接同样被拒绝，避免检查和后续 `cargo --locked` 观察不同文件。
if [[ ! -f "${cargo_toml}" || -L "${cargo_toml}" ]]; then
  echo "Rust 工作区缺少普通 Cargo.toml: ${cargo_toml}" >&2
  exit 1
fi
if [[ ! -f "${cargo_lock}" || -L "${cargo_lock}" ]]; then
  echo "Rust 工作区缺少普通 Cargo.lock: ${cargo_lock}" >&2
  exit 1
fi

python3 - "${cargo_lock}" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

try:
    import tomllib
except ImportError:
    print("Python 缺少 tomllib，无法以 fail-closed 方式解析 Cargo.lock", file=sys.stderr)
    raise SystemExit(1)


lock_path = Path(sys.argv[1])
try:
    with lock_path.open("rb") as lock_file:
        document = tomllib.load(lock_file)
except (OSError, tomllib.TOMLDecodeError) as error:
    print(f"Cargo.lock 无法解析: {error}", file=sys.stderr)
    raise SystemExit(1)

lock_version = document.get("version")
packages = document.get("package")
if not isinstance(lock_version, int) or not isinstance(packages, list) or not packages:
    print("Cargo.lock 缺少有效的 version 或非空 package 列表", file=sys.stderr)
    raise SystemExit(1)


def forbidden_reason(name: str) -> str | None:
    """返回禁止原因；OpenSSL 命名空间中只明确允许证书路径探测 crate。"""

    if name == "tonic" or name.startswith("tonic-"):
        return "Tunnel 使用原始 WebSocket + ProtoJSON，不允许 tonic/tonic-*"
    if name in {"git2", "libgit2-sys"}:
        return "Git 必须继续调用外部 git，不允许 git2/libgit2-sys"
    if name == "openssl" or (
        name.startswith("openssl-") and name != "openssl-probe"
    ):
        return "TLS 必须使用 rustls，不允许 OpenSSL crate、源码包、宏或 FFI"
    if name in {"native-tls", "hyper-tls", "tokio-native-tls"}:
        return "TLS 必须使用 rustls，不允许 native-tls 适配层"
    return None


forbidden: dict[str, str] = {}
for index, package in enumerate(packages):
    if not isinstance(package, dict) or not isinstance(package.get("name"), str):
        print(f"Cargo.lock package[{index}] 缺少字符串 name", file=sys.stderr)
        raise SystemExit(1)
    name = package["name"]
    reason = forbidden_reason(name)
    if reason is not None:
        forbidden[name] = reason

if forbidden:
    print("Cargo.lock 包含发布策略禁止的依赖:", file=sys.stderr)
    for name in sorted(forbidden):
        print(f"  - {name}: {forbidden[name]}", file=sys.stderr)
    raise SystemExit(1)

print(f"Cargo.lock 禁止依赖检查通过: {len(packages)} 个 package")
PY
