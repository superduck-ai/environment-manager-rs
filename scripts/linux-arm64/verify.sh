#!/usr/bin/env bash
# 同步源码并在 Linux ARM64 上执行全部已落地的本地质量门禁。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=container-images.env
source "${script_dir}/container-images.env"
readonly machine="${ORB_MACHINE:-ubuntu-2404}"
readonly user="${ORB_USER:-arthur}"
readonly target="/home/${user}/environment-manager"

"${script_dir}/sync.sh"

orb -m "${machine}" -u "${user}" -w "${target}" bash -s <<'REMOTE'
set -uo pipefail
export PATH="${CARGO_HOME:-${HOME}/.cargo}/bin:${PATH}"
# loopback fixture 不应经过宿主代理。保留调用方已有的 NO_PROXY 项。
existing_no_proxy="${NO_PROXY:-${no_proxy:-}}"
export NO_PROXY="localhost,127.0.0.0/8,::1${existing_no_proxy:+,${existing_no_proxy}}"
export no_proxy="${NO_PROXY}"
# 权威 VM 同时编译 lib、bin、集成测试和 Clippy；增量目录不影响
# 验收结果，却会对每种 profile/flags 保留一份大量 object。权威门禁
# 固定禁用增量编译，避免有限的 OrbStack 根盘影响稳定性。
export CARGO_INCREMENTAL=0
failures=()

run_gate() {
  local label="$1"
  shift
  printf '\n==> %s\n' "${label}"
  if "$@"; then
    printf '<== PASS: %s\n' "${label}"
  else
    local status=$?
    printf '<== FAIL(%d): %s\n' "${status}" "${label}" >&2
    failures+=("${label}")
  fi
}

check_platform() {
  # Ubuntu 24.04 是唯一权威测试宿主；只检查 kernel/CPU 会让其他发行版的 libc、
  # shell 或系统工具差异混入结果。rustc host 也必须是原生 GNU ARM64。
  [[ -r /etc/os-release ]] || return 1
  source /etc/os-release
  [[ "${ID:-}" == "ubuntu" ]] \
    && [[ "${VERSION_ID:-}" == "24.04" ]] \
    && [[ "$(uname -s)" == "Linux" ]] \
    && [[ "$(uname -m)" == "aarch64" ]] \
    && [[ "$(dpkg --print-architecture)" == "arm64" ]] \
    && [[ "$(rustc +1.97.0 --version)" =~ ^rustc\ 1\.97\.0\  ]] \
    && [[ "$(rustc +1.97.0 -vV | sed -n 's/^host: //p')" \
      == "aarch64-unknown-linux-gnu" ]] \
    && rustup target list --toolchain 1.97.0 --installed \
      | grep -qx 'aarch64-unknown-linux-gnu'
}

run_gate "Ubuntu 24.04 ARM64、Rust 1.97.0 与 GNU host triple" check_platform
run_gate "Cargo.lock 禁止 tonic/git2/OpenSSL 依赖" \
  scripts/linux-arm64/verify-cargo-dependencies.sh .
run_gate "cargo fmt --all --check" cargo +1.97.0 fmt --all --check
run_gate "中文注释覆盖审计" \
  cargo +1.97.0 test --locked --test comment_audit
# `syn` 审计可以确认文档存在且包含中文，但只有 rustdoc 能解析跨模块链接并发现
# 失效的 item path。该门禁仍在唯一权威 Linux ARM64 环境运行，并且不为依赖生成
# 文档，避免第三方 crate 的 warning 影响本项目验收。
run_gate "rustdoc 链接与文档警告审计" \
  env RUSTDOCFLAGS="-D warnings" \
  cargo +1.97.0 doc --locked --no-deps --document-private-items
run_gate "cargo clippy --all-targets" \
  cargo +1.97.0 clippy --all-targets --locked -- -D warnings
run_gate "cargo test --locked" cargo +1.97.0 test --locked

if ((${#failures[@]} > 0)); then
  printf '\nLinux ARM64 门禁失败（%d 项）:\n' "${#failures[@]}" >&2
  printf '  - %s\n' "${failures[@]}" >&2
  exit 1
fi

echo
echo "全部 Linux ARM64 本地门禁通过"
REMOTE
