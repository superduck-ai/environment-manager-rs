#!/usr/bin/env bash
# 同步源码并在 Linux ARM64 上执行全部已落地的本地质量门禁。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=container-images.env
source "${script_dir}/container-images.env"
readonly machine="${ORB_MACHINE:-ubuntu-2404}"
readonly user="${ORB_USER:-arthur}"
readonly target="/home/${user}/environment-manager"
readonly go_root="${VM_GO_SOURCE_ROOT:-/mnt/mac/Users/${user}/GolandProjects/environment-manager}"
readonly differential_runner="scripts/compat/run_differential.sh"
readonly go_oracle_image="${GO_ORACLE_IMAGE:-${DEFAULT_GO_ORACLE_IMAGE}}"

# 差分实现是发布门禁本身，不能由调用方替换成返回 0 的自定义脚本。旧变量即使
# 指向 canonical 路径也会被拒绝，以便 CI 配置中的过期绕过方式立刻暴露。
if [[ -n "${DIFFERENTIAL_RUNNER:-}" ]]; then
  echo "禁止设置 DIFFERENTIAL_RUNNER；唯一 runner 为 ${differential_runner}" >&2
  exit 1
fi
if ! is_audited_container_image go "${go_oracle_image}"; then
  echo "GO_ORACLE_IMAGE 不在审核清单 ${CONTAINER_IMAGE_ALLOWLIST_VERSION} 中" >&2
  echo "拒绝镜像: ${go_oracle_image}" >&2
  exit 1
fi

"${script_dir}/sync.sh"

orb -m "${machine}" -u "${user}" -w "${target}" bash -s -- \
  "${go_root}" "${differential_runner}" "${go_oracle_image}" <<'REMOTE'
set -uo pipefail
go_root="$1"
differential_runner="$2"
go_oracle_image="$3"
export PATH="${CARGO_HOME:-${HOME}/.cargo}/bin:${PATH}"
export GO_SOURCE_ROOT="${go_root}"
export GO_SOURCE_DIR="${go_root}"
# Go 的 ProxyFromEnvironment 无条件绕过 localhost 与 loopback IP；Reqwest 只读取
# NO_PROXY。在 OrbStack/Surge 注入 HTTP_PROXY 但未注入 NO_PROXY 时，loopback fixture
# 会错误地绕到 macOS 代理。权威测试环境显式补齐这一条 Go 传输层不变量，同时保留
# 调用方已有的 NO_PROXY 项，避免 API、Git proxy 与协议探针出现伪 503。
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
run_gate "Go 工作树不可变基线" \
  scripts/linux-arm64/verify-go-baseline.sh "${go_root}"
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
run_gate "Go/Rust 符号映射完整且新鲜" \
  python3 scripts/compat/generate_symbol_map.py \
    --go-root "${go_root}" \
    --rust-root . \
    --check \
    --expected-go-tests 295 \
    --require-complete
run_gate "Go logger source 映射 100% oracle 且新鲜" \
  env PYTHONDONTWRITEBYTECODE=1 \
  python3 scripts/compat/generate_log_source_map.py \
    --go-root "${go_root}" \
    --rust-root . \
    --check \
    --require-complete
run_gate "cargo clippy --all-targets" \
  cargo +1.97.0 clippy --all-targets --locked -- -D warnings
run_gate "cargo test --locked" cargo +1.97.0 test --locked
run_gate "Go/Rust logger source 严格差分" \
  env GO_ORACLE_IMAGE="${go_oracle_image}" \
  GO_SOURCE_ROOT="${go_root}" \
  GO_SOURCE_DIR="${go_root}" \
  bash scripts/compat/run_logger_source_differential.sh

if [[ ! -f "${differential_runner}" ]]; then
  printf '<== FAIL: canonical 差分 runner 不存在: %s\n' \
    "${differential_runner}" >&2
  failures+=("Go/Rust 差分测试")
else
  run_gate "Go/Rust canonical 差分测试" \
    env GO_ORACLE_IMAGE="${go_oracle_image}" \
    GO_SOURCE_ROOT="${go_root}" \
    GO_SOURCE_DIR="${go_root}" \
    bash "${differential_runner}"
fi

if ((${#failures[@]} > 0)); then
  printf '\nLinux ARM64 门禁失败（%d 项）:\n' "${#failures[@]}" >&2
  printf '  - %s\n' "${failures[@]}" >&2
  exit 1
fi

echo
echo "全部 Linux ARM64 本地门禁（含 canonical 差分）通过"
REMOTE
