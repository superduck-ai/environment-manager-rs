#!/usr/bin/env bash
# 在唯一支持的 Linux ARM64 环境中重建 Go source oracle，并严格比较 Go/Rust transcript。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly repo_root="$(cd "${script_dir}/../.." && pwd -P)"
requested_go_oracle_image="${GO_ORACLE_IMAGE:-}"
# shellcheck source=/dev/null
source "${repo_root}/compat/oracle-images.env"
if [[ -n "${requested_go_oracle_image}" ]]; then
  GO_ORACLE_IMAGE="${requested_go_oracle_image}"
fi

readonly go_source="${GO_SOURCE_DIR:-${GO_SOURCE_ROOT:-/mnt/mac/Users/arthur/GolandProjects/environment-manager}}"
readonly output_dir="${repo_root}/target/differential/logger-source"
readonly go_binary="${output_dir}/environment-manager-go"
readonly go_transcript="${output_dir}/go.jsonl"
readonly rust_transcript="${output_dir}/rust.jsonl"
readonly regenerated_oracle="${output_dir}/go-log-source-oracle.tsv"
readonly committed_oracle="${repo_root}/compat/go-log-source-oracle.tsv"
readonly canonical_overrides="${repo_root}/compat/go-log-source-canonical-overrides.tsv"
readonly probe_dir="${repo_root}/compat/go-probes/logger_source"
readonly catalog_probe_source="${repo_root}/compat/go-probes/logger_source_catalog_probe.go"
readonly catalog_probe_binary="${output_dir}/logger-source-catalog-probe"
readonly catalog_objdump="${output_dir}/logger-source-catalog-objdump.txt"
readonly catalog_source_pcs="${output_dir}/logger-source-catalog-pcs.txt"
readonly catalog_runtime_frames="${output_dir}/logger-source-catalog-frames.tsv"
readonly objdump_expression='github\.com/anthropics/anthropic/api-go/environment-manager/.*'

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "logger source 差分只允许在 Linux ARM64 上运行" >&2
  exit 2
fi
if [[ "${GO_ORACLE_PLATFORM}" != "linux/arm64" ]]; then
  echo "Go oracle 镜像平台不是 linux/arm64: ${GO_ORACLE_PLATFORM}" >&2
  exit 2
fi
if [[ ! "${GO_ORACLE_IMAGE}" =~ @sha256:[0-9a-f]{64}$ ]]; then
  echo "GO_ORACLE_IMAGE 必须固定到 sha256 digest" >&2
  exit 2
fi
if [[ ! -d "${go_source}/.git" ]]; then
  echo "找不到只读 Go oracle 工作树: ${go_source}" >&2
  exit 2
fi
if [[ ! -f "${probe_dir}/main.go" ]]; then
  echo "缺少 Go logger source probe: ${probe_dir}/main.go" >&2
  exit 2
fi
if [[ ! -f "${catalog_probe_source}" ]]; then
  echo "缺少 Go logger source catalog probe: ${catalog_probe_source}" >&2
  exit 2
fi
if [[ ! -f "${committed_oracle}" ]]; then
  echo "缺少已审核 Go logger source oracle: ${committed_oracle}" >&2
  exit 2
fi
if [[ ! -f "${canonical_overrides}" ]]; then
  echo "缺少 canonical Go CLI logger source 覆盖证据: ${canonical_overrides}" >&2
  exit 2
fi

podman_command=(podman)
if ! grep -q "^$(id -un):" /etc/subuid 2>/dev/null; then
  if ! sudo -n true 2>/dev/null; then
    echo "Podman 缺少 subuid/subgid，且当前用户不能无密码 sudo" >&2
    exit 2
  fi
  podman_command=(sudo podman)
fi

mkdir -p "${output_dir}"
rm -f \
  "${go_binary}" \
  "${go_transcript}" \
  "${rust_transcript}" \
  "${regenerated_oracle}" \
  "${catalog_probe_binary}" \
  "${catalog_objdump}" \
  "${catalog_source_pcs}" \
  "${catalog_runtime_frames}"

# 复用 canonical Go oracle 构建脚本的基线 hash、固定 digest 和 `-trimpath` 参数；
# 这样 source 证据与行为 oracle 使用同一个 ELF，而不是另造一套近似编译配置。
GO_ORACLE_OUTPUT="${go_binary}" \
GO_ORACLE_IMAGE="${GO_ORACLE_IMAGE}" \
GO_SOURCE_DIR="${go_source}" \
  "${script_dir}/build_go_oracle.sh" >/dev/null

if ! "${podman_command[@]}" image exists "${GO_ORACLE_IMAGE}"; then
  "${podman_command[@]}" pull "${GO_ORACLE_IMAGE}"
fi
actual_go_version="$(
  "${podman_command[@]}" run --rm --pull=never "${GO_ORACLE_IMAGE}" go version
)"
if [[ "${actual_go_version}" != "go version go1.25.7 linux/arm64" ]]; then
  echo "固定镜像工具链不符: ${actual_go_version}" >&2
  exit 2
fi

if [[ "${podman_command[0]}" == "sudo" ]]; then
  cache_namespace="sudo-v2"
else
  cache_namespace="rootless-v2"
fi
readonly cache_dir="${repo_root}/target/differential/go-cache-${cache_namespace}"
mkdir -p "${cache_dir}/build" "${cache_dir}/mod"

# catalog probe 只复制到容器临时目录，并以两个显式 root package 文件构建；Go
# oracle 工作树始终 :ro。probe 只增加一个受环境变量保护的 init，不改业务包源码。
"${podman_command[@]}" run --rm --pull=never --arch arm64 \
  --volume "${go_source}:/src:ro" \
  --volume "${catalog_probe_source}:/probe/logger_source_catalog_probe.go:ro" \
  --volume "${output_dir}:/out" \
  --volume "${cache_dir}/build:/cache/build" \
  --volume "${cache_dir}/mod:/cache/mod" \
  --env CGO_ENABLED=1 \
  --env GOOS=linux \
  --env GOARCH=arm64 \
  --env GOCACHE=/cache/build \
  --env GOMODCACHE=/cache/mod \
  "${GO_ORACLE_IMAGE}" \
  sh -euc '
    cp -a /src /tmp/work
    cp /probe/logger_source_catalog_probe.go /tmp/work/logger_source_catalog_probe.go
    cd /tmp/work
    go build -mod=readonly -trimpath -buildvcs=false \
      -o /out/logger-source-catalog-probe \
      ./main.go ./logger_source_catalog_probe.go
    go tool objdump \
      -s '"'"'github\.com/anthropics/anthropic/api-go/environment-manager/.*'"'"' \
      /out/logger-source-catalog-probe \
      >/out/logger-source-catalog-objdump.txt
  '

if [[ "${podman_command[0]}" == "sudo" ]]; then
  sudo chown -R "$(id -u):$(id -g)" "${output_dir}"
fi

# 两阶段解析避免猜测闭包编号：先提取调用表达式内的 project PC，再让同一 ELF
# 自己通过 runtime.CallersFrames 解析 function/file/line，最后才生成 oracle TSV。
PYTHONDONTWRITEBYTECODE=1 python3 \
  "${script_dir}/generate_go_log_source_oracle.py" \
  --go-root "${go_source}" \
  --binary "${catalog_probe_binary}" \
  --objdump "${catalog_objdump}" \
  --verified-go-version "${actual_go_version}" \
  --source-pcs-output "${catalog_source_pcs}" \
  --canonical-overrides "${canonical_overrides}" \
  --output "${regenerated_oracle}"

LOGGER_SOURCE_PC_PROBE=1 "${catalog_probe_binary}" \
  <"${catalog_source_pcs}" \
  >"${catalog_runtime_frames}"

# 宿主 Python 只解析容器产生的只读证据；禁写 bytecode，避免源码树出现
# `__pycache__`，且精确 Go version 文本由上面的固定镜像调用传入。
PYTHONDONTWRITEBYTECODE=1 python3 \
  "${script_dir}/generate_go_log_source_oracle.py" \
  --go-root "${go_source}" \
  --binary "${catalog_probe_binary}" \
  --objdump "${catalog_objdump}" \
  --verified-go-version "${actual_go_version}" \
  --runtime-frames "${catalog_runtime_frames}" \
  --canonical-overrides "${canonical_overrides}" \
  --output "${regenerated_oracle}"

if ! cmp -s "${committed_oracle}" "${regenerated_oracle}"; then
  echo "Go logger source oracle 已漂移；固定 ELF 证据与仓库目录不同" >&2
  diff -u "${committed_oracle}" "${regenerated_oracle}" >&2 || true
  exit 1
fi

# internal package 导入规则要求 probe 位于同一 module 树下。容器只把 Go oracle
# 以 :ro 挂载，再复制到临时目录编译；任何 probe 文件和 build cache 都不会回写。
"${podman_command[@]}" run --rm --pull=never --arch arm64 \
  --volume "${go_source}:/src:ro" \
  --volume "${probe_dir}:/probe:ro" \
  --volume "${output_dir}:/out" \
  --volume "${cache_dir}/build:/cache/build" \
  --volume "${cache_dir}/mod:/cache/mod" \
  --env CGO_ENABLED=1 \
  --env GOOS=linux \
  --env GOARCH=arm64 \
  --env GOCACHE=/cache/build \
  --env GOMODCACHE=/cache/mod \
  "${GO_ORACLE_IMAGE}" \
  sh -euc '
    mkdir -p /tmp/work/compat/logger-source-probe
    cp -a /src/. /tmp/work/
    cp /probe/main.go /tmp/work/compat/logger-source-probe/main.go
    cd /tmp/work
    go build -mod=readonly -trimpath -buildvcs=false \
      -o /tmp/logger-source-probe ./compat/logger-source-probe
    /tmp/logger-source-probe >/out/go.jsonl
  '

if [[ "${podman_command[0]}" == "sudo" ]]; then
  sudo chown -R "$(id -u):$(id -g)" "${output_dir}"
fi

LOGGER_SOURCE_TRANSCRIPT_OUTPUT="${rust_transcript}" \
  cargo +1.97.0 test --locked \
    --test logger_source_differential_probe \
    -- --ignored --exact write_logger_source_differential_transcript

# `cmp` 是刻意选择：transcript 根本不含 time，source/function/file/line 也没有任何
# normalizer。结构化诊断只在失败后打印，绝不能把差异改写成相等。
if ! cmp -s "${go_transcript}" "${rust_transcript}"; then
  echo "Go/Rust logger source transcript 不一致（source 不允许归一化）" >&2
  PYTHONDONTWRITEBYTECODE=1 python3 - \
    "${go_transcript}" "${rust_transcript}" <<'PY' >&2
import json
import sys
from pathlib import Path

for label, name in (("Go", sys.argv[1]), ("Rust", sys.argv[2])):
    values = [json.loads(line) for line in Path(name).read_text().splitlines()]
    print(f"{label}: {json.dumps(values, ensure_ascii=False, indent=2)}")
PY
  exit 1
fi

echo "Go/Rust logger source 严格差分通过（无 source 归一化）"
