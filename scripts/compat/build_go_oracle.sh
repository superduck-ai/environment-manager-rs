#!/usr/bin/env bash
# 在 Linux ARM64 上从只读 Go 工作树构建固定行为 oracle。
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "${script_dir}/../.." && pwd -P)"
# verify.sh 会按发布环境显式传入固定 digest。先保存调用者值，避免 source 默认配置
# 无条件覆盖它；空值仍回退仓库审核过的默认 digest。
requested_go_oracle_image="${GO_ORACLE_IMAGE:-}"
# shellcheck source=/dev/null
source "${repo_root}/compat/oracle-images.env"
if [[ -n "${requested_go_oracle_image}" ]]; then
  GO_ORACLE_IMAGE="${requested_go_oracle_image}"
fi

expected_head="65e3c65ce3f0f3b69819775d35ac448d9cd1b4fb"
expected_diff_sha256="d0606591baf87fc0c16ea1d0d8dc7b7934fcd67dcc1d5a619c6e390c799de775"
expected_executor_sha256="9b8541e81f706e243609cbce69371c8936753f00d089b3ad5cdd4d746d97f7f7"
expected_executor_test_sha256="4ce510a82b4b1c73d31526a30368d3d39a2a55e3e55234237190824fee237e1b"
expected_makefile_sha256="cc5f4b15b15901813f7b0ede9b72b0d91cc8b78f8e06989f0e8a143a5b057311"
expected_reference_elf_sha256="1c6a4f260f77ad12fa19186ad1fbe0b85f5a2f904bafbd5b11039f49be5f58c0"

# GO_SOURCE_DIR 是兼容旧调用者的精确 override；Linux verify.sh 的规范变量是
# GO_SOURCE_ROOT。两者均缺失时才使用 OrbStack 共享目录的历史默认值。
go_source="${GO_SOURCE_DIR:-${GO_SOURCE_ROOT:-/mnt/mac/Users/arthur/GolandProjects/environment-manager}}"
output="${GO_ORACLE_OUTPUT:-${repo_root}/target/differential/environment-manager-go}"
binary_version="${GO_ORACLE_VERSION:-0.1.0}"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "Go oracle 只允许在 Linux ARM64 上构建" >&2
  exit 2
fi
if [[ "${GO_ORACLE_PLATFORM}" != "linux/arm64" ]]; then
  echo "oracle 镜像配置的平台不是 linux/arm64: ${GO_ORACLE_PLATFORM}" >&2
  exit 2
fi
if [[ ! "${GO_ORACLE_IMAGE}" =~ @sha256:[0-9a-f]{64}$ ]]; then
  echo "GO_ORACLE_IMAGE 必须固定到 sha256 digest" >&2
  exit 2
fi
if [[ ! -d "${go_source}/.git" ]]; then
  echo "找不到 Go 工作树: ${go_source}" >&2
  exit 2
fi

# 构建前逐项核对不可变基线。Go 工作树只被 git/sha256sum 读取，随后也以 :ro
# 挂载进容器；模块缓存和编译缓存全部放在容器临时目录，绝不写回 oracle 源码。
actual_head="$(git -C "${go_source}" rev-parse HEAD)"
# locale、binary patch 和 HEAD 基准必须固定；否则同一工作树可能因 Git 配置或二进制
# 差异展示方式产生另一份输入，却仍被错误地视作已审核基线。
actual_diff_sha256="$(LC_ALL=C git -C "${go_source}" diff --binary HEAD | sha256sum | awk '{print $1}')"
if [[ "${actual_head}" != "${expected_head}" ]]; then
  echo "Go HEAD 漂移: got ${actual_head}, want ${expected_head}" >&2
  exit 2
fi
if [[ "${actual_diff_sha256}" != "${expected_diff_sha256}" ]]; then
  echo "Go 工作树 diff 漂移: got ${actual_diff_sha256}, want ${expected_diff_sha256}" >&2
  exit 2
fi

verify_sha256() {
  local relative_path="$1"
  local expected="$2"
  local actual
  actual="$(sha256sum "${go_source}/${relative_path}" | awk '{print $1}')"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "Go 基线文件漂移 ${relative_path}: got ${actual}, want ${expected}" >&2
    exit 2
  fi
}
verify_sha256 "internal/claude/claude_code_executor.go" "${expected_executor_sha256}"
verify_sha256 "internal/claude/claude_code_executor_test.go" "${expected_executor_test_sha256}"
verify_sha256 "Makefile" "${expected_makefile_sha256}"
verify_sha256 "environment-manager-06-25" "${expected_reference_elf_sha256}"

# OrbStack 验证机没有为普通用户配置 subuid/subgid，rootless Podman 无法解包 Go
# 镜像中的系统账号文件。因此优先使用 rootless，只有缺少映射时才明确回退 sudo。
podman_command=(podman)
if ! grep -q "^$(id -un):" /etc/subuid 2>/dev/null; then
  if ! sudo -n true 2>/dev/null; then
    echo "Podman 缺少 subuid/subgid，且当前用户不能无密码 sudo" >&2
    exit 2
  fi
  podman_command=(sudo podman)
fi

if ! "${podman_command[@]}" image exists "${GO_ORACLE_IMAGE}"; then
  "${podman_command[@]}" pull "${GO_ORACLE_IMAGE}"
fi
actual_go_version="$("${podman_command[@]}" run --rm --pull=never "${GO_ORACLE_IMAGE}" go version)"
if [[ "${actual_go_version}" != "go version go1.25.7 linux/arm64" ]]; then
  echo "固定镜像工具链不符: ${actual_go_version}" >&2
  exit 2
fi

output_dir="$(dirname "${output}")"
output_name="$(basename "${output}")"
# rootless 与 sudo Podman 会以不同的宿主 UID 写 bind-mounted cache。两种模式若
# 共用目录，前一次 rootful 构建留下的 0755 哈希子目录会让后续 rootless Go test
# 得到 permission denied。按执行模式和 schema 隔离 cache，既保留下载加速，也
# 不需要递归 chown 另一安全域创建的内容。
if [[ "${podman_command[0]}" == "sudo" ]]; then
  cache_namespace="sudo-v2"
else
  cache_namespace="rootless-v2"
fi
cache_dir="${repo_root}/target/differential/go-cache-${cache_namespace}"
mkdir -p "${output_dir}" "${cache_dir}/build" "${cache_dir}/mod"
# 根包只有审核过的 tracked `main.go`。显式构建该入口，避免 Go 工作树中与迁移
# 基线无关的临时 root-level probe 被 `go build .` 吞入；所有 imported package 仍从
# 当前只读工作树解析，因此 CCR 等已固定 hash 的修改不会被清洁 checkout 覆盖。
"${podman_command[@]}" run --rm --pull=never --arch arm64 \
  --volume "${go_source}:/src:ro" \
  --volume "${output_dir}:/out" \
  --volume "${cache_dir}/build:/cache/build" \
  --volume "${cache_dir}/mod:/cache/mod" \
  --workdir /src \
  --env CGO_ENABLED=1 \
  --env GOOS=linux \
  --env GOARCH=arm64 \
  --env GOCACHE=/cache/build \
  --env GOMODCACHE=/cache/mod \
  "${GO_ORACLE_IMAGE}" \
  go build -mod=readonly -trimpath -buildvcs=false \
    -ldflags "-X main.Version=${binary_version}" \
    -o "/out/${output_name}" ./main.go

# sudo Podman 生成的文件属于 root；只调整产物所有者，不触碰只读 Go 工作树。
if [[ "${podman_command[0]}" == "sudo" ]]; then
  sudo chown "$(id -u):$(id -g)" "${output}"
fi
if ! file "${output}" | grep -Eq 'ELF 64-bit.*(ARM aarch64|AArch64)'; then
  echo "Go oracle 不是 AArch64 ELF: $(file "${output}")" >&2
  exit 2
fi

printf '%s\n' "${output}"
