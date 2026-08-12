#!/usr/bin/env bash
# 使用固定 digest 的 Go 1.25.7 ARM64 容器构建只读行为 oracle。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=container-images.env
source "${script_dir}/container-images.env"
readonly go_root="${1:-${GO_SOURCE_ROOT:-}}"
readonly output="${2:-}"
readonly image="${GO_ORACLE_IMAGE:-${DEFAULT_GO_ORACLE_IMAGE}}"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "Go oracle/rollback 只能在 Linux ARM64 上构建" >&2
  exit 1
fi
if [[ -z "${go_root}" || ! -d "${go_root}" ]]; then
  echo "Go oracle 源目录不存在: ${go_root:-<empty>}" >&2
  exit 1
fi
if [[ -z "${output}" ]]; then
  echo "用法: GO_ORACLE_IMAGE=<image@sha256:...> $0 <go-root> <output>" >&2
  exit 1
fi
if ! is_audited_container_image go "${image}"; then
  echo "GO_ORACLE_IMAGE 不在审核清单 ${CONTAINER_IMAGE_ALLOWLIST_VERSION} 中" >&2
  echo "拒绝镜像: ${image}" >&2
  exit 1
fi
if [[ -L "${output}" ]]; then
  echo "Go oracle/rollback 输出不能是符号链接: ${output}" >&2
  exit 1
fi

mkdir -p "$(dirname "${output}")" "${HOME}/.cache/environment-manager"
staging="$(mktemp -d "${HOME}/.cache/environment-manager/go-oracle.XXXXXX")"
trap 'rm -rf "${staging}"' EXIT
version="$(git -C "${go_root}" describe --tags --always --dirty 2>/dev/null || printf dev)"

podman run --rm --pull=always --arch arm64 \
  --volume "${go_root}:/workspace:ro" \
  --volume "${staging}:/out" \
  --env "ORACLE_VERSION=${version}" \
  --workdir /workspace \
  "${image}" \
  sh -euc '
    test "$(uname -m)" = "aarch64"
    test "$(go env GOVERSION)" = "go1.25.7"
    git config --global --add safe.directory /workspace
    export CGO_ENABLED=0 GOOS=linux GOARCH=arm64
    # 正式 rollback 与行为 oracle 使用同一组核心构建参数。差分 runner 还会
    # 为协议 probe 使用 go test overlay；该 overlay 只影响 probe，不属于 rollback。
    go build -trimpath -buildvcs=false \
      -ldflags "-s -w -X main.Version=${ORACLE_VERSION}" \
      -o /out/environment-manager-go-oracle .
  '

oracle="${staging}/environment-manager-go-oracle"
# helper 的输出就是正式 release 使用的 rollback，因此不仅检查架构，还必须证明
# CGO=0 产物无 INTERP/NEEDED，并在权威宿主实际执行一次无副作用的 help 路径。
"${script_dir}/verify-go-rollback-artifact.sh" "${oracle}"
install -m 0755 "${oracle}" "${output}"
"${script_dir}/verify-go-rollback-artifact.sh" "${output}"
(
  cd "$(dirname "${output}")"
  sha256sum "$(basename "${output}")" >"$(basename "${output}").sha256"
)
echo "Go oracle 已生成: ${output}"
