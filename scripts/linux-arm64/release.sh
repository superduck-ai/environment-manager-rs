#!/usr/bin/env bash
# 复核并提升已经完成 staging E2E 的 Linux ARM64 候选；本脚本绝不重建 Rust。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=container-images.env
source "${script_dir}/container-images.env"
readonly machine="${ORB_MACHINE:-ubuntu-2404}"
readonly user="${ORB_USER:-arthur}"
readonly target="/home/${user}/environment-manager"
readonly rust_image="${RUST_RELEASE_IMAGE:-${DEFAULT_RUST_RELEASE_IMAGE}}"
readonly approved_sha256="${STAGING_E2E_ARTIFACT_SHA256:-}"

if (($# != 0)); then
  echo "release.sh 不接受参数" >&2
  exit 1
fi
if [[ "${STAGING_E2E_APPROVED:-0}" != "1" ]]; then
  echo "staging E2E 尚未显式放行；完成验证后设置 STAGING_E2E_APPROVED=1" >&2
  exit 1
fi
if [[ ! "${approved_sha256}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "必须通过 STAGING_E2E_ARTIFACT_SHA256 提供已验证候选的精确小写 SHA-256" >&2
  exit 1
fi
if ! is_audited_container_image rust "${rust_image}"; then
  echo "RUST_RELEASE_IMAGE 不在审核清单 ${CONTAINER_IMAGE_ALLOWLIST_VERSION} 中" >&2
  exit 1
fi
# 同步最新源码并重新执行全部本地门禁；随后复算源码清单，确保候选与
# 此次通过门禁的源码仍完全相同。
"${script_dir}/verify.sh"

orb -m "${machine}" -u "${user}" -w "${target}" bash -s -- \
  "${target}" "${approved_sha256}" "${rust_image}" \
  "${CONTAINER_IMAGE_ALLOWLIST_VERSION}" <<'REMOTE'
set -euo pipefail
target="$1"
approved_sha256="$2"
expected_rust_image="$3"
image_allowlist_version="$4"
artifact_name="environment-manager_linux_arm64"
candidate_directory="dist/preflight"
candidate="${candidate_directory}/${artifact_name}"
manifest="${candidate}.manifest"
source_manifest="${candidate}.source-manifest"
cache_root="${XDG_CACHE_HOME:-${HOME}/.cache}/environment-manager"
mkdir -p "${cache_root}"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "正式提升只能在 Linux ARM64 上执行" >&2
  exit 1
fi
source /etc/os-release
if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != "24.04" ]]; then
  echo "正式提升宿主必须是 Ubuntu 24.04 ARM64" >&2
  exit 1
fi
if [[ "$(realpath -m "${target}")" != "/home/$(id -un)/environment-manager" ]]; then
  echo "正式提升工作区路径不符合约束: ${target}" >&2
  exit 1
fi
if [[ -L dist || (-e dist && ! -d dist) ]]; then
  echo "dist 必须是工作区内的真实目录，不能是符号链接或其他文件" >&2
  exit 1
fi
mkdir -p dist
promotion="$(mktemp -d "${target}/dist/.promotion.XXXXXX")"
recomputed_source="$(mktemp "${cache_root}/release-source.XXXXXX")"
trap 'rm -rf "${promotion}"; rm -f "${recomputed_source}"' EXIT

for required_file in \
  "${candidate}" "${candidate}.sha256" "${manifest}" "${source_manifest}"; do
  if [[ ! -f "${required_file}" || -L "${required_file}" ]]; then
    echo "候选证据缺失或为符号链接: ${required_file}" >&2
    exit 1
  fi
done

# manifest 不使用 `source` 执行，避免把候选元数据当成 shell 代码。每个键必须
# 恰好出现一次；重复键同样视为篡改，而不是任意取第一项或最后一项。
manifest_value() {
  local key="$1"
  awk -F= -v key="${key}" '
    $1 == key { count += 1; value = substr($0, length(key) + 2) }
    END { if (count != 1) exit 1; print value }
  ' "${manifest}"
}

[[ "$(manifest_value schema)" == "environment-manager-release-candidate-v1" ]]
[[ "$(manifest_value artifact_name)" == "${artifact_name}" ]]
[[ "$(manifest_value source_manifest)" == "${artifact_name}.source-manifest" ]]
[[ "$(manifest_value target)" == "aarch64-unknown-linux-gnu" ]]
[[ "$(manifest_value rust_toolchain)" == "1.97.0" ]]
[[ "$(manifest_value release_image_allowlist)" == "${image_allowlist_version}" ]]
[[ "$(manifest_value release_image)" == "${expected_rust_image}" ]]

actual_artifact_sha256="$(sha256sum "${candidate}" | awk '{print $1}')"
manifest_artifact_sha256="$(manifest_value artifact_sha256)"
if [[ "${actual_artifact_sha256}" != "${manifest_artifact_sha256}" \
  || "${actual_artifact_sha256}" != "${approved_sha256}" ]]; then
  echo "候选 SHA 与 manifest 或 staging E2E 审批值不一致" >&2
  echo "实际=${actual_artifact_sha256}" >&2
  echo "manifest=${manifest_artifact_sha256}" >&2
  echo "staging=${approved_sha256}" >&2
  exit 1
fi
if [[ "$(cat "${candidate}.sha256")" \
  != "${actual_artifact_sha256}  ${artifact_name}" ]]; then
  echo "候选 .sha256 文件与实际字节不一致" >&2
  exit 1
fi

actual_source_sha256="$(sha256sum "${source_manifest}" | awk '{print $1}')"
if [[ "${actual_source_sha256}" != "$(manifest_value source_sha256)" ]]; then
  echo "候选源码清单自身的 SHA 与 manifest 不一致" >&2
  exit 1
fi
scripts/linux-arm64/source-manifest.sh "${target}" "${recomputed_source}" >/dev/null
if ! cmp -s "${source_manifest}" "${recomputed_source}"; then
  echo "当前源码相对 staging E2E 候选已经漂移；必须重建候选并重新验证" >&2
  exit 1
fi
scripts/linux-arm64/verify-release-artifact.sh "${candidate}"

# 将 preflight 候选原字节复制到 promotion 暂存区，并用 cmp 和 SHA 双重确认。
# 这里只复制，不调用 cargo；因此正式产物就是 staging E2E 验证过的同一二进制。
cp --reflink=auto --preserve=mode "${candidate}" "${promotion}/${artifact_name}"
cp "${candidate}.sha256" "${candidate}.manifest" \
  "${candidate}.source-manifest" "${promotion}/"
if ! cmp -s "${candidate}" "${promotion}/${artifact_name}" \
  || [[ "$(sha256sum "${promotion}/${artifact_name}" | awk '{print $1}')" \
    != "${approved_sha256}" ]]; then
  echo "候选复制后字节发生变化" >&2
  exit 1
fi

# promotion 全部准备成功后才替换正式路径。可执行文件最后移动，作为本次
# 提升完成的提交点；preflight 证据保留，便于事后核对 staging SHA。
rm -f -- \
  "dist/${artifact_name}.sha256" \
  "dist/${artifact_name}.manifest" \
  "dist/${artifact_name}.source-manifest"
mv "${promotion}/${artifact_name}.sha256" \
  "${promotion}/${artifact_name}.manifest" \
  "${promotion}/${artifact_name}.source-manifest" dist/
# promotion 与 dist 位于同一文件系统；直接 rename 覆盖旧产物，让读取方
# 只能观察到完整旧文件或完整新文件，不留“已删除但尚未写入”窗口。
mv -f "${promotion}/${artifact_name}" "dist/${artifact_name}"

echo "正式 Rust 产物: ${target}/dist/${artifact_name}"
echo "正式 Rust SHA-256: ${approved_sha256}"
REMOTE
