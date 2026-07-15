#!/usr/bin/env bash
# 将 macOS 工作区安全同步到 OrbStack 的 Linux 本地文件系统。
set -euo pipefail

readonly machine="${ORB_MACHINE:-ubuntu-2404}"
readonly user="${ORB_USER:-arthur}"
readonly target="/home/${user}/environment-manager"
readonly host_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
readonly shared_root="/mnt/mac${host_root}"
readonly marker_name=".environment-manager-sync-root"
readonly marker_schema="environment-manager-sync-root-v2"

if ! command -v orb >/dev/null 2>&1; then
  echo "缺少 OrbStack orb 命令；同步脚本必须从 macOS 主机运行" >&2
  exit 1
fi

orb -m "${machine}" -u "${user}" bash "${shared_root}/scripts/linux-arm64/bootstrap.sh"

orb -m "${machine}" -u "${user}" bash -s -- \
  "${shared_root}" "${target}" "${marker_name}" "${marker_schema}" <<'REMOTE'
set -euo pipefail
source_root="$1"
target="$2"
marker_name="$3"
marker_schema="$4"
expected_target="/home/$(id -un)/environment-manager"
marker_path="${target}/${marker_name}"
expected_marker="${marker_schema}"$'\n'"target=${expected_target}"$'\n'"owner=$(id -un)"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "同步目标不是 Linux ARM64" >&2
  exit 1
fi
if [[ "$(realpath -m "${target}")" != "${expected_target}" ]]; then
  echo "拒绝同步到非预期目录: ${target}" >&2
  exit 1
fi
if [[ -L "${target}" ]]; then
  echo "目标目录不能是符号链接: ${target}" >&2
  exit 1
fi
if [[ ! -f "${source_root}/Cargo.toml" \
  || ! -f "${source_root}/scripts/linux-arm64/sync.sh" \
  || ! -f "${source_root}/scripts/linux-arm64/bootstrap.sh" ]]; then
  echo "共享源目录不是完整的 environment-manager Rust 工作区: ${source_root}" >&2
  exit 1
fi
if ! grep -Eq '^name = "environment-manager"$' "${source_root}/Cargo.toml"; then
  echo "共享源目录的 Cargo 包名不匹配: ${source_root}" >&2
  exit 1
fi
if [[ "$(realpath -m "${source_root}")" == "$(realpath -m "${target}")" ]]; then
  echo "源目录和目标目录不能相同" >&2
  exit 1
fi

mkdir -p "${target}"
if [[ -e "${marker_path}" && (! -f "${marker_path}" || -L "${marker_path}") ]]; then
  echo "同步 marker 必须是普通文件且不能是符号链接: ${marker_path}" >&2
  exit 1
fi

write_marker() {
  local temporary_marker="${target}/.${marker_name}.tmp.$$"
  trap 'rm -f "${temporary_marker}"' RETURN
  printf '%s\n' "${expected_marker}" >"${temporary_marker}"
  chmod 0600 "${temporary_marker}"
  mv -f "${temporary_marker}" "${marker_path}"
  trap - RETURN
}

rsync_source() {
  rsync -a "$@" \
    --exclude '/.git/' \
    --exclude '/.idea/' \
    --exclude '/target/' \
    --exclude '/dist/' \
    --exclude '/.DS_Store' \
    --exclude '**/__pycache__/' \
    --exclude '*.py[cod]' \
    --exclude "/${marker_name}" \
    "${source_root}/" "${target}/"
}

if [[ ! -e "${marker_path}" ]]; then
  if [[ -n "$(find "${target}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "目标目录非空且缺少同步 marker，拒绝 --delete: ${target}" >&2
    exit 1
  fi
  write_marker
elif [[ ! -s "${marker_path}" ]]; then
  # v1 marker 是零字节文件。先执行一次不含 --delete 的同步，确认目标确实可由当前
  # Rust 工作区覆盖，再升级 marker；只有带内容的 v2 marker 才授权后续删除陈旧文件。
  rsync_source
  write_marker
elif [[ "$(cat "${marker_path}")" != "${expected_marker}" ]]; then
  echo "同步 marker 内容不匹配，拒绝 --delete: ${marker_path}" >&2
  exit 1
fi

rsync_source --delete --delete-delay
REMOTE
