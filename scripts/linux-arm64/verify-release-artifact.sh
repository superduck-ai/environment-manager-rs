#!/usr/bin/env bash
# 校验发布文件名、AArch64 GNU ELF 属性、动态依赖和最高 GLIBC 符号版本。
set -euo pipefail

readonly artifact="${1:-}"
readonly expected_name="environment-manager_linux_arm64"
readonly maximum_glibc="GLIBC_2.34"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "发布产物只能在 Linux ARM64 上验证" >&2
  exit 1
fi
if [[ -z "${artifact}" || ! -f "${artifact}" ]]; then
  echo "发布产物不存在: ${artifact:-<empty>}" >&2
  exit 1
fi
if [[ "$(basename "${artifact}")" != "${expected_name}" ]]; then
  echo "发布文件名错误: 期望 ${expected_name}，实际 $(basename "${artifact}")" >&2
  exit 1
fi
if [[ ! -x "${artifact}" ]]; then
  echo "发布产物没有可执行权限: ${artifact}" >&2
  exit 1
fi

file_description="$(file -b "${artifact}")"
if [[ "${file_description}" != *"ELF 64-bit LSB"* \
  || "${file_description}" != *"ARM aarch64"* ]]; then
  echo "发布产物不是 AArch64 ELF: ${file_description}" >&2
  exit 1
fi
elf_header="$(readelf -hW "${artifact}")"
if ! grep -Eq 'Machine:[[:space:]]+AArch64$' <<<"${elf_header}"; then
  echo "readelf 未识别到 AArch64 机器类型" >&2
  exit 1
fi
program_headers="$(readelf -lW "${artifact}")"
if ! grep -Fq '[Requesting program interpreter: /lib/ld-linux-aarch64.so.1]' \
  <<<"${program_headers}"; then
  echo "发布产物不是预期的 aarch64 GNU/glibc 动态可执行文件" >&2
  exit 1
fi

dynamic_section="$(readelf -dW "${artifact}")"
if ! grep -Fq 'Shared library: [libc.so.6]' <<<"${dynamic_section}"; then
  echo "发布产物没有声明 GNU libc 动态依赖" >&2
  exit 1
fi
if grep -Eq 'Shared library: \[(libssl|libcrypto)\.so' <<<"${dynamic_section}"; then
  echo "发布产物意外依赖 OpenSSL" >&2
  exit 1
fi

version_information="$(readelf --version-info -W "${artifact}")"
glibc_versions="$(grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' \
  <<<"${version_information}" | sort -Vu || true)"
if [[ -z "${glibc_versions}" ]]; then
  echo "发布产物没有可审计的 GLIBC 符号版本" >&2
  exit 1
fi
highest_glibc="$(tail -n 1 <<<"${glibc_versions}")"
highest_allowed="$(printf '%s\n%s\n' "${maximum_glibc}" "${highest_glibc}" \
  | sort -Vu | tail -n 1)"
if [[ "${highest_allowed}" != "${maximum_glibc}" ]]; then
  echo "GLIBC 符号版本过高: 最高 ${highest_glibc}，允许上限 ${maximum_glibc}" >&2
  exit 1
fi

# 产物来自当前构建流程，执行 help 是最小启动烟测，可发现动态加载器或共享库缺失。
timeout 15 "${artifact}" --help >/dev/null
echo "发布产物校验通过: ${file_description}; 最高 ${highest_glibc}"
