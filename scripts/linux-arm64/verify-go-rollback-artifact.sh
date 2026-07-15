#!/usr/bin/env bash
# 验证 CGO=0 Go rollback 是可在权威 Linux ARM64 宿主直接启动的静态 ELF。
set -euo pipefail

readonly artifact="${1:-}"

if (($# != 1)); then
  echo "用法: $0 <go-rollback-artifact>" >&2
  exit 1
fi
if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "Go rollback 只能在 Linux ARM64 上验证" >&2
  exit 1
fi
if [[ ! -f "${artifact}" || -L "${artifact}" ]]; then
  echo "Go rollback 不存在、不是普通文件或是符号链接: ${artifact}" >&2
  exit 1
fi
if [[ ! -x "${artifact}" ]]; then
  echo "Go rollback 没有可执行权限: ${artifact}" >&2
  exit 1
fi

file_description="$(file -b "${artifact}")"
if [[ "${file_description}" != *"ELF 64-bit LSB"* \
  || "${file_description}" != *"ARM aarch64"* \
  || "${file_description}" != *"statically linked"* ]]; then
  echo "Go rollback 不是静态 AArch64 ELF: ${file_description}" >&2
  exit 1
fi

elf_header="$(readelf -hW "${artifact}")"
if ! grep -Eq 'Machine:[[:space:]]+AArch64$' <<<"${elf_header}"; then
  echo "readelf 未识别到 Go rollback 的 AArch64 机器类型" >&2
  exit 1
fi

# CGO=0 基线产物不应请求动态加载器。单独检查 INTERP 和 NEEDED，避免只依赖
# `file` 的描述文本；即使未来 binutils 文案变化，动态链接回归仍会被拒绝。
program_headers="$(readelf -lW "${artifact}")"
if grep -Eq '(^|[[:space:]])INTERP([[:space:]]|$)|Requesting program interpreter' \
  <<<"${program_headers}"; then
  echo "Go rollback 意外包含动态加载器 INTERP" >&2
  exit 1
fi
if ! dynamic_section="$(readelf -dW "${artifact}" 2>&1)"; then
  echo "无法读取 Go rollback 动态段: ${dynamic_section}" >&2
  exit 1
fi
if grep -Eq '\(NEEDED\)|Shared library:' <<<"${dynamic_section}"; then
  echo "Go rollback 意外声明动态共享库依赖" >&2
  exit 1
fi

# 结构正确并不代表宿主可以实际执行。运行无副作用的根 help 路径，捕获错误架构、
# 内核加载失败或启动期初始化崩溃；timeout 防止异常产物阻塞发布流程。
if timeout 15 "${artifact}" --help >/dev/null; then
  :
else
  status=$?
  echo "Go rollback --help 烟测失败，退出码 ${status}" >&2
  exit "${status}"
fi

echo "Go rollback 校验通过: ${file_description}；无 INTERP/NEEDED"
