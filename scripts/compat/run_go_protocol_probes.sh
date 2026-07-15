#!/usr/bin/env bash
# 在固定 Go 1.25.7 Linux ARM64 容器内，用 overlay 对只读 oracle 注入协议 probe。
set -euo pipefail

readonly script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly repo_root="$(cd "${script_dir}/../.." && pwd -P)"
# 保留 verify.sh 显式传入的 digest；oracle-images.env 只应提供缺省值，不能把已审核
# 的发布参数悄悄换回仓库默认镜像。
requested_go_oracle_image="${GO_ORACLE_IMAGE:-}"
# shellcheck source=/dev/null
source "${repo_root}/compat/oracle-images.env"
if [[ -n "${requested_go_oracle_image}" ]]; then
  GO_ORACLE_IMAGE="${requested_go_oracle_image}"
fi

readonly expected_head="0706a81d488857f87cf13b68f5c8d2bb54b61834"
readonly expected_diff_sha256="fa90df15874d704b257f68c16ca0983c24af705e82d980ef3e762d852bae87ca"
readonly expected_executor_sha256="26a70bad574108d8b3344003b965e8edc79085f2087bf362456c01ca3791628b"
readonly expected_executor_test_sha256="4ce510a82b4b1c73d31526a30368d3d39a2a55e3e55234237190824fee237e1b"
readonly expected_makefile_sha256="cc5f4b15b15901813f7b0ede9b72b0d91cc8b78f8e06989f0e8a143a5b057311"
readonly expected_reference_elf_sha256="1c6a4f260f77ad12fa19186ad1fbe0b85f5a2f904bafbd5b11039f49be5f58c0"

# 与 build_go_oracle.sh 使用完全相同的 source 优先级，确保二进制 oracle 和 overlay
# probe 不会在一次门禁中意外读取两棵不同的 Go 工作树。
readonly go_source="${GO_SOURCE_DIR:-${GO_SOURCE_ROOT:-/mnt/mac/Users/arthur/GolandProjects/environment-manager}}"
readonly probe_dir="${repo_root}/compat/differential/probes/go"
readonly process_edge_probe_dir="${repo_root}/compat/go-probes/process-edge"
# Podman 把相对 `--volume` source 当成 named volume；先保留调用参数，创建目录后再
# 解析为真实绝对路径，使手工传入相对 target 目录也不会悄悄换成容器卷。
output_dir="${1:-${repo_root}/target/differential/protocol/go}"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "Go 协议 probe 只允许在 Linux ARM64 上运行" >&2
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
for probe in api_probe_test.go input_probe_test.go mcp_probe_test.go tunnel_protojson_probe_test.go tunnel_runtime_probe_test.go; do
  if [[ ! -f "${probe_dir}/${probe}" ]]; then
    echo "缺少 Go 协议 probe: ${probe_dir}/${probe}" >&2
    exit 2
  fi
done
if [[ ! -f "${process_edge_probe_dir}/main.go" ]]; then
  echo "缺少 Go process-edge probe: ${process_edge_probe_dir}/main.go" >&2
  exit 2
fi

# 与二进制 oracle 构建使用相同的不可变基线检查。这里不能只相信调用者传来的路径，
# 否则开发者可能把 probe 注入另一棵碰巧可以编译、但行为已漂移的 Go 工作树。
actual_head="$(git -C "${go_source}" rev-parse HEAD)"
actual_diff_sha256="$(LC_ALL=C git -C "${go_source}" diff --binary HEAD | sha256sum | awk '{print $1}')"
if [[ "${actual_head}" != "${expected_head}" ]]; then
  echo "Go HEAD 漂移: got ${actual_head}, want ${expected_head}" >&2
  exit 2
fi
if [[ "${actual_diff_sha256}" != "${expected_diff_sha256}" ]]; then
  echo "Go 工作树 diff 漂移: got ${actual_diff_sha256}, want ${expected_diff_sha256}" >&2
  exit 2
fi

# 对未提交 CCR 基线和未跟踪 Makefile 单独校验，因为它们不能仅由 HEAD 证明。
verify_baseline_file() {
  local relative_path="$1"
  local expected_sha256="$2"
  local actual_sha256
  actual_sha256="$(sha256sum "${go_source}/${relative_path}" | awk '{print $1}')"
  if [[ "${actual_sha256}" != "${expected_sha256}" ]]; then
    echo "Go 基线文件漂移 ${relative_path}: got ${actual_sha256}, want ${expected_sha256}" >&2
    exit 2
  fi
}
verify_baseline_file "internal/claude/claude_code_executor.go" "${expected_executor_sha256}"
verify_baseline_file "internal/claude/claude_code_executor_test.go" "${expected_executor_test_sha256}"
verify_baseline_file "Makefile" "${expected_makefile_sha256}"
verify_baseline_file "environment-manager-06-25" "${expected_reference_elf_sha256}"

# OrbStack VM 可能没有 rootless Podman 所需的 subuid/subgid。与 oracle 构建脚本一致，
# 仅在映射缺失时回退到无密码 sudo；Go 源码仍以 :ro 挂载，容器无法写回。
podman_command=(podman)
if ! grep -q "^$(id -un):" /etc/subuid 2>/dev/null; then
  if ! sudo -n true 2>/dev/null; then
    echo "Podman 缺少 subuid/subgid，且当前用户不能无密码 sudo" >&2
    exit 2
  fi
  podman_command=(sudo podman)
fi

# 与二进制 oracle 使用相同的 mode-specific cache schema。rootful Podman 会在宿主
# 留下 root-owned 哈希目录；把它交给 rootless 容器复用会在 Go 写新 entry 时失败。
# 分离 namespace 比递归 chown 更安全，也避免两个 Podman store 并发改变所有权。
if [[ "${podman_command[0]}" == "sudo" ]]; then
  cache_namespace="sudo-v2"
else
  cache_namespace="rootless-v2"
fi
readonly cache_dir="${repo_root}/target/differential/go-cache-${cache_namespace}"

if ! "${podman_command[@]}" image exists "${GO_ORACLE_IMAGE}"; then
  "${podman_command[@]}" pull "${GO_ORACLE_IMAGE}"
fi
actual_go_version="$("${podman_command[@]}" run --rm --pull=never "${GO_ORACLE_IMAGE}" go version)"
if [[ "${actual_go_version}" != "go version go1.25.7 linux/arm64" ]]; then
  echo "固定镜像工具链不符: ${actual_go_version}" >&2
  exit 2
fi

mkdir -p "${output_dir}" "${cache_dir}/build" "${cache_dir}/mod"
output_dir="$(cd "${output_dir}" && pwd -P)"
readonly output_dir
rm -f \
  "${output_dir}/http_api.json" \
  "${output_dir}/input.json" \
  "${output_dir}/mcp.json" \
  "${output_dir}/process_edge.raw" \
  "${output_dir}/process_edge.json" \
  "${output_dir}/tunnel_protojson.json" \
  "${output_dir}/tunnel_http_runtime.json" \
  "${output_dir}/tunnel_ws_runtime.json"

# 五个 overlay 映射到 oracle 包内不存在的 `_test.go` 虚拟路径，并生成原有六份协议
# transcript。process-edge 是只依赖 Go 标准库的独立 probe，从 Rust 仓库只读挂载；
# 两类 probe 都只能写 /out，绝不向 Go oracle 工作树回写文件。
"${podman_command[@]}" run --rm --pull=never --arch arm64 \
  --volume "${go_source}:/src:ro" \
  --volume "${probe_dir}:/probes:ro" \
  --volume "${process_edge_probe_dir}:/process-edge-probe:ro" \
  --volume "${output_dir}:/out" \
  --volume "${cache_dir}/build:/cache/build" \
  --volume "${cache_dir}/mod:/cache/mod" \
  --workdir /src \
  --env CGO_ENABLED=1 \
  --env GOOS=linux \
  --env GOARCH=arm64 \
  --env GOCACHE=/cache/build \
  --env GOMODCACHE=/cache/mod \
  --env MCP_DEBUG_TOKEN=1 \
  "${GO_ORACLE_IMAGE}" \
  sh -euc '
    cat >/tmp/api-overlay.json <<EOF
{"Replace":{"/src/internal/api/protocol_differential_probe_test.go":"/probes/api_probe_test.go"}}
EOF
    cat >/tmp/mcp-overlay.json <<EOF
{"Replace":{"/src/internal/mcp/protocol_differential_probe_test.go":"/probes/mcp_probe_test.go"}}
EOF
    cat >/tmp/input-overlay.json <<EOF
{"Replace":{"/src/internal/input/protocol_differential_probe_test.go":"/probes/input_probe_test.go"}}
EOF
    cat >/tmp/tunnel-overlay.json <<EOF
{"Replace":{"/src/gen/proto/anthropic/sessions/tunnel/v1alpha/protocol_differential_probe_test.go":"/probes/tunnel_protojson_probe_test.go"}}
EOF
    cat >/tmp/tunnel-runtime-overlay.json <<EOF
{"Replace":{"/src/internal/tunnel/protocol_differential_runtime_probe_test.go":"/probes/tunnel_runtime_probe_test.go"}}
EOF

    PROTOCOL_TRANSCRIPT_OUTPUT=/out/http_api.json \
      go test -mod=readonly -count=1 -overlay=/tmp/api-overlay.json \
      -run "^TestProtocolDifferentialTranscript$" ./internal/api
    PROTOCOL_TRANSCRIPT_OUTPUT=/out/mcp.json \
      go test -mod=readonly -count=1 -overlay=/tmp/mcp-overlay.json \
      -run "^TestProtocolDifferentialTranscript$" ./internal/mcp
    PROTOCOL_TRANSCRIPT_OUTPUT=/out/input.json \
      go test -mod=readonly -count=1 -overlay=/tmp/input-overlay.json \
      -run "^TestProtocolInputDifferentialTranscript$" ./internal/input
    PROTOCOL_TRANSCRIPT_OUTPUT=/out/tunnel_protojson.json \
      go test -mod=readonly -count=1 -overlay=/tmp/tunnel-overlay.json \
      -run "^TestProtocolDifferentialTranscript$" ./gen/proto/anthropic/sessions/tunnel/v1alpha
    PROTOCOL_HTTP_TUNNEL_OUTPUT=/out/tunnel_http_runtime.json \
    PROTOCOL_WS_TUNNEL_OUTPUT=/out/tunnel_ws_runtime.json \
      go test -mod=readonly -count=1 -overlay=/tmp/tunnel-runtime-overlay.json \
      -run "^TestProtocolTunnelRuntimeDifferentialTranscript$" ./internal/tunnel
    go run /process-edge-probe/main.go >/out/process_edge.raw
  '

# sudo Podman 创建的 transcript 属于 root，只修正门禁产物，不触碰只读 Go 工作树。
if [[ "${podman_command[0]}" == "sudo" ]]; then
  sudo chown -R "$(id -u):$(id -g)" "${output_dir}"
fi

# 原始 process-edge 文件保留实际临时根和 source FD；统一转换器只替换 probe 明确
# 声明的精确根字节，以及三个已验证不同、>=64 且带 CLOEXEC 的 FD 数值。
python3 "${script_dir}/process_edge_transcript.py" \
  --source go \
  --raw "${output_dir}/process_edge.raw" \
  --output "${output_dir}/process_edge.json"

# 输出必须是七个非空 JSON object。协议内容是否相等由后续严格比较器判断；runner
# 在这里先拒绝 probe 没运行、写入半截或把测试日志误当 transcript 的情况。
python3 - "${output_dir}" <<'PY'
import json
import sys
from pathlib import Path

directory = Path(sys.argv[1])
for name in (
    "http_api",
    "input",
    "mcp",
    "process_edge",
    "tunnel_protojson",
    "tunnel_http_runtime",
    "tunnel_ws_runtime",
):
    path = directory / f"{name}.json"
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"无效 Go 协议 transcript {path}: {error}") from error
    if not isinstance(document, dict) or document.get("protocol") != name:
        raise SystemExit(f"Go 协议 transcript 标识不符: {path}")
PY

printf '%s\n' "${output_dir}"
