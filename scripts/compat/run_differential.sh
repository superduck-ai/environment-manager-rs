#!/usr/bin/env bash
# 构建 Linux ARM64 oracle，并执行本地协议 transcript 差分门禁。
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "${script_dir}/../.." && pwd -P)"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "aarch64" ]]; then
  echo "差分测试只允许在 Linux ARM64 上运行" >&2
  exit 2
fi

# 先运行两个纯比较器测试，避免 normalizer、覆盖清单或递归字段比较本身发生回归
# 后产生假阳性。这些 Python 测试不构建目标程序，也不访问网络。
python3 -m unittest discover \
  --start-directory "${repo_root}/tests" \
  --pattern 'differential_*_test.py'

# 七份 transcript probe 使用与二进制 oracle 相同的固定 Go 1.25.7 digest 镜像。
# Go 测试通过 overlay 注入只读工作树；Rust 测试只连接进程内 loopback mock。两边
# 都写到 target 下的独立目录；process-edge 额外创建本机临时进程/pipe，但不访问
# staging，也不会在源码目录留下 fixture。
protocol_root="${PROTOCOL_TRANSCRIPT_ROOT:-${repo_root}/target/differential/protocol}"
go_protocol_dir="${protocol_root}/go"
rust_protocol_dir="${protocol_root}/rust"
mkdir -p "${go_protocol_dir}" "${rust_protocol_dir}"

"${script_dir}/run_go_protocol_probes.sh" "${go_protocol_dir}" >/dev/null

# MCP_DEBUG_TOKEN 只启用 probe 预置的固定 `fixed-mcp-token`，使 Bearer 认证本身可精确
# 比较而无需把随机 token 归一化。HTTP/WS runtime 的随机 loopback 端口也只用于连接，
# Host 以布尔契约验证，所有 transcript 均不记录随机端口。
# 先移除七份 Rust 旧产物；否则 probe 提前失败时，残留 transcript 可能让后续比较器
# 误读上一次运行结果。Go runner 内部执行同样的逐文件清理。
rm -f \
  "${rust_protocol_dir}/http_api.json" \
  "${rust_protocol_dir}/input.json" \
  "${rust_protocol_dir}/mcp.json" \
  "${rust_protocol_dir}/process_edge.raw" \
  "${rust_protocol_dir}/process_edge.json" \
  "${rust_protocol_dir}/tunnel_protojson.json" \
  "${rust_protocol_dir}/tunnel_http_runtime.json" \
  "${rust_protocol_dir}/tunnel_ws_runtime.json"
MCP_DEBUG_TOKEN=1 \
PROTOCOL_TRANSCRIPT_DIR="${rust_protocol_dir}" \
cargo +1.97.0 test --locked \
  --test protocol_differential_probe \
  protocol_differential_transcript \
  -- --ignored --exact --nocapture

# input parser 使用独立测试目标，避免 HTTP/Tunnel probe 的大型 fixture 与 panic 捕获
# 相互影响。固定 41791 端口只由 Go overlay 使用，Rust provider 直接注入同一 URL。
PROTOCOL_TRANSCRIPT_DIR="${rust_protocol_dir}" \
cargo +1.97.0 test --locked \
  --test input_differential_probe \
  input_protocol_differential_transcript \
  -- --ignored --exact --nocapture

# 低层进程 probe 使用 library 内部测试，才能直接覆盖生产 CombinedOutput、FD3+ 映射
# 与 reader-first wait 顺序。raw 文件保留动态根/FD，随后由 Go/Rust 共用转换器生成
# 第七份固定 JSON transcript；状态、errno、signal 和任意错误字节均不归一化。
PROCESS_EDGE_TRANSCRIPT_OUTPUT="${rust_protocol_dir}/process_edge.raw" \
cargo +1.97.0 test --locked --lib \
  internal::claude::process_edge_differential_test::write_process_edge_differential_transcript \
  -- --ignored --exact --nocapture
python3 "${script_dir}/process_edge_transcript.py" \
  --source rust \
  --raw "${rust_protocol_dir}/process_edge.raw" \
  --output "${rust_protocol_dir}/process_edge.json"

python3 "${script_dir}/differential_protocol.py" \
  --go-dir "${go_protocol_dir}" \
  --rust-dir "${rust_protocol_dir}" \
  --report "${protocol_root}/report.json"
