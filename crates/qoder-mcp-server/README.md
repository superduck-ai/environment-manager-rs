# qoder-mcp-server（Rust）

`qoder-mcp-server` 的完整运行功能已迁入此 Cargo workspace，提供独立 `mcp-server` 可执行文件和可嵌入的 Rust 库。生产构建和运行无需 Go。

```sh
cargo build --locked --release -p qoder-mcp-server
./target/release/mcp-server -transport stdio -workdir /data
./target/release/mcp-server -transport http -addr :3001 -workdir /data
```

HTTP 接口为 `/mcp`。支持原有 `-env-socket`、`-core-addr`、`WORK_DIR` 和 CLI 参数语义。运行环境需提供 `bash`、`rg`；`bash` 支持 Unix 进程组、TERM/KILL、请求取消及后台输出文件。

| 功能 | 实现 |
| --- | --- |
| MCP | JSON-RPC、initialize、ping、tools/list、tools/call、通知、stdio 和 HTTP |
| 内置工具 | bash、read、write、edit、grep、glob |
| 原有别名 | run_in_terminal、file_read、file_write、file_edit、grep_run、glob_run |
| 文件 | 原子覆盖、符号链接、权限、BOM/CRLF、唯一替换、diff、分页、输出持久化 |
| 图片 | PNG/JPEG/GIF/WebP、尺寸/编码预算、缩放、透明度、JPEG 和 PNG 兼容编码 |
| 搜索 | ripgrep、排序/分页、glob brace/extglob/字符类/递归/排除规则 |
| 运行集成 | Unix socket 环境快照、变量覆盖、交互命令提示、插件生命周期和服务停止客户端 |

库入口为 `Registry`、`Tool`、`Server`、`EnvironmentProvider` 和 `plugins::Manager`。主项目通过 `internal::mcp::servers::runtime_tools` 重导出该库。

使用标准库文件和进程接口、Tokio、Hyper/Axum、serde、reqwest/rustls、regex 和 image/gif/image-webp。JSON、错误文本、图片整数精度和压缩决策的兼容适配独立在专用模块；无需调用参考程序或持有原项目。图片派生算法许可见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。

## 验证

```sh
cargo test --locked -p qoder-mcp-server
cargo clippy --locked -p qoder-mcp-server --all-targets -- -D warnings
# 直接对照现成 x86-64 Go 二进制，无需 Go 源码或工具链。
python3 tests/qoder_compatibility/binary_e2e/run.py
# 需 Docker 和本机 Go 参考项目；参考版本由其 go.mod 锁定。
QODER_GO_SOURCE=/Users/arthur/GolandProjects/qoder-mcp-server tests/qoder_compatibility/run.sh
```

完整对照保存到 `target/qoder-compatibility/recovery/evidence/`：检查 JSON 字段和省略规则、声明、错误文本、HTTP 状态/响应头、stderr、真实文件及权限、进程组取消、输出文件和图片 base64。只归一化 Go map 工具顺序、系统生成临时路径/PID/日志时间；严格字面量语料不归一化用户内容。JPEG/WebP 扩展语料对完整 JSON（含 base64）逐字比较。

兼容性结论以回归语料的零差异结果为依据；测试不能数学证明所有可能输入均等价。主项目自身限定 Linux GNU，workspace 全量门禁需在相应平台运行。

验收数字与比较范围见 [迁移验收记录](../../tests/qoder_compatibility/VERIFICATION.md)。

新增 [MCP 功能 E2E](../../tests/qoder_compatibility/binary_e2e/README.md) 已对照用户提供的现成 Go 二进制：修复环境快照边界后，release 和 debug 各 502 个步骤均零差异。测试输出完整响应、副作用与差异报告，出现任何差异会以非零状态退出。
