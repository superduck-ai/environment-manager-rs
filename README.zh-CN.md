# Environment Manager

[English](README.md) | 简体中文

使用 Rust 实现的 Linux 环境管理器，提供兼容 `environment-runner` 的命令行接口。它负责管理 Agent 会话环境、轮询任务、准备仓库与软件包、执行 Claude Code 会话，并提供嵌入式 MCP 运行时工具。

## 环境要求

- Rust 1.97.0，版本固定在 `rust-toolchain.toml` 中。
- 主程序要求 Linux GNU 构建目标，不限制 CPU 架构；源码会拒绝原生 macOS、Windows 和 musl 目标。
- 按功能提供运行依赖：仓库操作需要 Git，MCP 工具需要 Bash 和 ripgrep（`rg`），`setup` 命令需要 npm。
- 调用后端 API 的命令需要后端网络连接及会话凭据。

下面的 Docker 构建可以从 macOS 或 Linux 发起，生成的 Linux 可执行文件需要在 Linux 上运行。

## 构建

在 Linux GNU 环境安装指定 Rust 工具链后执行：

```sh
cargo build --locked --release
./target/release/environment-manager --help
```

通过 Docker 构建 Linux ARM64 版本，在仓库根目录执行：

```sh
scripts/linux-arm64/docker-build.sh
# 已安装 just 时也可以执行：
just build
```

Docker 必须支持 `linux/arm64`；x86_64 宿主需要 ARM64 模拟。镜像使用 AlmaLinux 9、Rust 1.97.0，并通过 rsproxy 下载 Rust 工具链和 Cargo 依赖。产物路径为：

```text
target/docker-linux-arm64/aarch64-unknown-linux-gnu/release/environment-manager
```

该 GNU 可执行文件要求目标系统提供 glibc 2.34 或更新版本及所需动态库。缓存配置、验证和产物提升流程见 [Linux ARM64 构建与发布指南](scripts/linux-arm64/README.md)。

## 独立于沙箱镜像升级

Open Managed Agents 可以在不重建沙箱镜像的情况下替换 Manager。将发布产物（例如 `dist/environment-manager_linux_arm64` 及其 `.sha256`）交给 OMA 的 `environment_runner.manager_artifact`：

```yaml
environment_runner:
  manager_artifact:
    path: /srv/oma/environment-manager_linux_arm64
    # 或使用 url，此时 sha256 必填：
    # url: https://artifacts.example.com/environment-manager_linux_arm64
    # sha256: <environment-manager_linux_arm64.sha256 中的值>
```

OMA 的 Runner 在每次新建或恢复（`--session-mode resume-cached`）Sandbox、执行 `provision-packages` 和 `task-run` 之前，把该二进制上传到 `/tmp/oma/environment-manager/<sha256>/environment-manager`，校验 SHA-256，并以 `--version` 做冒烟检查，然后用它替代镜像内的 `/usr/local/bin/environment-manager`。使用 `path` 时原子替换文件即可让后续 Sandbox 使用新版本。产物架构必须与 Sandbox 一致，`--version` 必须在没有其他参数时以 0 退出。完整合同见 OMA 的 `docs/design/be/e2b-sandbox-image-contract.md`。

## 使用方式

可执行文件名为 `environment-manager`，帮助信息和补全脚本使用兼容的命令名 `environment-runner`。

| 命令 | 用途 |
| --- | --- |
| `orchestrator` | 持续轮询会话任务，通过 hook 或 `task-run` 执行，默认使用 sandbox-runtime。 |
| `poll` | 发起一次非阻塞任务请求，输出任务 JSON，队列为空时输出 `null`。 |
| `task-run` | 从 stdin 读取 JSON 载荷，执行已有会话。 |
| `setup` | 安装 Claude Code 和 sandbox-runtime，可选检查后端 API 连通性。 |
| `provision-packages` | 从带版本的 stdin manifest 安装软件包，输出 JSON 结果。 |
| `print-sandbox-settings` | 以 JSON 输出默认沙箱配置。 |
| `completion` | 生成 Bash、Zsh、Fish 或 PowerShell 补全脚本。 |

部署前可查看各子命令的参数：

```sh
./target/release/environment-manager orchestrator --help
./target/release/environment-manager task-run --help
./target/release/environment-manager print-sandbox-settings
```

`poll` 和 `orchestrator` 通过 `--service-key-file` 或 `ENVIRONMENT_SERVICE_KEY` 接收环境服务密钥，通过后端 `whoami` 接口发现环境与组织身份。需要连接其他后端时使用 `--api-url`。

```sh
./target/release/environment-manager poll --service-key-file /path/to/service-key
./target/release/environment-manager orchestrator --service-key-file /path/to/service-key
```

`task-run` 必须传入 `--session`。默认输入格式为 `v0`，包含 `startup_context`、`environment` 和 `auth`；`--input-format v1` 接收带有 packed secret 的任务响应。使用会话后端提供的载荷运行：

```sh
./target/release/environment-manager task-run \
  --session YOUR_SESSION_ID --input-format v0 < session.json
```

软件包安装必须同时指定两个协议参数，并提供有效的 v1 manifest：

```sh
./target/release/environment-manager provision-packages \
  --protocol v1 --stdin < packages.json
```

## MCP 运行时工具

工作区内的 `mcp-server` 同时提供可嵌入的库和独立可执行文件，包含 `bash`、`read`、`write`、`edit`、`grep`、`glob` 及兼容别名。生产构建无需 Go。

`task-run` 在环境初始化完成后启动嵌入式 HTTP 服务，默认监听 `0.0.0.0:3001`，接口为 `/mcp`。可通过 `ENVRUNNER_RUNTIME_MCP_ADDR` 修改监听地址。服务使用会话最终生效的工作目录和环境变量，随 manager 停止。使用 `skip_claude_code` 的 Brain 会话会提供工具服务，但不创建 Claude Code executor。

独立运行服务：

```sh
cargo build --locked --release -p mcp-server
./target/release/mcp-server -transport stdio -workdir /path/to/workspace
# 或使用 Streamable HTTP：
./target/release/mcp-server -transport http -addr :3001 -workdir /path/to/workspace
```

当前服务仅接受 MCP `2026-07-28`，使用 `server/discover`，不使用旧版 `initialize` 握手。请求元数据、HTTP 请求头、取消机制和 Host/Origin 配置见 [MCP 服务指南](crates/mcp-server/README.md)。

## 开发与验证

在 Linux GNU 环境执行工作区检查：

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

仓库还提供 `scripts/linux-arm64/verify.sh`，将源码同步至 OrbStack Ubuntu 24.04 ARM64 虚拟机，执行平台校验、依赖与文档审计、格式检查、Clippy 和测试。该流程依赖特定环境，使用前请阅读[环境配置与发布说明](scripts/linux-arm64/README.md)。

## 目录说明

| 路径 | 内容 |
| --- | --- |
| `src/cmd/` | 命令行解析与命令执行。 |
| `src/internal/` | 会话管理、环境、API 客户端、Git、沙箱、relay 和可观测性。 |
| `crates/mcp-server/` | MCP 服务、工具实现与协议测试。 |
| `gen/proto/` | 构建期生成 Rust 代码所用的协议定义。 |
| `scripts/linux-arm64/` | Docker 构建、验证和发布脚本。 |
| `tests/` | 集成测试与历史 MCP 兼容性记录。 |

环境变量优先级与 Brain relay 集成见[会话环境与嵌入 MCP 设计](docs/design/session-runtime-environment.md)。Cargo 包清单声明的许可证为 `UNLICENSED`；图片相关第三方声明见 [THIRD_PARTY_NOTICES.md](crates/mcp-server/THIRD_PARTY_NOTICES.md)。
