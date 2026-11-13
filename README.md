# Environment Manager

English | [简体中文](README.zh-CN.md)

A Rust implementation of the Linux environment manager, with an `environment-runner` compatible CLI. It manages agent session environments, polls for work, prepares repositories and packages, runs Claude Code sessions, and exposes embedded MCP runtime tools.

## Requirements

- Rust 1.97.0, pinned in `rust-toolchain.toml`.
- A Linux GNU target for the main application. CPU architecture is not restricted; native macOS, Windows, and musl builds of the manager are rejected by the source.
- Runtime programs for the features you use: Git for repository operations, Bash and ripgrep (`rg`) for MCP tools, and npm for the `setup` command.
- Backend connectivity and session credentials for API-backed commands.

The Docker build below can be launched from macOS or Linux. It produces a Linux executable that must run on Linux.

## Build

On Linux GNU with the pinned Rust toolchain:

```sh
cargo build --locked --release
./target/release/environment-manager --help
```

To build Linux ARM64 using Docker, run from the repository root:

```sh
scripts/linux-arm64/docker-build.sh
# Or, with just installed:
just build
```

Docker must support `linux/arm64`; an x86_64 host needs ARM64 emulation. The image uses AlmaLinux 9, Rust 1.97.0, and rsproxy for Rust and Cargo downloads. The output is:

```text
target/docker-linux-arm64/aarch64-unknown-linux-gnu/release/environment-manager
```

This GNU executable requires glibc 2.34 or newer and its dynamic library dependencies on the target system. See the [Linux ARM64 build and release guide](scripts/linux-arm64/README.md) for cache settings, verification, and artifact promotion.

## Upgrading without rebuilding the sandbox image

Open Managed Agents can replace the Manager without rebuilding the sandbox image. Point OMA's `environment_runner.manager_artifact` at a release artifact (for example `dist/environment-manager_linux_arm64` and its `.sha256`):

```yaml
environment_runner:
  manager_artifact:
    path: /srv/oma/environment-manager_linux_arm64
    # or a URL, which requires sha256:
    # url: https://artifacts.example.com/environment-manager_linux_arm64
    # sha256: <value from environment-manager_linux_arm64.sha256>
```

Every time the OMA Runner creates or resumes (`--session-mode resume-cached`) a sandbox, it uploads the binary to `/tmp/oma/environment-manager/<sha256>/environment-manager` before running `provision-packages` and `task-run`. It verifies the SHA-256, runs `--version` as a smoke test, and then uses that binary instead of the image's `/usr/local/bin/environment-manager`. With `path`, atomically replacing the file is enough for later sandboxes to pick up the new build. The artifact must match the sandbox architecture, and `--version` must exit 0 when called with no other arguments. See OMA's `docs/design/be/e2b-sandbox-image-contract.md` for the full contract.

## Usage

The executable is named `environment-manager`; its help and completion output use the compatible command name `environment-runner`.

| Command | Purpose |
| --- | --- |
| `orchestrator` | Continuously poll for session work and dispatch it through a hook or `task-run`; uses sandbox-runtime by default. |
| `poll` | Make one non-blocking work request; print work JSON or `null` for an empty queue. |
| `task-run` | Execute an existing session using a JSON payload from stdin. |
| `setup` | Install Claude Code and sandbox-runtime, with optional API connectivity checks. |
| `provision-packages` | Install packages from a versioned stdin manifest and print a JSON result. |
| `print-sandbox-settings` | Print the default sandbox configuration as JSON. |
| `completion` | Generate Bash, Zsh, Fish, or PowerShell completion scripts. |

Inspect command-specific options before configuring a deployment:

```sh
./target/release/environment-manager orchestrator --help
./target/release/environment-manager task-run --help
./target/release/environment-manager print-sandbox-settings
```

For `poll` and `orchestrator`, supply an environment service key using `--service-key-file` or `ENVIRONMENT_SERVICE_KEY`. Both commands discover environment and organization identity through the backend's `whoami` endpoint. Use `--api-url` to select a different backend.

```sh
./target/release/environment-manager poll --service-key-file /path/to/service-key
./target/release/environment-manager orchestrator --service-key-file /path/to/service-key
```

`task-run` requires `--session`. Its default input format is `v0`, containing `startup_context`, `environment`, and `auth`; `--input-format v1` accepts a work response with a packed secret. Supply a payload from your session backend:

```sh
./target/release/environment-manager task-run \
  --session YOUR_SESSION_ID --input-format v0 < session.json
```

Package provisioning requires both protocol flags and a valid v1 manifest:

```sh
./target/release/environment-manager provision-packages \
  --protocol v1 --stdin < packages.json
```

## MCP runtime tools

The workspace includes `mcp-server` as both an embeddable library and a standalone executable. It provides `bash`, `read`, `write`, `edit`, `grep`, and `glob`, along with compatibility aliases. Production builds do not require Go.

After environment initialization, `task-run` starts the embedded HTTP server at `0.0.0.0:3001`, with endpoint `/mcp`. Override the listen address with `ENVRUNNER_RUNTIME_MCP_ADDR`. The server uses the session's effective working directory and environment, and stops with the manager. Brain sessions using `skip_claude_code` expose the tools without creating a Claude Code executor.

To run the standalone server:

```sh
cargo build --locked --release -p mcp-server
./target/release/mcp-server -transport stdio -workdir /path/to/workspace
# Or use Streamable HTTP:
./target/release/mcp-server -transport http -addr :3001 -workdir /path/to/workspace
```

The current server only accepts MCP `2026-07-28`, using `server/discover` rather than the older `initialize` handshake. See the [MCP server guide](crates/mcp-server/README.md) for request metadata, HTTP headers, cancellation, and Host/Origin configuration.

## Development

Run workspace checks on Linux GNU:

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

The repository also provides `scripts/linux-arm64/verify.sh`, which synchronizes sources to an OrbStack Ubuntu 24.04 ARM64 VM and runs platform checks, dependency and documentation audits, formatting, Clippy, and tests. See its [setup and release documentation](scripts/linux-arm64/README.md) before using that environment-specific workflow.

## Repository guide

| Path | Contents |
| --- | --- |
| `src/cmd/` | CLI parsing and command execution. |
| `src/internal/` | Session manager, environments, API clients, Git, sandbox, relay, and observability. |
| `crates/mcp-server/` | MCP server, tool implementations, and protocol tests. |
| `gen/proto/` | Protocol definitions used by build-time Rust code generation. |
| `scripts/linux-arm64/` | Docker builds, verification, and release scripts. |
| `tests/` | Integration tests and historical MCP compatibility records. |

See [session environment and embedded MCP design](docs/design/session-runtime-environment.md) for environment precedence and Brain relay integration. Cargo package manifests declare `UNLICENSED`; image-related third-party notices are in [THIRD_PARTY_NOTICES.md](crates/mcp-server/THIRD_PARTY_NOTICES.md).
