//! code-sign MCP 工具、source 识别和专用启动行为。
//!
//! `sign_file` 只接受绝对输入/输出路径。source 匹配先解析 symlink，再退回绝对
//! lexical path；仓库根或其子目录都映射到同一 startup Git source。签名文件和
//! `/tmp` discovery 配置以 0600 创建。

use std::{
    fs::OpenOptions as StdOpenOptions,
    io::Write as _,
    os::unix::fs::OpenOptionsExt as _,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

use async_trait::async_trait;
use serde_json::json;
use tokio::{fs::OpenOptions, io::AsyncWriteExt as _};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    config::Source,
    mcp::{
        CallToolRequest, CallToolResult, MCPServer, McpError, McpResult, ServerConfig, Tool,
        ToolConfig, ToolHandler, new_tool_result_error, new_tool_result_text,
        zz_recovered_types::log_attributes,
    },
};

use super::{CodeSignMCPServer, SourceInfo, read_file_content};

const CONFIG_PATH: &str = "/tmp/codesign-mcp-config.json";
static ENVIRONMENT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

impl CodeSignMCPServer {
    /// 创建 code-sign 唯一的 `sign_file` 工具。
    #[must_use]
    pub fn get_tools(self: &Arc<Self>) -> Vec<ToolConfig> {
        let server = Arc::clone(self);
        vec![ToolConfig {
            tool: Tool::new(
                "sign_file",
                "Sign a file's contents with SSH-style Ed25519 signature. This tool reads a file and signs its contents for git commits and other authentication needs.",
                json!({
                    "type": "object",
                    "properties": {
                        "file_path": {
                            "type": "string",
                            "description": "Path to the file to sign"
                        },
                        "output_path": {
                            "type": "string",
                            "description": "Optional path where to write the signature (defaults to <file_path>.sig)"
                        },
                        "repo_directory": {
                            "type": "string",
                            "description": "The working directory of the git repository being signed"
                        },
                        "git_object_format": {
                            "type": "string",
                            "description": "Git object format (sha1 or sha256)",
                            "default": "sha1"
                        }
                    },
                    "required": ["file_path"]
                }),
            ),
            handler: ToolHandler::new(move |cancellation, request| {
                let server = Arc::clone(&server);
                async move { server.handle_sign_file(&cancellation, &request).await }
            }),
        }]
    }

    /// 读取、签名并以 0600 mode 写出一个绝对路径文件。
    pub async fn handle_sign_file(
        &self,
        cancellation: &CancellationToken,
        request: &CallToolRequest,
    ) -> McpResult<CallToolResult> {
        let file_path = request
            .require_string("file_path")
            .map_err(|error| McpError::message(format!("file_path is required: {error}")))?;
        let mut output_path = request.get_string("output_path", "");
        let repo_directory = request.get_string("repo_directory", "");
        let git_object_format = request.get_string("git_object_format", "sha1");
        if output_path.is_empty() {
            output_path = format!("{file_path}.sig");
        }

        let file_path_ref = Path::new(&file_path);
        if !file_path_ref.is_absolute() {
            self.base_server.logger.error(
                "File path must be absolute",
                log_attributes([("file_path", json!(&file_path))]),
            );
            return Ok(new_tool_result_error(format!(
                "Error: File path must be absolute, got: {file_path}"
            )));
        }
        let output_path_ref = Path::new(&output_path);
        if !output_path_ref.is_absolute() {
            self.base_server.logger.error(
                "Output path must be absolute",
                log_attributes([("output_path", json!(&output_path))]),
            );
            return Ok(new_tool_result_error(format!(
                "Error: Output path must be absolute, got: {output_path}"
            )));
        }

        let source_info = self.identify_source(&repo_directory);
        let source_type = source_info
            .source
            .as_ref()
            .map_or("", crate::internal::config::Source::get_type);
        self.base_server.logger.info(
            "Handling sign file request",
            log_attributes([
                ("file_path", json!(&file_path)),
                ("output_path", json!(&output_path)),
                ("repo_directory", json!(&repo_directory)),
                ("source_name", json!(&source_info.name)),
                ("source_type", json!(source_type)),
                ("git_object_format", json!(&git_object_format)),
            ]),
        );
        let content = match read_file_content(file_path_ref).await {
            Ok(content) => content,
            Err(error) => {
                self.base_server.logger.error(
                    "Failed to read file",
                    log_attributes([
                        ("error", json!(error.to_string())),
                        ("file_path", json!(&file_path)),
                    ]),
                );
                return Ok(new_tool_result_error(format!(
                    "Error: Cannot read file {file_path}: {error}"
                )));
            }
        };
        let signature = match self
            .sign_content(cancellation, content, &source_info, git_object_format)
            .await
        {
            Ok(signature) => signature,
            Err(error) => {
                self.base_server.logger.error(
                    "Signing failed",
                    log_attributes([("error", json!(error.to_string()))]),
                );
                return Ok(new_tool_result_error(format!("Signing failed: {error}")));
            }
        };

        // open、write 和 flush 都对应 Go 的单次 `os.WriteFile`；合并成一个 Result
        // 后只保留一个日志调用点，避免同一 Go source 被多个 Rust consumer 消费。
        let write_result = async {
            let mut output = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(output_path_ref)
                .await?;
            output.write_all(signature.as_bytes()).await?;
            // Tokio 文件通过阻塞线程桥接异步写入；`write_all` 返回时数据可能仍
            // 在后台写入。必须等待 flush，才能复刻 Go `os.WriteFile` 返回后读者立即
            // 可见完整签名的同步语义，并把 flush 失败纳入同一写文件错误分支。
            output.flush().await
        }
        .await;
        if let Err(error) = write_result {
            self.base_server.logger.error(
                "Failed to write signature",
                log_attributes([
                    ("error", json!(error.to_string())),
                    ("output_path", json!(&output_path)),
                ]),
            );
            return Ok(new_tool_result_error(format!(
                "Failed to write signature to {output_path}: {error}"
            )));
        }
        Ok(new_tool_result_text(format!(
            "Successfully signed file {file_path}\nSignature written to: {output_path}"
        )))
    }

    /// 将 repo directory 映射到 startup context 中的 Git source。
    #[must_use]
    pub fn identify_source(&self, repo_directory: &str) -> SourceInfo {
        if repo_directory.is_empty() {
            self.base_server.logger.debug(
                "No repo directory provided, cannot identify source",
                log_attributes([]),
            );
            return SourceInfo {
                name: "unknown".to_owned(),
                source: None,
            };
        }
        // Go 用同一个短路条件处理“上下文缺失”和“source 列表为空”。通过 `filter`
        // 保留一个失败分支，确保两种状态都从同一日志源位置发出完全相同的事件。
        let Some(startup_context) = self
            .startup_context
            .as_deref()
            .filter(|context| !context.sources.is_empty())
        else {
            self.base_server.logger.debug(
                "No startup context or sources available for mapping",
                log_attributes([("repo_directory", json!(repo_directory))]),
            );
            return SourceInfo {
                name: "unknown".to_owned(),
                source: None,
            };
        };

        let canonical_repo = canonical_or_absolute(Path::new(repo_directory));
        for source in &startup_context.sources {
            let source_directory = source.get_directory(&self.working_dir);
            let canonical_source = canonical_or_absolute(&source_directory);
            let exact_match = canonical_repo == canonical_source;
            if !exact_match && !canonical_repo.starts_with(&canonical_source) {
                continue;
            }
            let Source::GitRepository(repository) = source;
            let source_info = SourceInfo {
                name: format!(
                    "{} ({})",
                    repository.git_info.repo, repository.git_info.git_type
                ),
                source: Some(source.clone()),
            };
            let attributes = || {
                log_attributes([
                    ("repo_directory", json!(repo_directory)),
                    (
                        "canonical_repo_directory",
                        json!(canonical_repo.to_string_lossy()),
                    ),
                    (
                        "source_directory",
                        json!(source_directory.to_string_lossy()),
                    ),
                    (
                        "canonical_source_directory",
                        json!(canonical_source.to_string_lossy()),
                    ),
                ])
            };
            if exact_match {
                self.base_server
                    .logger
                    .info("Identified exact source match", attributes());
            } else {
                self.base_server
                    .logger
                    .info("Identified source (subdirectory)", attributes());
            }
            return source_info;
        }
        self.base_server.logger.warn(
            "Could not identify source for repo directory",
            log_attributes([("repo_directory", json!(repo_directory))]),
        );
        SourceInfo {
            name: "unknown".to_owned(),
            source: None,
        }
    }

    /// 启动基础 server，并写出专用 discovery 环境变量与 `/tmp` 配置。
    pub async fn start(&self, tools: Vec<ToolConfig>) -> McpResult<ServerConfig> {
        let config =
            self.base_server.start(tools).await.map_err(|error| {
                McpError::message(format!("failed to start base server: {error}"))
            })?;

        // Rust 2024 将进程环境修改标为 unsafe；全模块锁保证本模块不会并发修改，应用
        // 启动流程也在 worker/子进程启动前完成 MCP 配置。
        let _guard = ENVIRONMENT_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: 调用发生在 MCP 启动配置阶段，并由进程级锁串行化；值均为 owned UTF-8。
        unsafe {
            std::env::set_var("CODESIGN_MCP_PORT", config.port.to_string());
            std::env::set_var("CODESIGN_MCP_TOKEN", &config.bearer_token);
        }

        if let Ok(data) = serde_json::to_vec(&json!({
            "port": config.port,
            "bearer_token": config.bearer_token,
        })) {
            // 与 Go os.WriteFile 一致：新文件以 0600 创建，已有文件只截断、不重 chmod。
            let write_result = (|| -> std::io::Result<()> {
                let mut file = StdOpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .mode(0o600)
                    .open(CONFIG_PATH)?;
                file.write_all(&data)
            })();
            if let Err(error) = write_result {
                self.base_server.logger.warn(
                    "Failed to write codesign MCP config",
                    log_attributes([
                        ("config_path", json!(CONFIG_PATH)),
                        ("error", json!(error.to_string())),
                        ("server_name", json!(&self.base_server.name)),
                    ]),
                );
            }
        }
        self.base_server.logger.info(
            "Code signing MCP server started",
            log_attributes([("port", json!(config.port))]),
        );
        Ok(config)
    }

    /// code-sign 由环境变量发现，因此不执行 `claude mcp add`。
    #[must_use]
    pub fn should_register_with_claude(&self) -> bool {
        false
    }
}

#[async_trait]
impl MCPServer for CodeSignMCPServer {
    /// 返回基础 server 名称。
    fn get_name(&self) -> String {
        self.base_server.get_name()
    }

    /// 构造 `sign_file` 工具。
    fn get_tools(self: Arc<Self>) -> Vec<ToolConfig> {
        CodeSignMCPServer::get_tools(&self)
    }

    /// 委托专用启动流程。
    async fn start(&self, tools: Vec<ToolConfig>) -> McpResult<ServerConfig> {
        CodeSignMCPServer::start(self, tools).await
    }

    /// 停止基础 loopback server。
    async fn stop(&self) {
        self.base_server.stop().await;
    }

    /// 返回 false，避免重复注册。
    fn should_register_with_claude(&self) -> bool {
        false
    }
}

/// symlink 解析失败时退回绝对、去除 `.`/可消解 `..` 的 lexical path。
fn canonical_or_absolute(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let mut clean = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                clean.pop();
            }
            other => clean.push(other.as_os_str()),
        }
    }
    clean
}
