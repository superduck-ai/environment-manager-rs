//! 配置默认值、路径计算与业务校验。
//!
//! 校验错误文本尽量保持 措辞，使 CLI 错误快照和调用方的包含匹配能够
//! 在继续工作。

use std::path::{Path, PathBuf};

use thiserror::Error;

use super::types::{
    BaseSource, ClaudeConfig, GitRepositorySource, McpServerConfig, Session, Source, StartupContext,
};

/// 配置解析后的业务校验错误。
#[derive(Debug, Error)]
pub enum ConfigError {
    /// source 缺少顶层类型。
    #[error("source type is required")]
    SourceTypeRequired,
    /// Git source 缺少仓库名。
    #[error("repository is required")]
    RepositoryRequired,
    /// MCP 同时提供了远端和本地字段。
    #[error(
        "MCP server config cannot have both remote fields (type/url) and local fields (command/args)"
    )]
    McpMixedRemoteAndLocal,
    /// MCP 既没有远端字段也没有本地命令。
    #[error(
        "MCP server config must specify either remote fields (type/url) or local fields (command)"
    )]
    McpMissingTransport,
    /// 远端 MCP 缺少 URL。
    #[error("remote MCP server must have a URL")]
    McpRemoteUrlRequired,
    /// startup context 缺少 API base URL。
    #[error("api_base_url is required in startup_context")]
    ApiBaseUrlRequired,
    /// 指定索引的 source 校验失败。
    #[error("source[{index}] validation failed: {source}")]
    InvalidSource {
        /// source 在输入数组中的索引。
        index: usize,
        /// 具体 source 校验错误。
        #[source]
        source: Box<ConfigError>,
    },
    /// session 缺少 ID。
    #[error("session id is required")]
    SessionIdRequired,
    /// session 缺少 startup context。
    #[error("startup context is required")]
    StartupContextRequired,
    /// session 中的 startup context 无效。
    #[error("invalid startup context: {0}")]
    InvalidStartupContext(Box<ConfigError>),
}

/// 创建供 session 使用的默认 Claude 配置。
///
/// `args` 和 `environment` 会被显式初始化为空集合，其余字段采用空值或 false。
#[must_use]
pub fn default_claude_config() -> ClaudeConfig {
    ClaudeConfig {
        command: "claude".to_owned(),
        args: Vec::new(),
        environment: std::collections::HashMap::new(),
        working_dir: String::new(),
        timeout: std::time::Duration::ZERO,
        allowed_tools: String::new(),
        local_testing_mode: false,
        disallowed_tools: String::new(),
        verbose_logging: false,
        local_append_system_prompt: String::new(),
        claude_path: String::new(),
        print_code_logs: false,
        skip_claude_code: false,
    }
}

impl BaseSource {
    /// 返回 source 的 wire 类型标签。
    #[must_use]
    pub fn get_type(&self) -> &str {
        &self.source_type
    }

    /// 基础 source 不是仓库；具体仓库类型会覆盖这一语义。
    #[must_use]
    pub fn is_repository(&self) -> bool {
        false
    }

    /// 基础 source 没有独立目录，因此原样返回 base directory。
    #[must_use]
    pub fn get_directory(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        base_dir.as_ref().to_path_buf()
    }
}

impl GitRepositorySource {
    /// 验证 source 类型和仓库名均非空。
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.base.source_type.is_empty() {
            return Err(ConfigError::SourceTypeRequired);
        }
        if self.git_info.repo.is_empty() {
            return Err(ConfigError::RepositoryRequired);
        }
        Ok(())
    }

    /// Git repository source 始终被视为仓库。
    #[must_use]
    pub fn is_repository(&self) -> bool {
        true
    }

    /// 用仓库名最后一个路径分量计算 clone 目录。
    ///
    /// 空仓库名和无法取得文件名的异常路径会安全回退到 base directory。
    #[must_use]
    pub fn get_directory(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        if self.git_info.repo.is_empty() {
            return base_dir.as_ref().to_path_buf();
        }
        Path::new(&self.git_info.repo).file_name().map_or_else(
            || base_dir.as_ref().to_path_buf(),
            |name| base_dir.as_ref().join(name),
        )
    }
}

impl Source {
    /// 返回联合 source 的类型标签。
    #[must_use]
    pub fn get_type(&self) -> &str {
        match self {
            Self::GitRepository(source) => source.base.get_type(),
        }
    }

    /// 判断联合 source 是否代表仓库。
    #[must_use]
    pub fn is_repository(&self) -> bool {
        match self {
            Self::GitRepository(source) => source.is_repository(),
        }
    }

    /// 计算联合 source 在 workspace 中的目标目录。
    #[must_use]
    pub fn get_directory(&self, base_dir: impl AsRef<Path>) -> PathBuf {
        match self {
            Self::GitRepository(source) => source.get_directory(base_dir),
        }
    }

    /// 调用具体 source 的业务校验。
    pub fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::GitRepository(source) => source.validate(),
        }
    }
}

impl McpServerConfig {
    /// 判断该 server 是否使用已知远端传输。
    ///
    /// 与 一致：只要 `type` 非空就只按 `sse|http|ws` 判断；仅当 `type`
    /// 为空时，非空 URL 才会令结果为 true。
    #[must_use]
    pub fn is_remote(&self) -> bool {
        if !self.server_type.is_empty() {
            matches!(self.server_type.as_str(), "sse" | "http" | "ws")
        } else {
            !self.url.is_empty()
        }
    }

    /// 验证本地/远端 MCP 配置互斥且具备必要字段。
    pub fn validate(&self) -> Result<(), ConfigError> {
        let has_remote_fields = !self.server_type.is_empty() || !self.url.is_empty();
        let has_local_fields = !self.command.is_empty();
        if has_remote_fields && has_local_fields {
            return Err(ConfigError::McpMixedRemoteAndLocal);
        }
        if !has_remote_fields && !has_local_fields {
            return Err(ConfigError::McpMissingTransport);
        }
        if has_remote_fields && self.url.is_empty() {
            return Err(ConfigError::McpRemoteUrlRequired);
        }
        Ok(())
    }
}

impl StartupContext {
    /// 验证 API base URL 以及每个 source。
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.api_base_url.is_empty() {
            return Err(ConfigError::ApiBaseUrlRequired);
        }
        for (index, source) in self.sources.iter().enumerate() {
            source
                .validate()
                .map_err(|source| ConfigError::InvalidSource {
                    index,
                    source: Box::new(source),
                })?;
        }
        Ok(())
    }
}

impl Session {
    /// 验证 session ID、startup context 存在性及其内部约束。
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.session_id.is_empty() {
            return Err(ConfigError::SessionIdRequired);
        }
        let startup_context = self
            .startup_context
            .as_ref()
            .ok_or(ConfigError::StartupContextRequired)?;
        startup_context
            .validate()
            .map_err(|error| ConfigError::InvalidStartupContext(Box::new(error)))
    }
}
