//! Anthropic sandbox-runtime（SRT）配置、包装和安装。
//!
//! 本模块配置结构保持原 JSON 字段名；自动生成配置
//! 由 RAII runtime 管理，而用户提供的自定义配置永远不会被 cleanup 删除。

mod config;
mod install;
mod runtime;
mod types;

pub use config::{
    ConfigError, cleanup_config_file, load_and_validate_config, validate_config, write_config_file,
};
pub use install::{SandboxInstallError, install_sandbox_runtime};
pub use runtime::{SandboxRuntimeError, default_config, new_sandbox_runtime_with_config};
pub use types::{FilesystemConfig, NetworkConfig, SandboxRuntime, SrtConfig};

#[cfg(test)]
mod sandbox_test;
