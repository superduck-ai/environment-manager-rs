//! sandbox runtime 初始化、命令包装和配置清理。
//!
//! 自动配置包含 Go 版本相同的 Anthropic 域名、敏感路径 deny-list 和写入目录；
//! 自定义配置只验证和引用，不改变内容、不接管所有权。

use std::{
    io,
    os::unix::fs::DirBuilderExt as _,
    path::{Path, PathBuf},
};

use thiserror::Error;

use super::{
    ConfigError, FilesystemConfig, NetworkConfig, SrtConfig, cleanup_config_file,
    load_and_validate_config, write_config_file,
};
/// runtime 初始化或清理错误。
#[derive(Debug, Error)]
pub enum SandboxRuntimeError {
    /// 自定义配置无法读取、解析或通过安全验证。
    #[error("failed to load custom sandbox config: {0}")]
    CustomConfig(#[source] ConfigError),
    /// 自动配置无法写入临时文件。
    #[error("failed to write sandbox config file: {0}")]
    GeneratedConfig(#[source] ConfigError),
    /// 自动配置 cleanup 失败。
    #[error(transparent)]
    Cleanup(#[from] ConfigError),
}

/// 一个已初始化的 SRT command wrapper。
pub struct SandboxRuntime {
    srt_binary: String,
    config_path: PathBuf,
    owns_config_file: bool,
    custom_config: bool,
}

/// 对应 Go：`internal/sandbox/runtime.go` 中为 `/tmp/claude` 调用的
/// `os.MkdirAll(path, 0755)`。
///
/// 以 `0755` 作为新建目录的权限上限，并继续让进程 `umask` 收紧权限。不能使用
/// `std::fs::create_dir_all` 的默认 `0777`，否则在 `umask 000` 的容器中会把 Claude
/// 临时目录扩大为所有用户可写。已有目录与 Go `MkdirAll` 一样不主动 chmod。
///
/// # 参数
/// - `path`：需要递归创建的 Linux 目录；调用方仍负责记录失败并决定是否继续。
///
/// # 返回
/// 创建成功或目录已存在时返回 `Ok(())`；路径组件不可创建时返回原始 I/O 错误。
///
/// # 副作用
/// 按当前进程 `umask` 创建缺失目录，不修改任何已存在目录的权限。
fn create_claude_temp_directory(path: &Path) -> io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true).mode(0o755);
    builder.create(path)
}

impl SandboxRuntime {
    /// 对应 Go：`SandboxRuntime.srtBinary` 的只读访问。
    ///
    /// 返回实际调用的 SRT executable 名称或路径，不触发 PATH 查询或日志。
    pub fn srt_binary(&self) -> &str {
        &self.srt_binary
    }

    /// 对应 Go：`SandboxRuntime.configPath` 的只读访问。
    ///
    /// 返回当前 settings JSON 路径；成功清理 owned 配置后该路径为空。
    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// 判断配置是否由调用方提供；自定义配置永远不由 runtime 删除。
    pub fn uses_custom_config(&self) -> bool {
        self.custom_config
    }

    /// 对应 Go：`internal/sandbox/runtime.go` 中的 `(*SandboxRuntime).WrapCommand`。
    ///
    /// 把原始命令转换为 `srt --settings <path> -- <command> <args...>`，并记录原始及
    /// 包装后的参数数组。
    ///
    /// # 参数
    /// - `command`：未经 shell 解析的原始 executable。
    /// - `args`：原始 argv，不包含 executable；本函数只克隆、不修改调用方切片。
    ///
    /// # 返回
    /// 返回 SRT executable 和拥有所有权的包装参数。
    ///
    /// # 副作用
    /// 产生一条 DEBUG tracing 事件。
    pub fn wrap_command(&self, command: &str, args: &[String]) -> (String, Vec<String>) {
        let mut wrapped = Vec::with_capacity(args.len() + 4);
        wrapped.push("--settings".to_owned());
        wrapped.push(self.config_path.to_string_lossy().into_owned());
        wrapped.push("--".to_owned());
        wrapped.push(command.to_owned());
        wrapped.extend(args.iter().cloned());
        tracing::debug!("original_command" = ?(command), "original_args" = ?(args), "wrapped_command" = ?(self.srt_binary.clone()), "wrapped_args" = ?(wrapped.clone()), "{}", "wrapped command for sandbox");
        (self.srt_binary.clone(), wrapped)
    }

    /// 对应 Go：`internal/sandbox/runtime.go` 中的 `(*SandboxRuntime).Cleanup`。
    ///
    /// 清理由本 runtime 自动生成的配置；空路径幂等成功，自定义配置只记录跳过。
    /// 删除失败时先记录 Go 的内部 warning，再把原错误返回给 Hook defer，由后者记录
    /// `sandbox cleanup failed`，两条记录都属于兼容行为。
    ///
    /// # 返回
    /// 删除成功或无需删除时返回成功；文件系统删除失败时返回 `SandboxRuntimeError`。
    ///
    /// # 副作用
    /// 可能删除 settings 文件并清空内部路径，或写 DEBUG/WARN 日志。
    pub fn cleanup(&mut self) -> Result<(), SandboxRuntimeError> {
        if self.config_path.as_os_str().is_empty() {
            return Ok(());
        }
        if !self.owns_config_file {
            tracing::debug!("config_path" = ?(self.config_path.to_string_lossy()), "{}", "skipping cleanup of custom config");
            return Ok(());
        }
        tracing::debug!("config_path" = ?(self.config_path.to_string_lossy()), "{}", "cleaning up sandbox config");
        if let Err(error) = cleanup_config_file(&self.config_path) {
            tracing::warn!("config_path" = ?(self.config_path.to_string_lossy()), "error" = ?(error.to_string()), "{}", "failed to cleanup sandbox config");
            return Err(error.into());
        }
        self.config_path.clear();
        self.owns_config_file = false;
        Ok(())
    }
}

/// 对应 Go：`internal/sandbox/runtime.go` 中的 `NewSandboxRuntimeWithConfig`。
///
/// 初始化 SRT runtime，并选择自定义配置或生成默认配置。`_session_json` 在当前 Go
/// 实现中未使用，但仍保留参数以便逐函数对照；`working_dir` 非空时追加到
/// `allowWrite`。
///
/// # 参数
/// - `_session_json`：原始 session JSON；当前基线不解析该参数。
/// - `custom_config_path`：调用方拥有的 settings 路径；存在时只验证、不删除。
/// - `working_dir`：自动配置额外允许写入的工作目录。
///
/// # 返回
/// 成功时返回可包装命令的 runtime；自定义配置无效或自动配置写入失败时返回错误。
///
/// # 副作用
/// 尝试以 0755 创建 `/tmp/claude`，可能创建 0600 临时 settings 文件并写日志。
pub fn new_sandbox_runtime_with_config(
    _session_json: &[u8],
    custom_config_path: Option<&Path>,
    working_dir: Option<&Path>,
) -> Result<SandboxRuntime, SandboxRuntimeError> {
    if let Err(error) = create_claude_temp_directory(Path::new("/tmp/claude")) {
        tracing::warn!("path" = ?("/tmp/claude"), "error" = ?(error.to_string()), "{}", "failed to create claude tmp directory");
    }
    let srt_binary = std::env::var("SRT_BINARY_PATH")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "srt".to_owned());

    let (config_path, owns_config_file, custom_config) = if let Some(path) = custom_config_path {
        let config = load_and_validate_config(path).map_err(SandboxRuntimeError::CustomConfig)?;
        tracing::info!("config_path" = ?(path.to_string_lossy()), "allowed_domains" = ?(config.network.allowed_domains), "{}", "using custom sandbox config");
        (path.to_owned(), false, true)
    } else {
        let mut config = default_config();
        if let Some(working_dir) = working_dir
            && !working_dir.as_os_str().is_empty()
        {
            config
                .filesystem
                .allow_write
                .push(working_dir.to_string_lossy().into_owned());
        }
        let path = write_config_file(&config).map_err(SandboxRuntimeError::GeneratedConfig)?;
        (path, true, false)
    };

    tracing::info!("srt_binary" = ?(srt_binary.clone()), "config_path" = ?(config_path.to_string_lossy()), "custom_config" = ?(custom_config), "{}", "sandbox runtime initialized");
    Ok(SandboxRuntime {
        srt_binary,
        config_path,

        owns_config_file,
        custom_config,
    })
}

/// 构造与 Go runtime.go 完全相同的默认网络与文件系统规则。
///
/// 除 runtime 自动配置外，`print-sandbox-settings` 也调用此函数，保证展示模板与
/// 实际默认隔离规则来自同一份定义，不会在两个命令路径中漂移。
#[must_use]
pub fn default_config() -> SrtConfig {
    SrtConfig {
        network: NetworkConfig {
            allowed_domains: vec![
                "api.anthropic.com".to_owned(),
                "api-staging.anthropic.com".to_owned(),
                "*.anthropic.com".to_owned(),
            ],
            denied_domains: None,
        },
        filesystem: FilesystemConfig {
            deny_read: vec![
                "~/.ssh".to_owned(),
                "~/.aws".to_owned(),
                "~/.config/gcloud".to_owned(),
                "/etc/shadow".to_owned(),
                "/etc/passwd-".to_owned(),
                "/secrets".to_owned(),
            ],
            allow_write: vec![
                "/tmp".to_owned(),
                "/tmp/claude".to_owned(),
                "~".to_owned(),
                "/workspace".to_owned(),
            ],
            deny_write: None,
            allow_git_config: true,
        },
        enable_weaker_nested_sandbox: false,
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use tempfile::TempDir;

    use super::*;

    /// 验证自动配置包含工作目录、正确包装命令并可显式删除。
    #[test]
    fn generated_config_wraps_and_cleans_up() {
        let mut runtime = new_sandbox_runtime_with_config(
            br#"{"session":"ignored"}"#,
            None,
            Some(Path::new("/workspace/project")),
        )
        .unwrap();
        assert!(runtime.owns_config_file);
        assert!(!runtime.custom_config);
        let (command, args) = runtime.wrap_command("claude", &["--print".to_owned()]);
        assert_eq!(command, "srt");
        assert_eq!(args[0], "--settings");
        assert_eq!(&args[2..], ["--", "claude", "--print"]);
        let config = load_and_validate_config(runtime.config_path()).unwrap();
        assert!(
            config
                .filesystem
                .allow_write
                .contains(&"/workspace/project".to_owned())
        );
        assert!(
            config
                .filesystem
                .allow_write
                .contains(&"/tmp/claude".to_owned())
        );
        let path = runtime.config_path.clone();
        runtime.cleanup().unwrap();
        assert!(!path.exists());
        assert!(runtime.config_path.as_os_str().is_empty());
    }

    /// 验证自定义配置只被引用，不会在 cleanup 或 Drop 中删除。
    #[test]
    fn custom_config_is_never_deleted() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("custom.json");
        std::fs::write(&path, serde_json::to_vec(&default_config()).unwrap()).unwrap();
        {
            let mut runtime = new_sandbox_runtime_with_config(&[], Some(&path), None).unwrap();
            assert!(runtime.custom_config);
            runtime.cleanup().unwrap();
        }
        assert!(path.exists());
    }

    /// 验证非法自定义域名配置被初始化阶段拒绝。
    #[test]
    fn rejects_invalid_custom_config() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("invalid.json");
        std::fs::write(&path, br#"{"network":{"allowedDomains":["example.com"]}}"#).unwrap();
        assert!(matches!(
            new_sandbox_runtime_with_config(&[], Some(&path), None),
            Err(SandboxRuntimeError::CustomConfig(_))
        ));
    }

    /// 供父测试在独立进程、`umask 000` 环境中执行目录创建。
    ///
    /// 普通全量测试未设置探针环境变量时立即返回；独立子进程会验证生产 helper
    /// 的最终 mode，避免在并行测试进程内修改全局 umask。
    #[test]
    fn claude_temp_directory_mode_probe() {
        let Some(path) = std::env::var_os("ENV_MANAGER_CLAUDE_TEMP_MODE_PROBE") else {
            return;
        };
        let path = PathBuf::from(path);
        create_claude_temp_directory(&path).unwrap();
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }

    /// 验证 `umask 000` 时仍以 Go 指定的 0755 为权限上限。
    ///
    /// 通过 shell 只修改子进程 umask，避免并行 Rust 测试共享的进程级 umask 产生
    /// 数据竞争；子进程只运行上面的精确探针测试。
    #[test]
    fn claude_temp_directory_caps_mode_with_zero_umask() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("claude");
        let test_binary = std::env::current_exe().unwrap();
        let output = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("umask 000; exec \"$TEST_BINARY\" --exact internal::sandbox::runtime::tests::claude_temp_directory_mode_probe --nocapture")
            .env("TEST_BINARY", test_binary)
            .env("ENV_MANAGER_CLAUDE_TEMP_MODE_PROBE", &path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "umask 子进程失败：stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}
