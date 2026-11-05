//! SRT JSON 配置模型、验证和临时文件生命周期。
//!
//! 字段通过 serde 精确映射 camelCase JSON。临时配置使用 `tempfile`
//! 原子创建为 0600，cleanup 只允许删除系统临时目录内、且非隐藏路径的文件。

use std::{
    io::{self, Write},
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// sandbox-runtime 顶层配置。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SrtConfig {
    /// 网络访问白名单与黑名单。
    #[serde(default)]
    pub network: NetworkConfig,
    /// 文件系统读写约束。
    #[serde(default)]
    pub filesystem: FilesystemConfig,
    /// 是否允许较弱的嵌套 sandbox。
    pub enable_weaker_nested_sandbox: bool,
}

/// SRT 网络访问规则。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkConfig {
    /// 允许访问的精确域名或 `*.` 通配域名。
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    /// 明确拒绝的域名；`None` 保留 null 数组 编码为 JSON null 的区别。
    pub denied_domains: Option<Vec<String>>,
}

/// SRT 文件系统访问规则。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct FilesystemConfig {
    /// 禁止读取的路径。
    #[serde(default)]
    pub deny_read: Vec<String>,
    /// 允许写入的路径。
    #[serde(default)]
    pub allow_write: Vec<String>,
    /// 明确禁止写入的路径；`None` 保留 null 数组 编码为 JSON null 的区别。
    pub deny_write: Option<Vec<String>>,
    /// 是否允许更新用户 git 配置；false 时与 JSON 空值省略 一样省略。
    #[serde(skip_serializing_if = "is_false")]
    pub allow_git_config: bool,
}

/// serde 辅助判断：仅当布尔值为 false 时省略 `allowGitConfig`。
fn is_false(value: &bool) -> bool {
    !*value
}

/// 配置编码、文件操作或验证错误。
#[derive(Debug, Error)]
pub enum ConfigError {
    /// 无法编码为 JSON。
    #[error("failed to serialize sandbox config: {0}")]
    Serialize(#[source] serde_json::Error),
    /// 无法创建安全临时文件。
    #[error("failed to create temp config file: {0}")]
    Create(#[source] io::Error),
    /// 无法完整写入临时配置。
    #[error("failed to write sandbox config: {0}")]
    Write(#[source] io::Error),
    /// 无法把 tempfile 转为由 runtime 管理的持久临时路径。
    #[error("failed to persist sandbox config: {0}")]
    Persist(#[source] io::Error),
    /// cleanup 删除符合条件的临时文件失败。
    #[error("failed to remove config file: {source}")]
    Remove {
        /// 尝试删除的路径。
        path: PathBuf,
        /// 原始文件系统错误。
        #[source]
        source: io::Error,
    },
    /// 配置路径无法读取。
    #[error("failed to read config file {path}: {source}")]
    Read {
        /// 配置路径。
        path: PathBuf,
        /// 原始文件系统错误。
        #[source]
        source: io::Error,
    },
    /// 文件内容不是有效 SRT JSON。
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        /// 配置路径。
        path: PathBuf,
        /// JSON 解析错误。
        #[source]
        source: serde_json::Error,
    },
    /// 顶层配置引用为空；Rust API 通常不会产生，只用于可选验证入口。
    #[error("config is missing")]
    MissingConfig,
    /// 必需 Anthropic 域名没有被允许。
    #[error("required domain {required:?} not found in allowed domains (have: {allowed:?})")]
    MissingRequiredDomain {
        /// 缺失的必需模式。
        required: &'static str,
        /// 实际允许列表。
        allowed: Vec<String>,
    },
    /// 外层调用需要明确指出验证阶段失败。
    #[error("config validation failed: {0}")]
    Validation(Box<ConfigError>),
}

/// 把配置以两空格缩进 JSON 写入安全临时文件，并返回其 owned 路径。
///
/// `NamedTempFile` 在写入失败时自动删除；成功后 `keep` 关闭 RAII 删除责任，
/// 交给 [`cleanup_config_file`] 或 [`super::SandboxRuntime`] 管理。
pub fn write_config_file(config: &SrtConfig) -> Result<PathBuf, ConfigError> {
    let data = serde_json::to_vec_pretty(config).map_err(ConfigError::Serialize)?;
    let mut file = tempfile::Builder::new()
        .prefix("srt-config-")
        .suffix(".json")
        .tempfile()
        .map_err(ConfigError::Create)?;
    file.write_all(&data).map_err(ConfigError::Write)?;
    file.flush().map_err(ConfigError::Write)?;
    let (file, path) = file
        .keep()
        .map_err(|error| ConfigError::Persist(error.error))?;
    drop(file);
    Ok(path)
}

/// 仅当路径安全地位于系统临时目录下时删除配置文件。
///
/// 空路径、无法绝对化的路径、临时目录本身、目录外路径以及以 `.` 开头的相对路径
/// 都被静默忽略，以避免清理调用误删工作目录。实际删除失败会保留
/// `failed to remove config file: ...` 外层前缀和底层 I/O 错误。
pub fn cleanup_config_file(path: impl AsRef<Path>) -> Result<(), ConfigError> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    let Ok(absolute) = lexical_absolute(path) else {
        return Ok(());
    };
    let Ok(temp_dir) = lexical_absolute(&std::env::temp_dir()) else {
        return Ok(());
    };
    let Ok(relative) = absolute.strip_prefix(&temp_dir) else {
        return Ok(());
    };
    let relative_text = relative.as_os_str().to_string_lossy();
    if relative_text.is_empty() || relative_text.starts_with('.') {
        return Ok(());
    }

    remove_path(path).map_err(|source| ConfigError::Remove {
        path: path.to_owned(),
        source,
    })
}

/// 删除 Linux 上的普通文件、symlink 或空目录。
///
/// 在 Unix 上先调用 unlink；仅当内核以 EISDIR 拒绝目录时再调用 rmdir。保持这个
/// 顺序可确保指向目录的 symlink 被当作链接本身删除，不会跟随到目标目录；同时允许
/// cleanup 删除意外传入的空目录。非空目录和权限错误返回最终系统错误。
///
/// # 参数
/// - `path`：通过临时目录边界检查后的原始路径，保留 symlink 与 `..` 的内核语义。
///
/// # 返回
/// 文件、链接或空目录删除成功时返回 `Ok(())`；其他情况返回对应 I/O 错误。
fn remove_path(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::IsADirectory => std::fs::remove_dir(path),
        Err(error) => Err(error),
    }
}

/// 在 Linux 上执行“转绝对路径 + 词法路径归一化”。
///
/// `std::path::absolute` 会保留 `..`，不能直接用于 cleanup 的目录边界判断；否则
/// `/tmp/guard/../../home/file` 会先通过 `/tmp` 字面前缀检查，再由内核解析到目录外。
/// 本函数只做词法折叠，不调用 `canonicalize`，因此不会解析父目录中的 symlink。
///
/// # 参数
/// - `path`：待检查路径；相对路径以调用时工作目录为基准。
///
/// # 返回
/// 返回折叠 `.`、`..` 后的绝对 Linux 路径；仅在读取当前工作目录失败时返回错误。
fn lexical_absolute(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut cleaned = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => cleaned.push(prefix.as_os_str()),
            Component::RootDir => cleaned.push(Path::new("/")),
            Component::CurDir => {}
            Component::ParentDir => {
                // 对根目录调用 pop 会保持根不变，将多余的前导 `..` 截断在 `/`，
                // 而不是产生相对路径。
                let _at_root = cleaned.pop();
            }
            Component::Normal(name) => cleaned.push(name),
        }
    }
    if cleaned.as_os_str().is_empty() {
        cleaned.push(Path::new("/"));
    }
    Ok(cleaned)
}

/// 判断 allowed pattern 是否满足 required domain/pattern。
///
/// 仅支持 使用的 `*.` 前缀通配；suffix 自带前导点，因此
/// `evil-anthropic.com` 不会误匹配 `*.anthropic.com`。
pub(super) fn domain_matches(pattern: &str, required: &str) -> bool {
    if pattern == required {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix("*.")
        && required.ends_with(&format!(".{suffix}"))
    {
        return true;
    }
    if let Some(suffix) = required.strip_prefix("*.")
        && pattern.ends_with(&format!(".{suffix}"))
    {
        return true;
    }
    false
}

/// 验证配置允许调用 Anthropic API 所需的域名。
pub fn validate_config(config: &SrtConfig) -> Result<(), ConfigError> {
    const REQUIRED_DOMAINS: [&str; 2] = ["api.anthropic.com", "*.anthropic.com"];
    for required in REQUIRED_DOMAINS {
        if !config
            .network
            .allowed_domains
            .iter()
            .any(|allowed| domain_matches(allowed, required))
        {
            return Err(ConfigError::MissingRequiredDomain {
                required,
                allowed: config.network.allowed_domains.clone(),
            });
        }
    }
    Ok(())
}

/// 读取 JSON 文件、解析为类型化配置并执行 Anthropic 域名验证。
pub fn load_and_validate_config(path: impl AsRef<Path>) -> Result<SrtConfig, ConfigError> {
    let path = path.as_ref().to_owned();
    let data = std::fs::read(&path).map_err(|source| ConfigError::Read {
        path: path.clone(),
        source,
    })?;
    let config =
        serde_json::from_slice::<SrtConfig>(&data).map_err(|source| ConfigError::Parse {
            path: path.clone(),
            source,
        })?;
    validate_config(&config).map_err(|error| ConfigError::Validation(Box::new(error)))?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;

    use super::*;

    /// 构造满足必需域名验证的最小配置。
    fn valid_config() -> SrtConfig {
        SrtConfig {
            network: NetworkConfig {
                allowed_domains: vec!["*.anthropic.com".to_owned()],
                denied_domains: None,
            },
            filesystem: FilesystemConfig {
                deny_read: vec!["~/.ssh".to_owned()],
                allow_write: vec!["/tmp".to_owned()],
                deny_write: None,
                allow_git_config: true,
            },
            enable_weaker_nested_sandbox: false,
        }
    }

    /// 验证临时配置可往返解析、权限为 0600 且 cleanup 删除文件。
    #[test]
    fn write_load_validate_and_cleanup() {
        let config = valid_config();
        let path = write_config_file(&config).unwrap();
        #[cfg(unix)]
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(load_and_validate_config(&path).unwrap(), config);
        cleanup_config_file(&path).unwrap();
        assert!(!path.exists());
        cleanup_config_file(Path::new("")).unwrap();
    }

    /// 验证 sandbox writer 生成可往返的两空格缩进 JSON。
    #[test]
    fn serializes_indented_json() {
        let config = SrtConfig {
            network: NetworkConfig {
                allowed_domains: vec![
                    "api.<>&.example".to_owned(),
                    "line\u{2028}sep\u{2029}end".to_owned(),
                ],
                denied_domains: None,
            },
            filesystem: FilesystemConfig {
                deny_read: vec!["/a".to_owned(), "/b".to_owned()],
                allow_write: vec!["/tmp".to_owned()],
                deny_write: None,
                allow_git_config: false,
            },
            enable_weaker_nested_sandbox: false,
        };
        let expected = concat!(
            "{\n",
            "  \"network\": {\n",
            "    \"allowedDomains\": [\n",
            "      \"api.<>&.example\",\n",
            "      \"line sep end\"\n",
            "    ],\n",
            "    \"deniedDomains\": null\n",
            "  },\n",
            "  \"filesystem\": {\n",
            "    \"denyRead\": [\n",
            "      \"/a\",\n",
            "      \"/b\"\n",
            "    ],\n",
            "    \"allowWrite\": [\n",
            "      \"/tmp\"\n",
            "    ],\n",
            "    \"denyWrite\": null\n",
            "  },\n",
            "  \"enableWeakerNestedSandbox\": false\n",
            "}"
        );
        let encoded = serde_json::to_vec_pretty(&config).unwrap();
        assert_eq!(encoded, expected.as_bytes());
        assert!(!encoded.ends_with(b"\n"));
    }

    /// 验证缺字段采用默认值，未知顶层或嵌套字段被忽略。
    #[test]
    fn missing_and_unknown_fields_follow_json_schema() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("unknown.json");
        std::fs::write(
            &path,
            r#"{"network":{"allowedDomains":["*.anthropic.com"],"unknownNested":true},"unknown":123}"#,
        )
        .unwrap();
        let config = load_and_validate_config(path).unwrap();
        assert_eq!(config.network.allowed_domains, ["*.anthropic.com"]);
        assert!(config.network.denied_domains.is_none());
        assert!(config.filesystem.deny_read.is_empty());
        assert!(config.filesystem.allow_write.is_empty());
        assert!(config.filesystem.deny_write.is_none());
        assert!(!config.filesystem.allow_git_config);
    }

    /// 验证精确域名、双向 wildcard 和无关域名的匹配行为。
    #[test]
    fn validates_domains() {
        assert!(domain_matches("*.anthropic.com", "api.anthropic.com"));
        assert!(domain_matches("api.anthropic.com", "api.anthropic.com"));
        assert!(domain_matches("api.anthropic.com", "*.anthropic.com"));
        assert!(!domain_matches("example.com", "api.anthropic.com"));
        assert!(validate_config(&valid_config()).is_ok());
        let invalid = SrtConfig {
            network: NetworkConfig {
                allowed_domains: vec!["example.com".to_owned()],
                denied_domains: None,
            },
            ..SrtConfig::default()
        };
        assert!(matches!(
            validate_config(&invalid),
            Err(ConfigError::MissingRequiredDomain { .. })
        ));
    }

    /// 验证读取、解析和验证错误保持可区分。
    #[test]
    fn reports_read_parse_and_validation_errors() {
        let temp = TempDir::new().unwrap();
        assert!(matches!(
            load_and_validate_config(temp.path().join("missing.json")),
            Err(ConfigError::Read { .. })
        ));
        let bad = temp.path().join("bad.json");
        std::fs::write(&bad, b"{bad json").unwrap();
        assert!(matches!(
            load_and_validate_config(&bad),
            Err(ConfigError::Parse { .. })
        ));
        let invalid = temp.path().join("invalid.json");
        std::fs::write(
            &invalid,
            br#"{"network":{"allowedDomains":["example.com"]}}"#,
        )
        .unwrap();
        assert!(matches!(
            load_and_validate_config(&invalid),
            Err(ConfigError::Validation(_))
        ));
    }

    /// 验证 cleanup 不会删除系统临时目录之外的用户文件。
    #[test]
    fn cleanup_refuses_outside_temp_directory() {
        let temp = TempDir::new_in(std::env::current_dir().unwrap()).unwrap();
        let path = temp.path().join("custom.json");
        std::fs::write(&path, b"{}").unwrap();
        cleanup_config_file(&path).unwrap();
        assert!(path.exists());
    }

    /// 验证 cleanup 与 Linux 删除 一样可以删除临时目录内的空目录。
    #[test]
    fn cleanup_removes_empty_directory() {
        // cleanup 会像 一样拒绝相对名以 `.` 开头的路径；TempDir
        // 默认 `.tmp*` 前缀不适合此正向删除 fixture。
        let temp = tempfile::Builder::new()
            .prefix("srt-cleanup-test-")
            .tempdir_in(std::env::temp_dir())
            .unwrap();
        let directory = temp.path().join("empty-config");
        std::fs::create_dir(&directory).unwrap();
        cleanup_config_file(&directory).unwrap();
        assert!(!directory.exists());
    }

    /// 验证删除错误外层文案不重复注入 path，并保留稳定的错误上下文前缀。
    #[test]
    fn cleanup_remove_error_uses_stable_prefix() {
        let temp = tempfile::Builder::new()
            .prefix("srt-cleanup-test-")
            .tempdir_in(std::env::temp_dir())
            .unwrap();
        let directory = temp.path().join("non-empty-config");
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("child"), b"data").unwrap();
        let error = cleanup_config_file(&directory).unwrap_err();
        assert!(matches!(error, ConfigError::Remove { .. }));
        assert!(
            error
                .to_string()
                .starts_with("failed to remove config file: ")
        );
        assert!(!error.to_string().starts_with(&format!(
            "failed to remove config file {}:",
            directory.display()
        )));
    }

    /// 验证 cleanup 在检查目录边界前折叠 `..`，不能被 `/tmp` 字面前缀绕过。
    #[test]
    fn cleanup_refuses_parent_traversal_outside_temp_directory() {
        let current_directory = std::env::current_dir().unwrap();
        let sentinel = tempfile::Builder::new()
            .prefix("sandbox-cleanup-sentinel-")
            .tempfile_in(current_directory)
            .unwrap();
        let (sentinel_file, sentinel_path) = sentinel.keep().unwrap();
        drop(sentinel_file);

        let guard = TempDir::new().unwrap();
        let guard_path = lexical_absolute(guard.path()).unwrap();
        let mut disguised_path = guard_path.clone();
        // 从现有 guard 目录逐级返回根目录，再拼入 sentinel 的绝对组件。当前命令
        // 会因原始字符串仍以系统临时目录开头而误删 sentinel。
        for _ in guard_path
            .components()
            .filter(|component| matches!(component, Component::Normal(_)))
        {
            disguised_path.push("..");
        }
        for component in sentinel_path.components() {
            if let Component::Normal(name) = component {
                disguised_path.push(name);
            }
        }

        cleanup_config_file(&disguised_path).unwrap();
        assert!(sentinel_path.exists());
        std::fs::remove_file(sentinel_path).unwrap();
    }
}
