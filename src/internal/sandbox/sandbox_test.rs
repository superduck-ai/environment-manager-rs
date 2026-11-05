//! Sandbox 聚合行为测试。
//!
//! 测试覆盖 JSON/权限/清理、域名校验、自动与自定义配置的所有权、命令包装及
//! `current` 安装短路；所有路径副作用都限制在 Linux ARM64 测试机临时目录。

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::{
    ConfigError, FilesystemConfig, NetworkConfig, SrtConfig, cleanup_config_file, default_config,
    install_sandbox_runtime, load_and_validate_config, new_sandbox_runtime_with_config,
    validate_config, write_config_file,
};
/// 构造满足 Anthropic 必需域名约束的最小配置。
fn valid_sandbox_config() -> SrtConfig {
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

///
/// 验证配置往返、0600 权限、显式删除以及空路径清理的幂等语义。
#[test]
fn test_write_load_validate_and_cleanup_config_file() {
    let config = valid_sandbox_config();
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

///
/// 覆盖精确域名、双向 `*.` 通配、无关域名以及缺失必需域名的错误分支。
#[test]
fn test_validate_config_and_domain_matches() {
    use super::config::domain_matches;

    assert!(validate_config(&valid_sandbox_config()).is_ok());
    assert!(domain_matches("*.anthropic.com", "api.anthropic.com"));
    assert!(domain_matches("api.anthropic.com", "api.anthropic.com"));
    assert!(domain_matches("api.anthropic.com", "*.anthropic.com"));
    assert!(!domain_matches("example.com", "api.anthropic.com"));

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

///
/// 分别构造缺失文件、损坏 JSON 和缺失必需域名，确保三类错误不会被错误合并。
#[test]
fn test_load_and_validate_config_reports_read_parse_and_validation_errors() {
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

///
/// 自动配置必须加入工作目录、包装 SRT 参数，并在显式 cleanup 后删除 owned 文件。
#[test]
fn test_new_sandbox_runtime_with_generated_config_wrap_and_cleanup() {
    let mut runtime = new_sandbox_runtime_with_config(
        br#"{"session":"ignored"}"#,
        None,
        Some(Path::new("/workspace/project")),
    )
    .unwrap();
    let (command, args) = runtime.wrap_command("claude", &["--print".to_owned()]);
    assert_eq!(command, "srt");
    assert_eq!(&args[2..], ["--", "claude", "--print"]);
    let config_path = runtime.config_path().to_owned();
    let config = load_and_validate_config(&config_path).unwrap();
    assert!(contains(
        &config.filesystem.allow_write,
        "/workspace/project"
    ));
    assert!(config.filesystem.allow_git_config);
    runtime.cleanup().unwrap();
    assert!(!config_path.exists());
}

///
/// 调用方提供的 settings 文件只被引用，即使显式 cleanup 或 Drop 也必须保留。
#[test]
fn test_new_sandbox_runtime_with_custom_config_does_not_cleanup_file() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("custom-srt.json");
    write_sandbox_config(&path, &valid_sandbox_config());
    {
        let mut runtime = new_sandbox_runtime_with_config(&[], Some(&path), None).unwrap();
        assert!(runtime.uses_custom_config());
        let (command, args) = runtime.wrap_command("claude", &[]);
        assert_eq!(command, "srt");
        assert_eq!(&args[0..3], ["--settings", path.to_str().unwrap(), "--"]);
        runtime.cleanup().unwrap();
    }
    assert!(path.exists());
}

///
/// 自定义配置在创建任何 runtime 副作用前完成读取和安全验证。
#[test]
fn test_new_sandbox_runtime_rejects_invalid_custom_config() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("invalid-srt.json");
    std::fs::write(&path, br#"{"network":{"allowedDomains":["example.com"]}}"#).unwrap();
    let error = new_sandbox_runtime_with_config(&[], Some(&path), None)
        .err()
        .expect("非法自定义配置必须失败");
    assert!(
        error
            .to_string()
            .contains("failed to load custom sandbox config")
    );
}

///
/// `current` 是明确的禁用安装值，不得查询或启动 npm，因而预取消 token 也应成功。
#[tokio::test]
async fn test_install_sandbox_runtime_skips_current_version() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    install_sandbox_runtime(&cancellation, "current")
        .await
        .unwrap();
    assert_eq!(default_config().network.allowed_domains.len(), 3);
}

/// 以生产 JSON 形状写入自定义 SRT fixture。
fn write_sandbox_config(path: &Path, config: &SrtConfig) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, serde_json::to_vec(config).unwrap()).unwrap();
}

/// 按精确字符串判断 allow/deny 列表成员。
fn contains(values: &[String], target: &str) -> bool {
    values.iter().any(|value| value == target)
}
