//! Managed Agents Git 资源的输入边界；旧版 source 合同不经过这些规则。

use std::path::{Path, PathBuf};

use super::{ConfigError, GitCheckout, GitRepositorySource, Source};

/// 一次校验全部资源，保证路径冲突在任何 clone 副作用前失败。
pub(crate) fn validate_resources(sources: &[Source]) -> Result<(), ConfigError> {
    let mut paths = Vec::<PathBuf>::new();
    for Source::GitRepository(source) in sources {
        let path = validate_resource(source)?;
        if paths
            .iter()
            .any(|previous| path.starts_with(previous) || previous.starts_with(&path))
        {
            return Err(invalid("repository mount paths overlap"));
        }
        paths.push(path);
    }
    Ok(())
}

/// 校验公开 URL、checkout 和 /workspace 内的显式目标目录。
pub(crate) fn validate_resource(source: &GitRepositorySource) -> Result<PathBuf, ConfigError> {
    let mount_path = source
        .mount_path
        .as_deref()
        .ok_or_else(|| invalid("mount_path is required"))?;
    let path = validate_mount_path(mount_path, Path::new("/workspace"))?;
    if source.git_info.auth.is_some() || source.git_info.git_ref.is_some() {
        return Err(invalid(
            "resource sources use checkout and server-side credentials",
        ));
    }
    let url = source
        .git_info
        .url
        .as_deref()
        .ok_or_else(|| invalid("url is required"))?;
    let url = url::Url::parse(url).map_err(|_| invalid("invalid repository URL"))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "repository URL must be HTTPS without credentials, query or fragment",
        ));
    }
    match &source.checkout {
        Some(GitCheckout::Commit { sha })
            if !(7..=64).contains(&sha.len())
                || !sha.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            return Err(invalid(
                "commit sha must contain 7 to 64 hexadecimal characters",
            ));
        }
        Some(GitCheckout::Branch { name })
            if name.is_empty()
                || name.chars().count() > 255
                || name.starts_with('-')
                || name.chars().any(char::is_control) =>
        {
            return Err(invalid("invalid branch name"));
        }
        _ => {}
    }
    Ok(path)
}

/// 拒绝路径归一化产生歧义的输入；运行时另行拒绝既有 symlink。
pub(crate) fn validate_mount_path(value: &str, root: &Path) -> Result<PathBuf, ConfigError> {
    let path = PathBuf::from(value);
    if !path.is_absolute()
        || path == root
        || !path.starts_with(root)
        || value.chars().any(char::is_control)
        || value
            .split('/')
            .skip(1)
            .any(|part| matches!(part, "" | "." | ".." | ".git" | ".claude" | ".oma"))
    {
        return Err(invalid(
            "mount_path must be a normalized directory below /workspace",
        ));
    }
    Ok(path)
}

/// 把资源边界错误映射到配置验证结果。
fn invalid(message: &str) -> ConfigError {
    ConfigError::InvalidGitResource(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 绝对路径越界、遍历和归一化歧义不能进入 clone 阶段。
    #[test]
    fn rejects_unsafe_mount_paths() {
        for path in [
            "/workspace",
            "/workspaces/repo",
            "/tmp/repo",
            "repo",
            "/workspace/../repo",
            "/workspace/./repo",
            "/workspace//repo",
            "/workspace/repo/",
            "/workspace/.git",
            "/workspace/repo\n",
        ] {
            assert!(
                validate_mount_path(path, Path::new("/workspace")).is_err(),
                "{path}"
            );
        }
        assert!(validate_mount_path("/workspace/team/repo", Path::new("/workspace")).is_ok());
    }

    /// 缺省或旧来源不能启用绝对 mount_path，也不能带入新 checkout 语义。
    #[test]
    fn legacy_origins_reject_resource_only_fields() {
        for origin in ["", "legacy", "cloud"] {
            for fields in [
                serde_json::json!({"mount_path": "/"}),
                serde_json::json!({"checkout": {"type": "branch", "name": "main"}}),
            ] {
                let mut source = serde_json::json!({"type": "git_repository", "git_info": {"repo": "owner/repo", "host": "github"}});
                source
                    .as_object_mut()
                    .unwrap()
                    .extend(fields.as_object().unwrap().clone());
                let context: super::super::StartupContext = serde_json::from_value(serde_json::json!({
                    "origin": origin, "api_base_url": "https://oma.example.test", "sources": [source],
                })).unwrap();
                assert!(context.validate().is_err(), "{origin:?}");
            }
        }
    }

    /// 冲突资源必须在第一份仓库写入之前被整体拒绝。
    #[test]
    fn rejects_overlapping_resources_and_raw_credentials() {
        let resource = |mount: &str| {
            serde_json::from_value::<Source>(serde_json::json!({
            "type": "git_repository", "mount_path": mount,
            "git_info": {"repo": "owner/repo", "host": "github", "url": "https://github.com/owner/repo"}
        })).unwrap()
        };
        for second in ["/workspace/repo", "/workspace/repo/child", "/workspace"] {
            assert!(validate_resources(&[resource("/workspace/repo"), resource(second)]).is_err());
        }
        assert!(
            validate_resources(&[resource("/workspace/repo"), resource("/workspace/repo2")])
                .is_ok()
        );
        let Source::GitRepository(mut source) = resource("/workspace/repo");
        source.git_info.url = Some("https://user:secret@github.com/owner/repo".to_owned());
        assert!(validate_resource(&source).is_err());
        source.git_info.url = Some("https://github.com/owner/repo".to_owned());
        source.checkout = Some(GitCheckout::Commit {
            sha: "main".to_owned(),
        });
        assert!(validate_resource(&source).is_err());
    }
}
