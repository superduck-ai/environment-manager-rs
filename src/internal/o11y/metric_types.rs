//! 指标标签 provider、错误分类与覆盖式合并。

use std::error::Error;

use crate::internal::process::ProcessExitError;

use super::{MetricTags, TagProvider};

/// 简单 key/value 标签 provider。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvTagProvider {
    tags: MetricTags,
}

impl KvTagProvider {
    /// 从完整标签 map 构造 provider。
    pub fn new(tags: MetricTags) -> Self {
        Self { tags }
    }

    /// 从单个标签构造 provider，减少常见调用的样板代码。
    pub fn one(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            tags: [(key.into(), value.into())].into_iter().collect(),
        }
    }
}

impl TagProvider for KvTagProvider {
    /// 返回标签快照，避免调用方随后修改 provider 内部状态。
    fn tags(&self) -> MetricTags {
        self.tags.clone()
    }
}

/// 把错误链转换为稳定的 `error` 与 `error_code` 标签。
///
/// 成功返回空 map；遇到 `ProcessExitError::ExitCode` 时保留真实退出码，其他错误使用
/// `unknown`。逐层扫描 source chain 因此包装错误不会丢失退出码。
pub fn error_tags(error: Option<&(dyn Error + Send + Sync + 'static)>) -> KvTagProvider {
    let Some(error) = error else {
        return KvTagProvider::default();
    };

    let mut current: Option<&(dyn Error + 'static)> = Some(error);
    while let Some(candidate) = current {
        if let Some(ProcessExitError::ExitCode(exit_code)) =
            candidate.downcast_ref::<ProcessExitError>()
        {
            return KvTagProvider::new(
                [
                    ("error".to_owned(), "true".to_owned()),
                    ("error_code".to_owned(), exit_code.to_string()),
                ]
                .into_iter()
                .collect(),
            );
        }
        current = candidate.source();
    }

    KvTagProvider::new(
        [
            ("error".to_owned(), "true".to_owned()),
            ("error_code".to_owned(), "unknown".to_owned()),
        ]
        .into_iter()
        .collect(),
    )
}

/// 按 provider 顺序合并标签，后出现的 provider 覆盖同名 key。
pub fn merge_tags(providers: &[&dyn TagProvider]) -> MetricTags {
    let mut merged = MetricTags::new();
    for provider in providers {
        merged.extend(provider.tags());
    }
    merged
}
