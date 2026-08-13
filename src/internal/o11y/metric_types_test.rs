//! 指标标签分类与覆盖合并回归测试。

use std::io;

use crate::internal::process::ProcessExitError;

use super::{KvTagProvider, TagProvider, error_tags, merge_tags};

/// 验证成功、普通错误和子进程退出错误的标签保持。
#[test]
fn test_error_tags() {
    assert!(error_tags(None).tags().is_empty());

    let generic = io::Error::other("boom");
    let generic_tags = error_tags(Some(&generic)).tags();
    assert_eq!(generic_tags.get("error").map(String::as_str), Some("true"));
    assert_eq!(
        generic_tags.get("error_code").map(String::as_str),
        Some("unknown")
    );

    let exit = ProcessExitError::ExitCode(7);
    let exit_tags = error_tags(Some(&exit)).tags();
    assert_eq!(exit_tags.get("error").map(String::as_str), Some("true"));
    assert_eq!(exit_tags.get("error_code").map(String::as_str), Some("7"));
}

/// 验证后出现的 provider 覆盖同名标签且其他标签全部保留。
#[test]
fn test_merge_tags() {
    let first = KvTagProvider::new(
        [
            ("shared".to_owned(), "first".to_owned()),
            ("only_first".to_owned(), "1".to_owned()),
        ]
        .into_iter()
        .collect(),
    );
    let second = KvTagProvider::new(
        [
            ("shared".to_owned(), "second".to_owned()),
            ("only_second".to_owned(), "2".to_owned()),
        ]
        .into_iter()
        .collect(),
    );

    let tags = merge_tags(&[&first, &second]);
    assert_eq!(tags.get("shared").map(String::as_str), Some("second"));
    assert_eq!(tags.get("only_first").map(String::as_str), Some("1"));
    assert_eq!(tags.get("only_second").map(String::as_str), Some("2"));
}
