//! `provision-packages` v1 stdin manifest 的严格 schema 与安全校验。
//!
//! Serde derive 负责精确字段名、重复键、未知字段与类型约束；package spec 只作为
//! argv 传递，仍拒绝 option 前缀和 URL authority 凭据，防止参数注入与 secret 泄露。

use serde::Deserialize;

/// stdin manifest 的最大字节数，与 `1 << 20` 完全一致。
pub(super) const MAX_MANIFEST_BYTES: usize = 1 << 20;

/// 顶层版本化 package manifest。
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Manifest {
    /// schema 版本，当前只接受 `1`。
    pub version: i32,
    /// package 配置对象；缺失和显式 `null` 均由语义校验拒绝。
    pub packages: Option<PackageManifest>,
}

/// 按 manager 分组的原生 package spec。
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct PackageManifest {
    /// discriminator，当前必须是 `packages`。
    #[serde(rename = "type")]
    pub package_type: String,
    /// APT package spec。
    pub apt: Vec<String>,
    /// Cargo package spec。
    pub cargo: Vec<String>,
    /// Ruby Gem package spec。
    pub gem: Vec<String>,
    /// module package spec。
    pub go: Vec<String>,
    /// NPM package spec。
    pub npm: Vec<String>,
    /// PIP package spec。
    pub pip: Vec<String>,
}

impl PackageManifest {
    /// 按固定顺序返回六个 manager 列表，供校验和计数共享。
    #[must_use]
    pub(super) fn lists(&self) -> [&Vec<String>; 6] {
        [
            &self.apt,
            &self.cargo,
            &self.gem,
            &self.go,
            &self.npm,
            &self.pip,
        ]
    }
}

/// 执行版本、discriminator 和 spec 安全校验。
///
/// # 返回
/// 任一字段违反 v1 schema 时返回不含用户输入的静态错误；错误正文不会进入协议输出。
pub(super) fn validate_manifest(manifest: &Manifest) -> Result<(), &'static str> {
    if manifest.version != 1 {
        return Err("unsupported manifest version");
    }
    let packages = manifest
        .packages
        .as_ref()
        .ok_or("packages must be an object")?;
    if packages.package_type != "packages" {
        return Err("packages type must be packages");
    }
    for specs in packages.lists() {
        if specs.iter().any(|spec| is_package_manager_option(spec)) {
            return Err("package spec must not be an option");
        }
        if specs.iter().any(|spec| contains_url_credentials(spec)) {
            return Err("package spec must not contain URL credentials");
        }
    }
    Ok(())
}

/// 拒绝去除前导空白后以 `-` 开头的 spec。
#[must_use]
fn is_package_manager_option(spec: &str) -> bool {
    spec.trim_start().starts_with('-')
}

/// 判断任意 URL authority 中是否出现 `@`。
///
/// 这里只实现固定模式 `[a-z][a-z0-9+.-]*://[^/\s?#]*@` 的状态机，避免为单个安全
/// 检查增加 regex 依赖。Kelvin sign 和 long s 作为 ASCII scheme 字母的视觉混淆字符
/// 一并拒绝；percent-encoding 是否有效不影响凭据判定。
#[must_use]
fn contains_url_credentials(spec: &str) -> bool {
    for (start, first) in spec.char_indices() {
        if !is_scheme_letter(first) {
            continue;
        }
        let mut cursor = start + first.len_utf8();
        while let Some(character) = spec[cursor..].chars().next() {
            if !(is_scheme_letter(character)
                || character.is_ascii_digit()
                || matches!(character, '+' | '.' | '-'))
            {
                break;
            }
            cursor += character.len_utf8();
        }
        if !spec[cursor..].starts_with("://") {
            continue;
        }
        cursor += 3;
        while let Some(character) = spec[cursor..].chars().next() {
            if character == '@' {
                return true;
            }
            if matches!(character, '/' | '?' | '#') || is_pattern_whitespace(character) {
                break;
            }
            cursor += character.len_utf8();
        }
    }
    false
}

/// 判断字符是否属于 URL scheme 首字母的 simple-fold 闭包。
///
/// ASCII K/S 的 Unicode simple-fold 轨道额外包含 Kelvin sign 与 long s；其他 ASCII
/// 字母只有普通大小写两项。这里不能用 `is_alphabetic`，否则会错误接受任意 Unicode
/// 字母作为 URL scheme。
#[must_use]
fn is_scheme_letter(character: char) -> bool {
    character.is_ascii_alphabetic() || matches!(character, '\u{212a}' | '\u{017f}')
}

/// 判断字符是否属于凭据扫描模式使用的 ASCII 空白集合。
#[must_use]
fn is_pattern_whitespace(character: char) -> bool {
    matches!(character, ' ' | '\t' | '\n' | '\r' | '\u{000c}')
}
