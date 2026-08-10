//! `provision-packages` v1 stdin manifest 的严格 schema 与安全校验。
//!
//! 对应 Go `internal/packages/manifest.go`。Serde 默认会忽略重复键或允许别名式字段
//! 处理，因此这里手写 map visitor：字段名区分大小写、未知字段和重复字段均失败；
//! manager 显式为 `null` 也与省略字段分开记录。package spec 只作为 argv 传递，
//! 仍拒绝 option 前缀和 URL authority 凭据，防止参数注入与 secret 泄露。

use std::fmt::{self, Formatter};

use serde::{Deserialize, Deserializer, de};

/// stdin manifest 的最大字节数，与 Go 的 `1 << 20` 完全一致。
pub(super) const MAX_MANIFEST_BYTES: usize = 1 << 20;

/// 顶层版本化 package manifest。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct Manifest {
    /// schema 版本，当前只接受 `1`。
    pub version: i32,
    /// package 配置对象；缺失和显式 `null` 均由语义校验拒绝。
    pub packages: Option<PackageManifest>,
}

/// 按 manager 分组的原生 package spec。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct PackageManifest {
    /// discriminator，当前必须是 `packages`。
    pub package_type: String,
    /// APT package spec。
    pub apt: SpecList,
    /// Cargo package spec。
    pub cargo: SpecList,
    /// Ruby Gem package spec。
    pub gem: SpecList,
    /// Go module package spec。
    pub go: SpecList,
    /// NPM package spec。
    pub npm: SpecList,
    /// PIP package spec。
    pub pip: SpecList,
}

/// 保留 manager 字段是否显式写成 `null`。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct SpecList {
    /// `true` 表示 JSON 明确提供了 `null`，这种输入不符合 schema。
    pub is_null: bool,
    /// 省略字段或有效字符串数组中的 package spec。
    pub values: Vec<String>,
}

impl<'de> Deserialize<'de> for SpecList {
    /// 对应 Go `SpecList.UnmarshalJSON`，将 `null` 与省略字段区分并拒绝非字符串数组。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Option::<Vec<String>>::deserialize(deserializer)?;
        Ok(match values {
            Some(values) => Self {
                is_null: false,
                values,
            },
            None => Self {
                is_null: true,
                values: Vec::new(),
            },
        })
    }
}

impl<'de> Deserialize<'de> for Manifest {
    /// 只接受精确的 `version` 和 `packages` 字段，并拒绝重复键。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ManifestVisitor)
    }
}

/// 顶层 manifest 的严格 Serde map visitor。
struct ManifestVisitor;

impl<'de> de::Visitor<'de> for ManifestVisitor {
    type Value = Manifest;

    /// 描述顶层 schema，供 Serde 类型错误构造有界内部消息。
    fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("a package provisioning manifest object")
    }

    /// 逐字段解析顶层对象，在读取值前拒绝未知或重复字段。
    ///
    /// 对应 Go `validateManifestFieldNames` 和 `discardJSONValue`；Serde 的 map visitor
    /// 已同时承担字段扫描与安全跳过职责，所以不再另外保留只为 decoder 服务的 helper。
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut manifest = Manifest::default();
        let mut saw_version = false;
        let mut saw_packages = false;
        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "version" if !saw_version => {
                    saw_version = true;
                    manifest.version = map.next_value()?;
                }
                "packages" if !saw_packages => {
                    saw_packages = true;
                    manifest.packages = map.next_value()?;
                }
                "version" | "packages" => return Err(de::Error::custom("duplicate JSON field")),
                _ => {
                    return Err(de::Error::custom(
                        "manifest field name does not match schema",
                    ));
                }
            }
        }
        Ok(manifest)
    }
}

impl<'de> Deserialize<'de> for PackageManifest {
    /// 只接受 v1 schema 的七个精确字段，并拒绝重复键。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(PackageManifestVisitor)
    }
}

/// package 对象的严格 Serde map visitor。
struct PackageManifestVisitor;

impl<'de> de::Visitor<'de> for PackageManifestVisitor {
    type Value = PackageManifest;

    /// 描述 package 子对象 schema，供 Serde 类型错误构造有界内部消息。
    fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("a package manager manifest object")
    }

    /// 逐字段解析 package 子对象并维护位掩码，以最低开销拒绝重复键。
    ///
    /// 对应 Go `validatePackageFieldNames`、`rememberJSONField` 和
    /// `isPackageFieldName`；字段白名单和重复检测在同一遍 map 读取中完成，避免二次解析。
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut packages = PackageManifest::default();
        let mut seen = 0_u8;
        while let Some(field) = map.next_key::<String>()? {
            let (bit, target) = match field.as_str() {
                "type" => (1, 0),
                "apt" => (2, 1),
                "cargo" => (4, 2),
                "gem" => (8, 3),
                "go" => (16, 4),
                "npm" => (32, 5),
                "pip" => (64, 6),
                _ => {
                    return Err(de::Error::custom(
                        "package field name does not match schema",
                    ));
                }
            };
            if seen & bit != 0 {
                return Err(de::Error::custom("duplicate JSON field"));
            }
            seen |= bit;
            match target {
                0 => packages.package_type = map.next_value()?,
                1 => packages.apt = map.next_value()?,
                2 => packages.cargo = map.next_value()?,
                3 => packages.gem = map.next_value()?,
                4 => packages.go = map.next_value()?,
                5 => packages.npm = map.next_value()?,
                6 => packages.pip = map.next_value()?,
                _ => unreachable!("package field dispatch is exhaustive"),
            }
        }
        Ok(packages)
    }
}

impl PackageManifest {
    /// 按 Go 固定顺序返回六个 manager 列表，供校验和计数共享。
    #[must_use]
    pub(super) fn lists(&self) -> [&SpecList; 6] {
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

/// 对应 Go `validateManifest`，执行版本、discriminator 和 spec 安全校验。
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
        if specs.is_null {
            return Err("package manager list must not be null");
        }
        if specs
            .values
            .iter()
            .any(|spec| is_package_manager_option(spec))
        {
            return Err("package spec must not be an option");
        }
        if specs
            .values
            .iter()
            .any(|spec| contains_url_credentials(spec))
        {
            return Err("package spec must not contain URL credentials");
        }
    }
    Ok(())
}

/// 对应 Go `isPackageManagerOption`，拒绝去除前导空白后以 `-` 开头的 spec。
#[must_use]
fn is_package_manager_option(spec: &str) -> bool {
    spec.trim_start().starts_with('-')
}

/// 复刻 Go `credentialURLPattern`，判断任意 URL authority 中是否出现 `@`。
///
/// 这里只实现固定正则 `[a-z][a-z0-9+.-]*://[^/\s?#]*@` 的状态机，避免为单个安全
/// 检查增加 regex 依赖。Go `(?i)` 的 Unicode simple-fold 特例和 RE2 的 ASCII
/// `\s` 均显式保留；percent-encoding 是否有效不影响凭据判定。
#[must_use]
fn contains_url_credentials(spec: &str) -> bool {
    for (start, first) in spec.char_indices() {
        if !is_go_ascii_fold_letter(first) {
            continue;
        }
        let mut cursor = start + first.len_utf8();
        while let Some(character) = spec[cursor..].chars().next() {
            if !(is_go_ascii_fold_letter(character)
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
            if matches!(character, '/' | '?' | '#') || is_go_regexp_space(character) {
                break;
            }
            cursor += character.len_utf8();
        }
    }
    false
}

/// 判断字符是否属于 Go `(?i)[a-z]` 的 simple-fold 闭包。
///
/// ASCII K/S 的 Unicode simple-fold 轨道额外包含 Kelvin sign 与 long s；其他 ASCII
/// 字母只有普通大小写两项。这里不能用 `is_alphabetic`，否则会错误接受任意 Unicode
/// 字母作为 URL scheme。
#[must_use]
fn is_go_ascii_fold_letter(character: char) -> bool {
    character.is_ascii_alphabetic() || matches!(character, '\u{212a}' | '\u{017f}')
}

/// 判断字符是否属于 Go regexp/RE2 的 Perl `\s` 字节集合。
#[must_use]
fn is_go_regexp_space(character: char) -> bool {
    matches!(character, ' ' | '\t' | '\n' | '\r' | '\u{000c}')
}
