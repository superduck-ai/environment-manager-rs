//! 中文注释覆盖审计。
//!
//! 该测试扫描手写生产源码，保证每个模块和函数都具有中文文档注释。自动生成的
//! 生成器产物位于 Cargo `OUT_DIR`，不进入扫描；`src/gen` 中的手写 wrapper 仍参与
//! 审计。测试模块不参与函数 rustdoc 门禁，避免测试样板产生噪声；但其中的 unsafe
//! 仍必须说明。迁移期旧符号也不得留在注释中，避免实现说明重新依赖已经删除的代码。
//! `vendor` 位于扫描根之外，第三方补丁由各依赖目录单独审计。

use std::fs;
use std::path::{Path, PathBuf};

use quote::ToTokens;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, ExprUnsafe, ImplItemFn, ItemFn, ItemMod, TraitItemFn};
use walkdir::WalkDir;

/// 判断字符串中是否至少包含一个常用或扩展中文字符。
///
/// # 参数
/// - `value`：待检查的 rustdoc 文本。
///
/// # 返回
/// 找到中文字符时返回 `true`。
fn contains_chinese(value: &str) -> bool {
    value.chars().any(|character| {
        matches!(
            character as u32,
            0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
        )
    })
}

/// 从属性集合中提取编译器保存的 `#[doc = "..."]` 文本。
///
/// ` ` 和 ` ` 在语法树中都会转换成 doc 属性，因此统一从属性读取可以避免依赖
/// 源码缩进或换行形式。
fn has_chinese_doc(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        if !attribute.path().is_ident("doc") {
            return false;
        }
        let syn::Meta::NameValue(name_value) = &attribute.meta else {
            return false;
        };
        let syn::Expr::Lit(expression) = &name_value.value else {
            return false;
        };
        let syn::Lit::Str(value) = &expression.lit else {
            return false;
        };
        contains_chinese(&value.value())
    })
}

/// 判断模块是否仅用于测试；测试实现不属于生产函数注释门禁。
fn is_test_module(module: &ItemMod) -> bool {
    module.ident == "tests"
        || module.attrs.iter().any(|attribute| {
            attribute.path().is_ident("cfg")
                && attribute
                    .meta
                    .to_token_stream()
                    .to_string()
                    .contains("test")
        })
}

/// 收集缺少中文 rustdoc 的函数、方法和 trait 方法。
struct FunctionDocVisitor<'a> {
    file: &'a Path,
    missing: Vec<String>,
}

impl FunctionDocVisitor<'_> {
    /// 记录一个没有中文文档的符号，错误信息同时携带文件路径，便于直接修复。
    fn require_doc(&mut self, symbol: &str, attributes: &[Attribute]) {
        if !has_chinese_doc(attributes) {
            self.missing.push(format!(
                "{}: `{symbol}` 缺少中文 /// 文档",
                self.file.display()
            ));
        }
    }
}

impl<'ast> Visit<'ast> for FunctionDocVisitor<'_> {
    /// 审计自由函数，并继续访问其中可能存在的嵌套 item。
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        self.require_doc(&function.sig.ident.to_string(), &function.attrs);
        visit::visit_item_fn(self, function);
    }

    /// 审计结构体或枚举的实现方法。
    fn visit_impl_item_fn(&mut self, function: &'ast ImplItemFn) {
        self.require_doc(&function.sig.ident.to_string(), &function.attrs);
        visit::visit_impl_item_fn(self, function);
    }

    /// 审计 trait 中声明或提供默认实现的方法。
    fn visit_trait_item_fn(&mut self, function: &'ast TraitItemFn) {
        self.require_doc(&function.sig.ident.to_string(), &function.attrs);
        visit::visit_trait_item_fn(self, function);
    }

    /// 跳过 `tests` 或 `#[cfg(test)]` 模块，其他模块按正常层级递归。
    fn visit_item_mod(&mut self, module: &'ast ItemMod) {
        if !is_test_module(module) {
            visit::visit_item_mod(self, module);
        }
    }
}

/// 返回需要参与审计的全部手写 Rust 源文件。
fn production_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = handwritten_sources(root)
        .into_iter()
        .filter(|path| {
            !path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem.ends_with("_test"))
        })
        .collect::<Vec<_>>();
    let build_script = root.join("build.rs");
    if build_script.is_file() {
        sources.push(build_script);
    }
    sources
}

/// 返回全部手写 Rust 源文件，包括需要审计 unsafe 说明的测试文件。
fn handwritten_sources(root: &Path) -> Vec<PathBuf> {
    [root.join("src"), root.join("crates/mcp-server/src")]
        .into_iter()
        .flat_map(|path| WalkDir::new(path).into_iter())
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.into_path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect()
}

/// 返回需要检查 `unsafe` 说明的全部项目手写 Rust 文件。
///
/// 函数级中文 rustdoc 门禁覆盖主包与兼容 MCP crate 的生产模块；`unsafe` 约束更严格，还要
/// 覆盖集成测试和根构建脚本，避免测试 fixture 或代码生成入口留下未解释的裸指针、
/// 环境变量写入与系统调用。第三方 `vendor` 与 Cargo `OUT_DIR` 生成物不在扫描根内。
fn unsafe_audit_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = handwritten_sources(root);
    sources.extend(
        [root.join("tests"), root.join("crates/mcp-server/tests")]
            .into_iter()
            .flat_map(|path| WalkDir::new(path).into_iter())
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| entry.into_path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "rs")),
    );
    let build_script = root.join("build.rs");
    if build_script.is_file() {
        sources.push(build_script);
    }
    sources
}

/// 收集源码内每个 unsafe block 的起始行。
#[derive(Default)]
struct UnsafeBlockVisitor {
    lines: Vec<usize>,
}

impl<'ast> Visit<'ast> for UnsafeBlockVisitor {
    /// 记录 unsafe block 行号并继续访问其内部表达式。
    fn visit_expr_unsafe(&mut self, expression: &'ast ExprUnsafe) {
        self.lines.push(expression.span().start().line);
        visit::visit_expr_unsafe(self, expression);
    }
}

/// 判断 unsafe block 前相邻注释是否同时包含 `SAFETY:` 标记和中文解释。
fn has_chinese_safety_comment(lines: &[&str], unsafe_line: usize) -> bool {
    let mut comments = Vec::new();
    let mut index = unsafe_line.saturating_sub(1);
    while index > 0 {
        let candidate = lines[index - 1].trim();
        if candidate.starts_with("//")
            || candidate.starts_with("/*")
            || candidate.starts_with('*')
            || candidate.ends_with("*/")
        {
            comments.push(candidate);
            index -= 1;
        } else if candidate.is_empty() && comments.is_empty() {
            index -= 1;
        } else {
            break;
        }
    }
    let comment = comments.join("\n");
    comment.contains("SAFETY:") && contains_chinese(&comment)
}

/// 验证所有手写生产文件都具备模块级和函数级中文说明。
#[test]
fn handwritten_sources_have_chinese_documentation() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut failures = Vec::new();

    for file in production_sources(&root) {
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("读取 {} 失败: {error}", file.display()));
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("解析 {} 失败: {error}", file.display()));

        if !has_chinese_doc(&syntax.attrs) {
            failures.push(format!("{} 缺少中文 //! 模块说明", file.display()));
        }

        let mut visitor = FunctionDocVisitor {
            file: &file,
            missing: Vec::new(),
        };
        visitor.visit_file(&syntax);
        failures.extend(visitor.missing);
    }

    assert!(
        failures.is_empty(),
        "中文注释审计失败:\n{}",
        failures.join("\n")
    );
}

/// 验证生产与测试中的每个 unsafe block 都有中文 `SAFETY:` 约束说明。
#[test]
fn unsafe_blocks_have_chinese_safety_comments() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut failures = Vec::new();

    for file in unsafe_audit_sources(&root) {
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("读取 {} 失败: {error}", file.display()));
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("解析 {} 失败: {error}", file.display()));
        let mut visitor = UnsafeBlockVisitor::default();
        visitor.visit_file(&syntax);
        let lines = source.lines().collect::<Vec<_>>();
        for line in visitor.lines {
            if !has_chinese_safety_comment(&lines, line) {
                failures.push(format!(
                    "{}:{line} unsafe block 缺少中文 // SAFETY: 说明",
                    file.display()
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "unsafe 中文说明审计失败:\n{}",
        failures.join("\n")
    );
}

/// 验证注释没有重新引用已经删除的迁移期符号。
#[test]
fn comments_do_not_reference_removed_migration_symbols() {
    const FORBIDDEN_TERMS: &[&str] = &[
        "RunE",
        "fmt.Fscanf",
        "runTaskRunManager",
        "Cmd.Run",
        "cmd.Stdin",
        "Cmd.Dir",
        "bytes.Buffer",
        "os/exec",
        "skipStdinCopyError",
        "PathError",
        "registerMCPServersAsync",
        "initializeEnvironmentAsync",
        "GetSessionContext",
        "ClientOptions.Timeout",
        "isEventData",
        "api.LogCategory",
        "closeTunnelDone",
        "Conn.Close",
        "runGitCommand",
        "CreateLeaseManager",
        "podmonitor.NewLeaseManager",
        "statPath",
        "GetAnthropicOAuthToken",
        "SanitizeOutput",
        "proxy_FromURL",
        "resp.Status",
        "passwordSet",
        "asWSOpen",
        "SetReadLimit",
        "ReadMessage",
        "RepoAuths",
        "time.Since",
        "cancel(); wg.Wait()",
        "requestHeader",
        "Request.Header",
        "Request.Host",
        "NewToolResultText",
        "NewToolResultError",
        "handleInstallError",
        "result.err",
        "gitproxy.Manager",
    ];
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut failures = Vec::new();

    for file in unsafe_audit_sources(&root) {
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("读取 {} 失败: {error}", file.display()));
        for (index, line) in source.lines().enumerate() {
            let trimmed = line.trim_start();
            if !(trimmed.starts_with("//") || trimmed.starts_with("/*") || trimmed.starts_with('*'))
            {
                continue;
            }
            for term in FORBIDDEN_TERMS {
                if line.contains(term) {
                    failures.push(format!(
                        "{}:{} 注释仍引用迁移期符号 `{term}`",
                        file.display(),
                        index + 1
                    ));
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "迁移注释审计失败:\n{}",
        failures.join("\n")
    );
}
