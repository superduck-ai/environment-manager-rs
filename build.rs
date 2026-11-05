//! Tunnel Proto 的构建期代码生成入口。
//!
//! 根据工作区 `gen/proto/anthropic/sessions/tunnel/v1alpha/tunnel.proto` 运行
//! `protoc`。生成的 Prost 消息和 pbjson 实现只写入 Cargo `OUT_DIR`，源码树中
//! 只保留协议原文件与薄封装，避免手工修改生成代码。

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// 读取命令的单行 stdout；版本元数据不可用时由调用方提供稳定回退值。
fn command_output(program: &str, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program).args(arguments).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

/// 直接用 Cargo 构建时，提交或切换分支后刷新二进制中的 Git 哈希。
fn emit_git_rerun_paths() {
    for git_path in ["HEAD", "logs/HEAD", "packed-refs"] {
        let Some(path) = command_output("git", &["rev-parse", "--git-path", git_path]) else {
            continue;
        };
        if Path::new(&path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(reference) = command_output("git", &["symbolic-ref", "-q", "HEAD"])
        && let Some(path) = command_output("git", &["rev-parse", "--git-path", &reference])
        && Path::new(&path).exists()
    {
        println!("cargo:rerun-if-changed={path}");
    }
}

/// 把 Git 提交号与 UTC 构建时间注入最终二进制。
fn emit_build_metadata() {
    println!("cargo:rerun-if-env-changed=BUILD_GIT_HASH");
    println!("cargo:rerun-if-env-changed=BUILD_TIME");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=Cargo.lock");
    emit_git_rerun_paths();

    let git_hash = std::env::var("BUILD_GIT_HASH")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| command_output("git", &["rev-parse", "--short=12", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_owned());
    let build_time = std::env::var("BUILD_TIME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| command_output("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]))
        .unwrap_or_else(|| {
            let seconds = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| duration.as_secs());
            format!("unix:{seconds}")
        });

    println!("cargo:rustc-env=BUILD_GIT_HASH={git_hash}");
    println!("cargo:rustc-env=BUILD_TIME={build_time}");
}

/// 从固定的 Tunnel Proto 生成 Prost 类型和 ProtoJSON 序列化实现。
///
/// 构建脚本使用随 crate 固定的 `protoc`，避免验证机全局工具版本影响生成结果。
/// `pbjson-build` 读取同一次生成的 descriptor set，因此 bytes、oneof、默认值和
/// lowerCamel 字段名都遵守 ProtoJSON，而不是普通 Serde JSON 规则。
fn main() -> Result<(), Box<dyn Error>> {
    emit_build_metadata();

    let proto_root = Path::new("gen/proto");
    let proto_file = proto_root.join("anthropic/sessions/tunnel/v1alpha/tunnel.proto");
    println!("cargo:rerun-if-changed={}", proto_file.display());

    let output_directory = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = output_directory.join("tunnel_descriptor.bin");

    let mut prost = prost_build::Config::new();
    prost.file_descriptor_set_path(&descriptor_path);
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    prost.compile_protos(&[proto_file], &[proto_root])?;

    let descriptors = fs::read(descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptors)?
        .ignore_unknown_fields()
        .build(&[".anthropic.sessions.tunnel.v1alpha"])?;
    Ok(())
}
