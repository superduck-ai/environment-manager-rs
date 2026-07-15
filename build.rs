//! Tunnel Proto 的构建期代码生成入口。
//!
//! 对应 Go 工作区 `gen/proto/anthropic/sessions/tunnel/v1alpha/tunnel.proto` 的
//! `protoc` 流程。生成的 Prost 消息和 pbjson 实现只写入 Cargo `OUT_DIR`，源码树中
//! 只保留协议原文件与薄封装，避免手工修改生成代码。

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

/// 从固定的 Tunnel Proto 生成 Prost 类型和 ProtoJSON 序列化实现。
///
/// 构建脚本使用随 crate 固定的 `protoc`，避免验证机全局工具版本影响生成结果。
/// `pbjson-build` 读取同一次生成的 descriptor set，因此 bytes、oneof、默认值和
/// lowerCamel 字段名都遵守 ProtoJSON，而不是普通 Serde JSON 规则。
fn main() -> Result<(), Box<dyn Error>> {
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
        .build(&[".anthropic.sessions.tunnel.v1alpha"])?;
    Ok(())
}
