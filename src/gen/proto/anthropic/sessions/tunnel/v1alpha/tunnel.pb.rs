//! 自动生成的 Tunnel v1alpha Prost 与 ProtoJSON 类型。
//!
//! 来源为仓库内
//! `tunnel.proto`。实际内容由 `build.rs` 写入 `OUT_DIR`，本文件只保持原始文件基名
//! 和模块边界；禁止手工修改生成代码。

// pbjson 0.9 的模板会把字段名列表写成 `&FIELDS`。这在 Rust 1.97 上仅触发风格
// lint，因此只对自动生成模块豁免，手写代码仍保持全量 Clippy 门禁。
#![allow(clippy::useless_borrows_in_formatting)]

include!(concat!(
    env!("OUT_DIR"),
    "/anthropic.sessions.tunnel.v1alpha.rs"
));
include!(concat!(
    env!("OUT_DIR"),
    "/anthropic.sessions.tunnel.v1alpha.serde.rs"
));
