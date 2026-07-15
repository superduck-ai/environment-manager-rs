//! 环境类型注册表及具体环境实现。
//!
//! 本模块机械对应 Go 的 `internal/envtype`。本批只接入 BYOC 和共享嵌入资源，
//! Anthropic 环境会在其实现迁移完成后再显式声明，避免出现空壳注册项。

pub mod anthropic;
pub mod byoc;
// 保留 Go `envtype/envtype.go` 到 Rust `envtype/envtype.rs` 的机械映射。
#[allow(clippy::module_inception)]
pub mod envtype;
pub mod shared;
pub mod zz_recovered_types;

pub use envtype::{EnvTypeError, new_env_types};
pub use zz_recovered_types::{
    EnvTypes, EnvironmentType, Factory, LeaseManagerHandle, LeaseManagerProvider, Registration,
};

#[cfg(test)]
mod envtype_test;
