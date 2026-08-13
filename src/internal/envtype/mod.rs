//! 环境类型注册表及具体环境实现。
//!
//! 注册 BYOC、Anthropic 环境和共享嵌入资源。

pub mod anthropic;
pub mod byoc;
// 允许实现模块与父模块同名。
#[allow(clippy::module_inception)]
pub mod envtype;
pub mod shared;
pub mod types;

pub use envtype::{EnvTypeError, new_env_types};
pub use types::{
    EnvTypes, EnvironmentType, Factory, LeaseManagerHandle, LeaseManagerProvider, Registration,
};

#[cfg(test)]
mod envtype_test;
