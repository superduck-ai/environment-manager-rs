//! BYOC（Bring Your Own Cloud）环境实现。
//!
//! 模块实现配置、脚本、Git 和认证语义；生产构造默认接入
//! sources/podmonitor，同时保留最小 trait 边界供测试替换外部副作用。

// 允许实现模块与父模块同名。
#[allow(clippy::module_inception)]
mod byoc;
mod source_processor;
mod types;

pub use byoc::{MAX_TASK_SETUP_SCRIPT_BYTES, REGISTRATION, new};
pub use types::{
    ByocConfig, ByocEnvironmentType, ByocError, ContainProvideAuthTransport, LeaseManagerFactory,
    LeaseManagerRequest, SourceProcessMode, SourceProcessRequest, SourceProcessor,
    SourceProcessorError,
};

#[cfg(test)]
mod byoc_test;
