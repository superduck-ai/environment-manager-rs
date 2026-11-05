//! 具体环境类型共用的内置 Claude Code 配置与 stop hook。

mod embedded;

pub use embedded::{
    BAKU_SETTINGS_JSON, BAKU_STOP_HOOK_SCRIPT, DEFAULT_SETTINGS_JSON, STOP_HOOK_SCRIPT,
};
