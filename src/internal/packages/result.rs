//! 包安装协议的终态结果与稳定退出码。
//!
//! 枚举在 Rust 内提供类型约束，序列化时使用协议规定的 snake_case 字符串；可选字段
//! 借助 `skip_serializing_if` 省略，结构体字段顺序固定最终 JSON 的键顺序。

use serde::{Deserialize, Serialize};

/// 包安装命令的终态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// 所有非空 manager 列表均安装成功。
    Succeeded,
    /// manifest、执行环境或某个 package manager 失败。
    Failed,
}

/// 包安装失败的稳定分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Category {
    /// stdin 不是合法、受支持且严格符合 schema 的 manifest。
    InvalidManifest,
    /// package manager 已启动，但以非零状态退出。
    PackageManager,
    /// root、工作目录或可执行文件等运行条件不满足。
    ExecutionEnvironment,
    /// 调用方提供的截止时间已经到期。
    Timeout,
    /// 调用方或进程信号取消了安装。
    Cancelled,
    /// 子进程状态或最终输出等内部不变量失败。
    Internal,
}

/// manifest 支持的 package manager。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Manager {
    /// Debian APT。
    Apt,
    /// Rust Cargo。
    Cargo,
    /// Ruby Gem。
    Gem,
    /// toolchain。
    Go,
    /// Node.js NPM。
    Npm,
    /// Python PIP。
    Pip,
}

impl Manager {
    /// 返回 诊断文本和协议 JSON 使用的小写 manager 名称。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Apt => "apt",
            Self::Cargo => "cargo",
            Self::Gem => "gem",
            Self::Go => "go",
            Self::Npm => "npm",
            Self::Pip => "pip",
        }
    }
}

/// 包安装流程中可对外报告的阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    /// 读取或检查 JSON 完整性。
    Decode,
    /// 检查 schema、版本和 package spec。
    Validate,
    /// 检查 root、工作目录或预先取消状态。
    Preflight,
    /// 启动 package manager 可执行文件。
    Start,
    /// 执行 APT metadata 更新等安装前准备。
    Prepare,
    /// 执行实际安装命令。
    Install,
    /// 序列化或写出唯一终态 JSON。
    Finalize,
}

impl Stage {
    /// 返回 诊断文本和协议 JSON 使用的小写阶段名。
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Validate => "validate",
            Self::Preflight => "preflight",
            Self::Start => "start",
            Self::Prepare => "prepare",
            Self::Install => "install",
            Self::Finalize => "finalize",
        }
    }
}

/// `provision-packages` stdout 唯一允许出现的权威 JSON 对象。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Result {
    /// 协议结果版本，当前固定为 `1`。
    pub version: i32,
    /// 成功或失败终态。
    pub status: Status,
    /// 失败分类；成功时省略。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<Category>,
    /// 失败的 package manager；manifest 或环境失败时省略。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manager: Option<Manager>,
    /// 失败阶段；无 package manager 的成功结果省略。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<Stage>,
    /// manifest 内所有 manager spec 的总数；decode/validate 失败时省略。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_count: Option<usize>,
    /// 从命令开始到终态产生的非负毫秒数。
    pub duration_ms: i64,
    /// package manager 自身的非零退出码；信号或启动失败时省略。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

impl Result {
    /// 创建尚未分类的 v1 失败结果，供各阶段只填写自身掌握的字段。
    #[must_use]
    pub const fn failed() -> Self {
        Self {
            version: 1,
            status: Status::Failed,
            category: None,
            manager: None,
            stage: None,
            package_count: None,
            duration_ms: 0,
            exit_code: None,
        }
    }

    /// 返回供 shell 使用的粗粒度稳定退出码。
    ///
    /// JSON 中的 `category` 和 `exit_code` 才是完整诊断；进程退出码仅区分成功、
    /// manifest、manager、取消/超时以及执行环境/内部失败。
    #[must_use]
    pub const fn process_exit_code(&self) -> i32 {
        if matches!(self.status, Status::Succeeded) {
            return 0;
        }
        match self.category {
            Some(Category::InvalidManifest) => 2,
            Some(Category::PackageManager) => 10,
            Some(Category::Timeout | Category::Cancelled) => 11,
            _ => 12,
        }
    }
}
