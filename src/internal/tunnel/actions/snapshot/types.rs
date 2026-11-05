//! Snapshot action 的数据结构。
//!
//! Snapshot action 类型。路径保留为字段，
//! 使 Linux 测试能把固定生产路径替换为临时目录；外部协议字段显式使用 snake_case。
/// 收集 Baku 草稿、探索文件和 Git 元数据的 action。
pub struct SnapshotAction {
    pub(crate) drafts_path: std::path::PathBuf,
    pub(crate) explorations_path: std::path::PathBuf,
    pub(crate) project_path: std::path::PathBuf,
}

/// 快照中的单个文本文件。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct FileEntry {
    pub(crate) filename: String,
    pub(crate) content: String,
}

/// 返回给控制端的稳定 JSON 结构。
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SnapshotResponse {
    pub(crate) drafts: Vec<FileEntry>,
    pub(crate) explorations: Vec<FileEntry>,
    pub(crate) has_built_content: bool,
    pub(crate) commit_count: i32,
    pub(crate) truncated: bool,
    pub(crate) app_modified: bool,
}
