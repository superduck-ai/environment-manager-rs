//! Baku 项目部署 action。
//!
//! Vercel 路径先运行 `npm run build`，再以 SHA-1 内容索引完成缺失文件上传；Antspace
//! 路径把项目按 Go 相同的目录遍历和 tar.gz 规则打包。所有外部进程和 HTTP 请求均
//! 接受 tunnel action 的取消令牌。

mod action;
mod antspace;
mod types;
mod vercel;

pub use action::{DEPLOY_TIMEOUT, new_deploy_action};
pub use antspace::{create_tarball, new_antspace_client};
pub use types::{
    AntspaceClient, AntspaceDeployResult, DeployAction, Deployment, FileEntry, VercelClient,
};
pub use vercel::{
    CollectedFiles, MAX_DEPLOYMENT_BYTES, collect_files, project_name, validate_project_dir,
};

#[cfg(test)]
mod deploy_test;
