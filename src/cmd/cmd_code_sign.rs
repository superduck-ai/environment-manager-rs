//! 隐藏签名包装命令。
//!
//! Git 的 `gpg.ssh.program` 会传入 OpenSSH 的 `-Y`、
//! `-f`、`-n` 等值必须原样保留。本文件还实现 Git object format 探测，并使用
//! session ingress token 直接请求远端签名 API、写出同名 `.sig` 文件。

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::time::Duration;

use clap::builder::OsStringValueParser;
use clap::{Arg, ArgMatches, Command};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncWriteExt as _;

use super::error::CliError;
use crate::internal::{
    codesign::{CONTEXT_ENV as CODESIGN_CONTEXT_ENV, CodeSignContext},
    util::{claude_code_version, version},
};

const SESSION_INGRESS_TOKEN_FILE_ENV: &str = "CLAUDE_SESSION_INGRESS_TOKEN_FILE";

/// 禁用 flag 解析后捕获到的 code-sign 原始 argv。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeSignArgs {
    /// 不含程序名和 `code-sign` 子命令本身的原始参数。
    pub raw_args: Vec<OsString>,
}

impl CodeSignArgs {
    /// 根据参数序列识别受支持的签名操作。
    ///
    /// 当前只支持参数序列中出现 `-Y sign` 的 SSH 签名。该方法不访问文件系统，
    /// 相对路径的绝对化、凭据读取与远端 API 调用留给执行层完成。
    pub fn parse_operation(&self) -> Result<CodeSignOperation, String> {
        let (found_sign_operation, filtered) = remove_ssh_sign_marker(&self.raw_args);
        if !found_sign_operation {
            return Err(
                "unsupported code-sign operation: currently only SSH-style signing (-Y sign) is supported"
                    .to_owned(),
            );
        }
        parse_ssh_sign_args(&filtered).map(CodeSignOperation::SshSign)
    }
}

/// code-sign 执行层能够处理的操作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeSignOperation {
    /// OpenSSH `ssh-keygen -Y sign` 风格请求。
    SshSign(SshSignArgs),
}

/// 从 OpenSSH 参数中提取的签名请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshSignArgs {
    /// 需要签名的文件；执行层应将相对路径转换为绝对路径。
    pub file_path: PathBuf,
    /// `-f` 指定的 key 文件；当前命令仅记录并忽略它。
    pub key_file: Option<PathBuf>,
    /// `-n` 指定的 namespace；当前命令仅记录并忽略它。
    pub namespace: Option<OsString>,
}

/// 构造隐藏且完全关闭 help/flag 解析的 `code-sign` 子命令。
pub(crate) fn add_code_sign_command() -> Command {
    Command::new("code-sign")
        .about("Act as a GPG/SSH signing wrapper using the remote signing API")
        .long_about("This command acts as a GPG/SSH signing wrapper that invokes the remote \nsigning API using the current session ingress token. It's designed to be used \nwith git's gpg.ssh.program configuration.")
        .hide(true)
        .disable_help_flag(true)
        .disable_version_flag(true)
        .trailing_var_arg(true)
        .arg(
            Arg::new("raw-args")
                .num_args(0..)
                .allow_hyphen_values(true)
                .value_parser(OsStringValueParser::new()),
        )
}

/// 将禁用 flag 解析后的所有值原样复制到调用对象。
pub(crate) fn from_matches(matches: &ArgMatches) -> Result<CodeSignArgs, CliError> {
    let raw_args = matches
        .get_many::<OsString>("raw-args")
        .map(|values| values.cloned().collect())
        .unwrap_or_default();
    Ok(CodeSignArgs { raw_args })
}

/// 删除参数中的所有 `-Y sign` 对，并报告是否至少找到一对。
fn remove_ssh_sign_marker(args: &[OsString]) -> (bool, Vec<OsString>) {
    let mut found = false;
    let mut filtered = Vec::with_capacity(args.len());
    let mut index = 0;
    while index < args.len() {
        if args[index] == OsStr::new("-Y")
            && args
                .get(index + 1)
                .is_some_and(|value| value == OsStr::new("sign"))
        {
            found = true;
            index += 2;
        } else {
            filtered.push(args[index].clone());
            index += 1;
        }
    }
    (found, filtered)
}

/// 以宽松的 flag 扫描方式提取首个非 flag 文件。
fn parse_ssh_sign_args(args: &[OsString]) -> Result<SshSignArgs, String> {
    let mut file_path = None;
    let mut key_file = None;
    let mut namespace = None;
    let mut index = 0;

    while index < args.len() {
        let argument = &args[index];
        if starts_with_dash(argument) {
            if argument == OsStr::new("-f") {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing argument for -f flag".to_owned())?;
                key_file = Some(PathBuf::from(value));
                index += 2;
            } else if argument == OsStr::new("-n") {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| "missing argument for -n flag".to_owned())?;
                namespace = Some(value.clone());
                index += 2;
            } else {
                // 会把未知 flag 后紧邻的第一个非 flag 值也跳过。
                index += 1;
                if args
                    .get(index)
                    .is_some_and(|value| !starts_with_dash(value))
                {
                    index += 1;
                }
            }
        } else {
            if file_path.is_none() {
                file_path = Some(PathBuf::from(argument));
            }
            index += 1;
        }
    }

    let file_path = file_path.ok_or_else(|| "no file specified to sign".to_owned())?;
    Ok(SshSignArgs {
        file_path,
        key_file,
        namespace,
    })
}

/// 用有损视图只判断首字节是否为 `-`，不会修改或返回原始路径数据。
fn starts_with_dash(value: &OsStr) -> bool {
    value.to_string_lossy().starts_with('-')
}

///
/// 识别原始 `-Y sign` 操作并转交 SSH 签名实现。任何其他 GPG/SSH 操作都返回明确
/// 的稳定错误；本函数不会把原始 argv（可能含路径或其他敏感参数）写入日志。
pub async fn run_code_sign(arguments: &CodeSignArgs) -> Result<(), String> {
    match arguments.parse_operation()? {
        CodeSignOperation::SshSign(sign) => handle_ssh_sign(&sign).await,
    }
}

///
/// 将文件路径绝对化，探测仓库 object format，从 Claude child 环境读取非敏感签名
/// 上下文和 ingress token 文件，直接请求远端签名 API 并以 0600 写出 `.sig`。
/// `-n` 与 `-f` 仅打印调试提示，继续由远端服务选择实际签名 key。
///
/// # 副作用
/// 读取工作目录、配置和待签名文件，执行 `git config`，访问远端 HTTP API，创建签名
/// 文件并写 stderr。等待 response headers 最多 30 秒。
pub(super) async fn handle_ssh_sign(arguments: &SshSignArgs) -> Result<(), String> {
    let original_path = arguments.file_path.to_string_lossy().into_owned();
    let file_path = if arguments.file_path.is_absolute() {
        arguments.file_path.clone()
    } else {
        std::path::absolute(&arguments.file_path).map_err(|error| {
            format!("failed to get absolute path for {}: {error}", original_path)
        })?
    };

    if let Some(namespace) = &arguments.namespace {
        eprintln!(
            "Debug: Namespace set to {:?} (ignored)",
            namespace.to_string_lossy()
        );
    }
    if let Some(key_file) = &arguments.key_file {
        eprintln!(
            "Debug: Key file set to {:?} (ignored, using server key)",
            key_file.to_string_lossy()
        );
    }

    let working_dir = match std::env::current_dir() {
        Ok(directory) => directory,
        Err(error) => {
            eprintln!("Warning: could not determine working directory: {error}");
            PathBuf::new()
        }
    };
    let git_object_format = get_git_object_format(&working_dir);
    let context = read_code_sign_context()?;
    let ingress_token = read_session_ingress_token()?;
    let bytes = tokio::fs::read(&file_path)
        .await
        .map_err(|error| format!("cannot open file {}: {error}", file_path.display()))?;
    let content = String::from_utf8_lossy(&bytes).into_owned();
    if content.is_empty() {
        return Err("content to sign cannot be empty".to_owned());
    }
    let source = context.source_for_repository(&working_dir);
    let signature = request_signature(
        &context,
        &ingress_token,
        RemoteSignRequest {
            contents: content,
            source,
            git_object_format: git_object_format.to_owned(),
        },
    )
    .await?;
    let signature_path = PathBuf::from(format!("{}.sig", file_path.to_string_lossy()));
    write_signature(&signature_path, signature.as_bytes()).await
}

/// 发送给远端签名 API 的稳定 JSON body。
#[derive(Debug, Serialize)]
struct RemoteSignRequest {
    contents: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<Value>,
    #[serde(skip_serializing_if = "String::is_empty")]
    git_object_format: String,
}

/// 远端签名 API 的成功响应。
#[derive(Debug, Deserialize)]
struct RemoteSignResponse {
    #[serde(default)]
    signature: String,
}

/// 从 Claude child 的可信环境项读取直接签名上下文。
pub(super) fn read_code_sign_context() -> Result<CodeSignContext, String> {
    let raw = std::env::var(CODESIGN_CONTEXT_ENV)
        .map_err(|_| format!("code-sign context not found in {CODESIGN_CONTEXT_ENV}"))?;
    serde_json::from_str(&raw).map_err(|error| format!("invalid code-sign context: {error}"))
}

/// 从 executor 创建的 0600 文件读取 session ingress token。
pub(super) fn read_session_ingress_token() -> Result<String, String> {
    let path = std::env::var_os(SESSION_INGRESS_TOKEN_FILE_ENV)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            format!("session ingress token file not found in {SESSION_INGRESS_TOKEN_FILE_ENV}")
        })?;
    let token = std::fs::read_to_string(&path).map_err(|error| {
        format!(
            "failed to read session ingress token file {}: {error}",
            PathBuf::from(&path).display()
        )
    })?;
    if token.is_empty() {
        return Err("session ingress token is required for authentication".to_owned());
    }
    Ok(token)
}

///
/// 在工作目录执行 `git config --get extensions.objectFormat`；仅精确的 `sha256` 选择
/// SHA-256，命令不存在、退出失败、非 UTF-8 输出和所有其他值都回退 Git 默认 SHA-1。
pub(super) fn get_git_object_format(working_dir: &std::path::Path) -> &'static str {
    if working_dir.as_os_str().is_empty() {
        return "sha1";
    }
    let output = std::process::Command::new("git")
        .args(["config", "--get", "extensions.objectFormat"])
        .current_dir(working_dir)
        .output();
    match output {
        Ok(output)
            if output.status.success()
                && String::from_utf8_lossy(&output.stdout).trim() == "sha256" =>
        {
            "sha256"
        }
        _ => "sha1",
    }
}

///
/// 使用 session ingress token 调用一次远端签名接口并验证签名响应。
async fn request_signature(
    context: &CodeSignContext,
    ingress_token: &str,
    request: RemoteSignRequest,
) -> Result<String, String> {
    let endpoint = context.endpoint()?;
    let client = reqwest::Client::new();
    let mut request_builder = client
        .post(&endpoint)
        .header(CONTENT_TYPE, "application/json")
        .header("anthropic-version", "2023-06-01")
        .header("x-environment-runner-version", version())
        .header(AUTHORIZATION, format!("Bearer {ingress_token}"))
        .json(&request);
    if let Some(claude_version) = claude_code_version().filter(|value| !value.is_empty()) {
        request_builder = request_builder.header("x-claude-code-version", claude_version);
    }
    let response = tokio::time::timeout(Duration::from_secs(30), request_builder.send())
        .await
        .map_err(|_| "failed to call signing server: response header timeout".to_owned())?
        .map_err(|error| format!("failed to call signing server: {error}"))?;
    let status = response.status();
    let response_body = response
        .bytes()
        .await
        .map_err(|error| format!("failed to read signing response: {error}"))?;
    if status != reqwest::StatusCode::OK {
        return Err(format!(
            "signing server returned status {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&response_body)
        ));
    }
    let response: RemoteSignResponse = serde_json::from_slice(&response_body)
        .map_err(|error| format!("failed to parse signing response: {error}"))?;
    if response.signature.is_empty() {
        return Err("no signature returned from server".to_owned());
    }
    Ok(response.signature)
}

/// 创建或截断 Git 约定的 `.sig` 文件，并在返回前 flush 完整签名。
async fn write_signature(path: &std::path::Path, signature: &[u8]) -> Result<(), String> {
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true).mode(0o600);
    let mut output = options
        .open(path)
        .await
        .map_err(|error| format!("failed to write signature to {}: {error}", path.display()))?;
    output
        .write_all(signature)
        .await
        .map_err(|error| format!("failed to write signature to {}: {error}", path.display()))?;
    output
        .flush()
        .await
        .map_err(|error| format!("failed to write signature to {}: {error}", path.display()))
}
