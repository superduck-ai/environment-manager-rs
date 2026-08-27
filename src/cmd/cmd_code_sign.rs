//! 隐藏签名包装命令。
//!
//! Git 的 `gpg.ssh.program` 会传入 OpenSSH 的 `-Y`、
//! `-f`、`-n` 等值必须原样保留。本文件还实现配置发现、Git object format 探测、
//! loopback MCP JSON-RPC、指数退避以及 `.sig` 文件副作用校验。

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::builder::OsStringValueParser;
use clap::{Arg, ArgMatches, Command};
use rand::Rng as _;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{Value, json};

use super::error::CliError;
use super::types::{
    CodeSignConfig, McpRequest, McpResponse, McpToolCallParams, McpToolResult, RetryConfig,
};

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
    /// 相对路径的绝对化、配置读取与 MCP 调用留给执行层完成。
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
        .about("Act as a GPG/SSH signing wrapper using the MCP codesign server")
        .long_about("This command acts as a GPG/SSH signing wrapper that invokes the MCP \ncodesign server for signing operations. It's designed to be used with git's \ngpg.ssh.program configuration.")
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
/// attempt 为零时直接返回初始值；其余轮次先按指数因子计算并截断到最大值，再添加
/// `[0%, 30%)` 的正抖动。最大值只约束抖动前基础值，这是 的刻意行为。
pub(super) fn calculate_backoff(attempt: usize, config: RetryConfig) -> Duration {
    if attempt == 0 {
        return config.initial_backoff;
    }
    let initial = config.initial_backoff.as_secs_f64();
    let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
    let base =
        (initial * config.backoff_factor.powi(exponent)).min(config.max_backoff.as_secs_f64());
    let jitter = rand::rng().random_range(0.0_f64..1.0) * 0.3 * base;
    Duration::from_secs_f64(base + jitter)
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
/// 将文件路径绝对化，探测仓库 object format，从环境或临时文件读取 MCP 配置，调用
/// `sign_file` 工具并确认服务端创建了同名 `.sig` 文件。`-n` 与 `-f` 仅打印调试
/// 提示，继续由服务端选择实际签名 key。
///
/// # 副作用
/// 读取工作目录和配置、执行 `git config`、访问 loopback HTTP server、读取签名文件
/// 元数据并写 stderr。HTTP 调用和重试不受外层命令取消影响，只受 10 秒
/// 客户端超时约束。
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
    let config = read_code_sign_config()
        .map_err(|error| format!("failed to read codesign server configuration: {error}"))?;
    let request_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let request = McpRequest {
        jsonrpc: "2.0".to_owned(),
        id: format!("sign-{request_id}"),
        method: "tools/call".to_owned(),
        params: McpToolCallParams {
            name: "sign_file".to_owned(),
            arguments: BTreeMap::from([
                ("file_path".to_owned(), json!(file_path.to_string_lossy())),
                (
                    "repo_directory".to_owned(),
                    json!(working_dir.to_string_lossy()),
                ),
                ("git_object_format".to_owned(), json!(git_object_format)),
            ]),
        },
    };
    let result_raw = call_mcp_server(&config, &request)
        .await
        .map_err(|error| format!("failed to call MCP server: {error}"))?;
    let tool_result: McpToolResult = serde_json::from_slice(&result_raw)
        .map_err(|error| format!("failed to parse tool result: {error}"))?;
    if tool_result.is_error {
        let message = tool_result
            .content
            .first()
            .and_then(|content| {
                // 类型字段不参与错误文案，但读取它可明确表明未知 MCP content 类型也
                // 仍按 使用首个非空 text，而不是拒绝整个响应。
                let _content_type = &content.r#type;
                (!content.text.is_empty()).then_some(content.text.as_str())
            })
            .unwrap_or("unknown error");
        return Err(format!("signing failed: {message}"));
    }

    let signature_path = PathBuf::from(format!("{}.sig", file_path.to_string_lossy()));
    match std::fs::metadata(&signature_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(format!(
            "signature file not created: {}",
            signature_path.display()
        )),
        // 只针对 not-found 判定 失败；权限等其他 stat 错误会被忽略，不能在错误处理时
        // 擅自收紧为“所有 metadata 错误都失败”。
        _ => Ok(()),
    }
}

///
/// 先尝试同时存在的 `CODESIGN_MCP_PORT`/`CODESIGN_MCP_TOKEN`；端口不是正整数时静默
/// 回退 `/tmp/codesign-mcp-config.json`。文件缺失、JSON 无效或字段不完整统一折叠为
/// “configuration not found”，避免泄露 token 或产生来源相关分支。
pub(super) fn read_code_sign_config() -> Result<CodeSignConfig, String> {
    const CONFIG_PATH: &str = "/tmp/codesign-mcp-config.json";
    let port_env = std::env::var("CODESIGN_MCP_PORT").unwrap_or_default();
    let token_env = std::env::var("CODESIGN_MCP_TOKEN").unwrap_or_default();
    if !port_env.is_empty()
        && !token_env.is_empty()
        && let Some(port) = scan_positive_integer(&port_env)
    {
        return Ok(CodeSignConfig {
            port,
            bearer_token: token_env,
        });
    }

    if let Ok(data) = std::fs::read(CONFIG_PATH)
        && let Ok(config) = serde_json::from_slice::<CodeSignConfig>(&data)
        && config.port > 0
        && !config.bearer_token.is_empty()
    {
        return Ok(config);
    }
    Err(format!(
        "codesign server configuration not found: checked CODESIGN_MCP_PORT/CODESIGN_MCP_TOKEN env vars and {CONFIG_PATH}"
    ))
}

/// 读取首个有符号十进制整数并要求其大于零；尾随非数字内容不影响结果。
pub(super) fn scan_positive_integer(value: &str) -> Option<i64> {
    let value = value.trim_start();
    let mut end = 0;
    for (index, character) in value.char_indices() {
        if index == 0 && matches!(character, '+' | '-') {
            end = character.len_utf8();
            continue;
        }
        if character.is_ascii_digit() {
            end = index + character.len_utf8();
        } else {
            break;
        }
    }
    let parsed = value.get(..end)?.parse::<i64>().ok()?;
    (parsed > 0).then_some(parsed)
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
/// 发送一次带 JSON content type 和 Bearer token 的 POST，请求成功必须同时满足 HTTP
/// 200、合法 JSON-RPC response、没有 error 且存在 result。JSON-RPC 允许
/// `result: null`，因此该值仍区别于字段缺失并原样返回。
pub(super) async fn do_single_attempt(
    client: &reqwest::Client,
    url: &str,
    request_data: &[u8],
    bearer_token: &str,
) -> Result<Vec<u8>, String> {
    let request = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .header(AUTHORIZATION, format!("Bearer {bearer_token}"))
        .body(request_data.to_vec())
        .build()
        .map_err(|error| format!("failed to create request: {error}"))?;
    let response = client
        .execute(request)
        .await
        .map_err(|error| format!("failed to send request: {error}"))?;
    let status = response.status();
    let response_body = response
        .bytes()
        .await
        .map_err(|error| format!("failed to read response: {error}"))?;
    if status != reqwest::StatusCode::OK {
        return Err(format!(
            "MCP server returned status {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&response_body)
        ));
    }

    let value: Value = serde_json::from_slice(&response_body)
        .map_err(|error| format!("failed to parse response: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "failed to parse response: expected a JSON object".to_owned())?;
    let response: McpResponse = serde_json::from_value(value.clone())
        .map_err(|error| format!("failed to parse response: {error}"))?;
    // jsonrpc、id 和 typed result 不参与 控制流，但读取后保留类型作为协议审计点。
    let _protocol_fields = (&response.jsonrpc, &response.id, &response.result);
    if let Some(error) = response.error {
        let _error_code = error.code;
        return Err(format!("MCP error: {}", error.message));
    }
    let result = object
        .get("result")
        .ok_or_else(|| "no result in MCP response".to_owned())?;
    serde_json::to_vec(result).map_err(|error| format!("failed to parse response: {error}"))
}

///
/// 总尝试次数为 `max_retries + 1`。每次失败在 stderr 打印当前尝试、总次数、带抖动
/// 的等待和原始错误；最后一次失败不再 sleep，并用“after N retries”包装最后原因。
pub(super) async fn do_attempts_with_retry(
    client: &reqwest::Client,
    url: &str,
    request_data: &[u8],
    bearer_token: &str,
    retry_config: RetryConfig,
) -> Result<Vec<u8>, String> {
    let mut last_error = String::new();
    for attempt in 0..=retry_config.max_retries {
        match do_single_attempt(client, url, request_data, bearer_token).await {
            Ok(result) => return Ok(result),
            Err(error) => last_error = error,
        }
        if attempt >= retry_config.max_retries {
            continue;
        }
        let backoff = calculate_backoff(attempt + 1, retry_config);
        eprintln!(
            "MCP server request failed (attempt {}/{}), retrying in {:?}: {}",
            attempt + 1,
            retry_config.max_retries + 1,
            backoff,
            last_error
        );
        tokio::time::sleep(backoff).await;
    }
    Err(format!(
        "MCP server request failed after {} retries: {}",
        retry_config.max_retries, last_error
    ))
}

///
/// 将请求编码为紧凑 JSON，创建 10 秒总超时客户端，并访问固定 loopback `/mcp`
/// 路径。默认最多重试两次，基础退避 100ms、上限 2s、指数因子 2。
pub(super) async fn call_mcp_server(
    config: &CodeSignConfig,
    request: &McpRequest,
) -> Result<Vec<u8>, String> {
    let request_data = serde_json::to_vec(request)
        .map_err(|error| format!("failed to serialize request: {error}"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| format!("failed to create request: {error}"))?;
    let url = format!("http://127.0.0.1:{}/mcp", config.port);
    let retry_config = RetryConfig {
        max_retries: 2,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(2),
        backoff_factor: 2.0,
    };
    do_attempts_with_retry(
        &client,
        &url,
        &request_data,
        &config.bearer_token,
        retry_config,
    )
    .await
}
