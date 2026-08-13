//! 单次取工作命令。
//!
//! 本文件完整保留参数、service key 来源、whoami 身份发现/回退、poll 请求以及
//! stdout JSON 输出顺序。网络细节复用 `internal::orchestrator` 的同源客户端。

use std::io::Write;
use std::path::PathBuf;

use clap::builder::ValueParser;
use clap::{Arg, ArgMatches, Command, value_parser};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::error::CliError;
use super::utils::{LogLevel, parse_log_level, tracing_level};
use crate::internal::orchestrator::{new_poller_with_worker_id, new_whoami_client};

const LONG_ABOUT: &str = r#"The poll command makes a single non-blocking request to the API for work.

This is a thin wrapper around the poll endpoint that returns work JSON on stdout
if work is available, or "null" if the queue is empty.

The worker_id parameter enables per-worker IDs for better observability
and faster reclaim of unacknowledged work. If not provided, it defaults to the
system hostname. The worker ID is sent via the Anthropic-Worker-ID HTTP header.

Required environment variable:
  ENVIRONMENT_SERVICE_KEY: Service key for the environment

The environment ID and organization ID are discovered automatically via the whoami
endpoint. You can optionally provide --environment-id and --organization-id flags
to validate them against the token's identity."#;

const EXAMPLES: &str = r#"Examples:
  environment-runner poll
  environment-runner poll --worker-id "worker-$(hostname)-$$"
  environment-runner poll --service-key-file /path/to/service-key"#;

/// 已验证的单次 poll 参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollArgs {
    /// Anthropic API 基地址。
    pub api_url: String,
    /// 可选组织 ID，用于 whoami 回退或一致性校验。
    pub organization_id: Option<String>,
    /// 可选环境 ID，用于 whoami 回退或一致性校验。
    pub environment_id: Option<String>,
    /// 可选工作器 ID；未提供时运行时使用 hostname。
    pub worker_id: Option<String>,
    /// 可选 service key 文件路径。
    pub service_key_file: Option<PathBuf>,
    /// 回收未 ACK 工作的年龄阈值，单位毫秒。
    pub reclaim_older_than_ms: i64,
    /// 日志级别。
    pub log_level: LogLevel,
}

/// 构造 `poll` 子命令及全部 flag。
pub(crate) fn add_poll_command() -> Command {
    Command::new("poll")
        .about("Make a single poll request for work")
        .long_about(LONG_ABOUT)
        .after_long_help(EXAMPLES)
        .arg(Arg::new("api-url").long("api-url").default_value("https://api.anthropic.com").help("API base URL"))
        .arg(Arg::new("organization-id").long("organization-id").help("Organization ID (find at claude.ai/settings > Account)"))
        .arg(Arg::new("environment-id").long("environment-id").help("Environment ID (find at claude.ai/settings, e.g., env_01ABC123)"))
        .arg(Arg::new("worker-id").long("worker-id").help("Unique worker identifier for this worker. Sent via Anthropic-Worker-ID header. If not set, defaults to system hostname"))
        .arg(Arg::new("service-key-file").long("service-key-file").help("Path to file containing the environment service key. If not set, falls back to ENVIRONMENT_SERVICE_KEY environment variable").value_parser(value_parser!(PathBuf)))
        .arg(Arg::new("reclaim-older-than-ms").long("reclaim-older-than-ms").default_value("0").help("Reclaim unacknowledged work items older than this many milliseconds (0 = use API default of 5000ms)").allow_hyphen_values(true).value_parser(value_parser!(i64)))
        .arg(Arg::new("log-level").long("log-level").default_value("info").help("Log level (debug, info, warn, error)").value_parser(ValueParser::new(parse_log_level)))
}

/// 将 Clap matches 转换为运行时独立的 poll 参数。
pub(crate) fn from_matches(matches: &ArgMatches) -> Result<PollArgs, CliError> {
    Ok(PollArgs {
        api_url: matches
            .get_one::<String>("api-url")
            .expect("defaulted api-url")
            .clone(),
        organization_id: optional_string(matches, "organization-id"),
        environment_id: optional_string(matches, "environment-id"),
        worker_id: optional_string(matches, "worker-id"),
        service_key_file: matches.get_one::<PathBuf>("service-key-file").cloned(),
        reclaim_older_than_ms: *matches
            .get_one::<i64>("reclaim-older-than-ms")
            .expect("defaulted reclaim age"),
        log_level: *matches
            .get_one::<LogLevel>("log-level")
            .expect("defaulted log level"),
    })
}

/// 把可选字符串参数转换为 owned 值。
fn optional_string(matches: &ArgMatches, id: &str) -> Option<String> {
    matches
        .get_one::<String>(id)
        .filter(|value| !value.is_empty())
        .cloned()
}

/// `RunE` 实现。
///
/// 读取 service key 后先调用 whoami。whoami 失败时，只有调用方同时提供组织和环境
/// ID 才允许继续；成功时则逐项校验显式 ID。随后执行一次非阻塞 poll，并把空队列
/// 输出为 `null\n`，把 work 输出为两空格缩进 JSON。
///
/// # 参数
/// - `arguments`：已经通过 Clap 校验的命令参数。
/// - `cancellation`：覆盖 whoami 和 poll 两次 HTTP 请求的取消令牌。
/// - `output`：stdout 输出目标；写失败不会覆盖 poll 结果。
///
/// # 副作用
/// 读取文件或 `ENVIRONMENT_SERVICE_KEY`、追加日志文件并访问后端 HTTP 接口。
pub async fn run_poll(
    arguments: &PollArgs,
    cancellation: &CancellationToken,
    output: &mut dyn Write,
) -> Result<(), String> {
    crate::internal::logger::initialize(tracing_level(arguments.log_level));
    let service_key = if let Some(path) = &arguments.service_key_file {
        let bytes = std::fs::read(path).map_err(|error| {
            format!(
                "failed to read service key file {:?}: {error}",
                path.display().to_string()
            )
        })?;
        String::from_utf8_lossy(&bytes).trim().to_owned()
    } else {
        std::env::var("ENVIRONMENT_SERVICE_KEY").unwrap_or_default()
    };

    if service_key.is_empty() {
        return Err(
            "service key is required: use --service-key-file or set ENVIRONMENT_SERVICE_KEY environment variable\n\nSet this environment variable before running poll:\n  export ENVIRONMENT_SERVICE_KEY=\"your-environment-service-key\"\n\nOr use --service-key-file to read from a file:\n  --service-key-file /path/to/service-key\n\nYou can create and manage environment service keys at claude.ai/settings\nunder your environment's settings"
                .to_owned(),
        );
    }

    let mut organization_id = arguments.organization_id.clone().unwrap_or_default();
    let mut environment_id = arguments.environment_id.clone().unwrap_or_default();
    let whoami = new_whoami_client(&arguments.api_url, &service_key);
    match whoami.get_identity(cancellation).await {
        Ok(identity) => {
            if !organization_id.is_empty() && organization_id != identity.organization_uuid {
                return Err(format!(
                    "provided --organization-id {:?} does not match token's organization {:?}",
                    organization_id, identity.organization_uuid
                ));
            }
            if !environment_id.is_empty() && environment_id != identity.environment_id {
                return Err(format!(
                    "provided --environment-id {:?} does not match token's environment {:?}",
                    environment_id, identity.environment_id
                ));
            }
            organization_id = identity.organization_uuid;
            environment_id = identity.environment_id;
            tracing::info!("organization_id" = ?(organization_id), "environment_id" = ?(environment_id), "{}", "environment identity confirmed via whoami");
        }
        Err(error) => {
            if organization_id.is_empty() || environment_id.is_empty() {
                return Err(format!(
                    "failed to get environment identity from whoami (provide --organization-id and --environment-id to use fallback): {error}"
                ));
            }
            tracing::warn!("error" = ?(error.to_string()), "organization_id" = ?(organization_id), "environment_id" = ?(environment_id), "{}", "whoami failed, using CLI-provided identity");
        }
    }

    let poller = new_poller_with_worker_id(
        &arguments.api_url,
        environment_id,
        service_key,
        arguments.worker_id.clone().unwrap_or_default(),
        arguments.reclaim_older_than_ms,
    );
    let Some(work_json) = poller
        .poll(cancellation)
        .await
        .map_err(|error| format!("poll request failed: {error}"))?
    else {
        let _ = output.write_all(b"null\n");
        return Ok(());
    };

    // Poller 已经验证 work envelope；这里重新解码只为输出人类可读的缩进 JSON。
    let payload: Value = match serde_json::from_slice(&work_json) {
        Ok(payload) => payload,
        Err(_) => {
            let _ = output.write_all(&work_json);
            let _ = output.write_all(b"\n");
            return Ok(());
        }
    };
    match serde_json::to_vec_pretty(&payload) {
        Ok(pretty) => {
            let _ = output.write_all(&pretty);
            let _ = output.write_all(b"\n");
        }
        Err(_) => {
            let _ = output.write_all(&work_json);
            let _ = output.write_all(b"\n");
        }
    }
    Ok(())
}
