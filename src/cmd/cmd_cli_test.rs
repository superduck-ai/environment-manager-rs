//! 命令面兼容测试。
//!
//! 这些测试保留 Go `cmd/cmd_cli_test.go` 的精确测试名，并补充迁移最容易回归的
//! argv0、默认值、显式 flag 来源和 Cobra 输出约定。HTTP 路径使用 loopback mock
//! server；安装路径只验证 skip/current 与 PATH 预检，不访问公网或真实安装 npm 包。

#![allow(non_snake_case, reason = "保留 Go cmd_cli_test.go 的精确测试名称")]

use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{body_partial_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use super::code_sign::{
    calculate_backoff, do_attempts_with_retry, do_single_attempt, read_code_sign_config,
    scan_positive_integer,
};
use super::orchestrator::orchestrator_configured_attributes;
use super::setup::{
    check_npm_available, install_sandbox_runtime, load_service_key, run_api_healthcheck,
};
use super::utils::mark_required_flags;
use super::zz_recovered_types::RetryConfig;
use super::*;
use crate::internal::orchestrator::default_orchestrator_logger;

/// 根命令应注册全部公开命令，并从普通帮助中隐藏 code-sign。
#[test]
fn root_command_exposes_expected_surface() {
    let root = command("1.2.3");
    let names: Vec<_> = root
        .get_subcommands()
        .map(clap::Command::get_name)
        .collect();
    assert_eq!(
        names,
        [
            "completion",
            "orchestrator",
            "task-run",
            "provision-packages",
            "poll",
            "print-sandbox-settings",
            "setup",
            "code-sign",
        ]
    );

    let help = command("1.2.3").render_long_help().to_string();
    assert!(help.contains("Environment Runner handles the lifecycle of agentic sessions"));
    assert!(help.contains("orchestrator"));
    assert!(help.contains("task-run"));
    assert!(help.contains("completion"));
    assert!(!help.contains("code-sign"));
}

/// Cobra 自动注入的 completion 命令应解析 shell 与说明开关。
#[test]
fn completion_command_is_part_of_the_compatibility_surface() {
    let Invocation::Completion(args) = parse_from(
        "dev",
        [
            "environment-runner",
            "completion",
            "zsh",
            "--no-descriptions",
        ],
    )
    .expect("completion command must parse") else {
        panic!("unexpected invocation");
    };
    assert_eq!(args.shell, CompletionShell::Zsh);
    assert!(args.no_descriptions);
}

/// 仅运行根命令应像 Cobra 一样打印帮助并成功退出。
#[test]
fn root_without_subcommand_is_successful_help_exit() {
    let error = parse_from("dev", ["environment-runner"]).expect_err("root must render help");
    assert_eq!(error.kind(), CliErrorKind::Help);
    assert_eq!(error.exit_code(), 0);
    assert_eq!(error.stream(), OutputStream::Stdout);
    assert!(error.rendered().contains("Usage:"));
}

/// 版本 flag 应保留 Clap 的正常文本和零退出码。
#[test]
fn version_is_successful_early_exit() {
    let error = parse_from("9.8.7", ["environment-runner", "--version"])
        .expect_err("version must be an early exit");
    assert_eq!(error.kind(), CliErrorKind::Version);
    assert_eq!(error.exit_code(), 0);
    assert_eq!(error.stream(), OutputStream::Stdout);
    assert!(error.rendered().contains("9.8.7"));

    let short = parse_from("9.8.7", ["environment-runner", "-v"])
        .expect_err("Cobra's short version flag must be an early exit");
    assert_eq!(short.kind(), CliErrorKind::Version);
    assert_eq!(short.exit_code(), 0);
}

/// Cobra bool flag 的显式 `=false` 语法不应因迁移到 Clap 而丢失。
#[test]
fn cobra_boolean_flags_accept_explicit_false() {
    let Invocation::Setup(args) = parse_from(
        "dev",
        [
            "environment-runner",
            "setup",
            "--skip-claude-code=false",
            "--skip-sandbox-runtime=false",
        ],
    )
    .expect("explicit false booleans must parse") else {
        panic!("unexpected invocation");
    };
    assert!(!args.skip_claude_code);
    assert!(!args.skip_sandbox_runtime);
}

/// orchestrator 默认值必须逐项对应 Go 命令。
#[test]
fn orchestrator_defaults_match_go_command() {
    let Invocation::Orchestrator(args) = parse_from("dev", ["environment-runner", "orchestrator"])
        .expect("orchestrator defaults must parse")
    else {
        panic!("unexpected invocation");
    };

    assert_eq!(args.api_url, "https://api.anthropic.com");
    assert_eq!(args.poll_timeout, Duration::from_secs(300));
    assert_eq!(args.loop_timeout, Duration::from_secs(300));
    assert_eq!(args.timeout_hook_timeout, Duration::from_secs(300));
    assert_eq!(args.execute_hook_timeout, Duration::ZERO);
    assert_eq!(args.poll_hook_timeout, Duration::from_secs(30));
    assert_eq!(args.log_level, LogLevel::Info);
    assert_eq!(args.sandbox_backend, SandboxBackend::SandboxRuntime);
    assert_eq!(args.max_poll_failures, 0);
    assert_eq!(args.reclaim_older_than_ms, 0);
    assert!(!args.skip_git_config);
    assert!(!args.skip_container_lock);

    let configured = orchestrator_configured_attributes(&args);
    assert_eq!(
        configured.get("poll_timeout"),
        Some(&json!(300_000_000_000_i64))
    );
    assert_eq!(
        configured.get("loop_timeout"),
        Some(&json!(300_000_000_000_i64))
    );
    assert!(configured["poll_timeout"].is_i64());
    assert!(configured["loop_timeout"].is_i64());
}

/// orchestrator 的路径、hook、整数、duration 和 bool flag 应完整落入类型模型。
#[test]
fn orchestrator_overrides_are_typed() {
    let Invocation::Orchestrator(args) = parse_from(
        "dev",
        [
            "environment-runner",
            "orchestrator",
            "--api-url",
            "http://localhost:8080",
            "--organization-id",
            "org-1",
            "--environment-id",
            "env-1",
            "--client-id",
            "worker-1",
            "--poll-timeout",
            "1h30m",
            "--loop-timeout",
            "1.5s",
            "--max-poll-failures",
            "-1",
            "--reclaim-older-than-ms",
            "123",
            "--execute-hook",
            "./run.sh",
            "--sandbox-backend",
            "none",
            "--sandbox-settings",
            "/tmp/sandbox.json",
            "--service-key-file",
            "/tmp/key",
            "--skip-git-config",
            "--skip-container-lock",
        ],
    )
    .expect("orchestrator overrides must parse") else {
        panic!("unexpected invocation");
    };

    assert_eq!(args.api_url, "http://localhost:8080");
    assert_eq!(args.organization_id.as_deref(), Some("org-1"));
    assert_eq!(args.environment_id.as_deref(), Some("env-1"));
    assert_eq!(args.client_id.as_deref(), Some("worker-1"));
    assert_eq!(args.poll_timeout, Duration::from_secs(5_400));
    assert_eq!(args.loop_timeout, Duration::from_millis(1_500));
    assert_eq!(args.max_poll_failures, -1);
    assert_eq!(args.reclaim_older_than_ms, 123);
    assert_eq!(args.execute_hook.as_deref(), Some("./run.sh"));
    assert_eq!(args.sandbox_backend, SandboxBackend::None);
    assert_eq!(
        args.sandbox_settings,
        Some(PathBuf::from("/tmp/sandbox.json"))
    );
    assert_eq!(args.service_key_file, Some(PathBuf::from("/tmp/key")));
    assert!(args.skip_git_config);
    assert!(args.skip_container_lock);
}

/// poll 与 setup 的默认值和主要覆盖值应保持 Go 接口。
#[test]
fn poll_and_setup_parse_without_runtime_side_effects() {
    let Invocation::Poll(poll) = parse_from(
        "dev",
        [
            "environment-runner",
            "poll",
            "--worker-id",
            "worker-1",
            "--service-key-file",
            "/tmp/key",
            "--reclaim-older-than-ms",
            "123",
        ],
    )
    .expect("poll must parse") else {
        panic!("unexpected invocation");
    };
    assert_eq!(poll.worker_id.as_deref(), Some("worker-1"));
    assert_eq!(poll.service_key_file, Some(PathBuf::from("/tmp/key")));
    assert_eq!(poll.reclaim_older_than_ms, 123);

    let Invocation::Setup(setup) = parse_from(
        "dev",
        [
            "environment-runner",
            "setup",
            "--claude-code-version",
            "2.0.20",
            "--skip-sandbox-runtime",
            "--log-level",
            "debug",
        ],
    )
    .expect("setup must parse") else {
        panic!("unexpected invocation");
    };
    assert_eq!(setup.claude_code_version, "2.0.20");
    assert_eq!(setup.sandbox_runtime_version, "latest");
    assert!(setup.skip_sandbox_runtime);
    assert_eq!(setup.log_level, LogLevel::Debug);
}

/// argv0 basename 为 code-sign 时应在其后准确插入隐藏子命令一次。
#[test]
fn code_sign_argv0_is_normalized() {
    let normalized = normalize_argv_for_codesign([
        OsString::from("/usr/local/bin/code-sign"),
        OsString::from("-Y"),
        OsString::from("sign"),
        OsString::from("payload"),
    ]);
    assert_eq!(
        normalized,
        [
            OsString::from("/usr/local/bin/code-sign"),
            OsString::from("code-sign"),
            OsString::from("-Y"),
            OsString::from("sign"),
            OsString::from("payload"),
        ]
    );
}

/// code-sign 应禁用 help/flag 解析并按旧规则提取 SSH sign 参数。
#[test]
fn code_sign_preserves_and_interprets_raw_arguments() {
    let Invocation::CodeSign(args) = parse_from(
        "dev",
        [
            "/usr/local/bin/code-sign",
            "-Y",
            "sign",
            "-n",
            "git",
            "-f",
            "/tmp/key.pub",
            "payload",
        ],
    )
    .expect("argv0 code-sign must parse") else {
        panic!("unexpected invocation");
    };
    assert_eq!(args.raw_args.first(), Some(&OsString::from("-Y")));

    let CodeSignOperation::SshSign(sign) = args
        .parse_operation()
        .expect("SSH sign operation must be recognized");
    assert_eq!(sign.file_path, PathBuf::from("payload"));
    assert_eq!(sign.key_file, Some(PathBuf::from("/tmp/key.pub")));
    assert_eq!(sign.namespace, Some(OsString::from("git")));
}

/// 非 SSH sign 调用应保留旧实现的明确失败，而不是形成伪成功操作。
#[test]
fn code_sign_rejects_unsupported_operation() {
    let args = CodeSignArgs {
        raw_args: vec![OsString::from("--help")],
    };
    let error = args
        .parse_operation()
        .expect_err("unsupported operation must fail");
    assert!(error.contains("currently only SSH-style signing"));
}

/// Go duration 解析器应覆盖默认值、组合、小数和微秒拼写。
#[test]
fn go_duration_parser_supports_legacy_syntax() {
    assert_eq!(parse_go_duration("0").unwrap(), Duration::ZERO);
    assert_eq!(
        parse_go_duration("500ms").unwrap(),
        Duration::from_millis(500)
    );
    assert_eq!(
        parse_go_duration("1.5s").unwrap(),
        Duration::from_millis(1_500)
    );
    assert_eq!(
        parse_go_duration("1h30m").unwrap(),
        Duration::from_secs(5_400)
    );
    assert_eq!(parse_go_duration("2µs").unwrap(), Duration::from_micros(2));
    assert_eq!(parse_go_duration("+2s").unwrap(), Duration::from_secs(2));
    assert_eq!(parse_go_duration("0.5ns").unwrap(), Duration::ZERO);
    assert!(parse_go_duration("5").is_err());
    assert!(parse_go_duration("-1s").is_err());
}

/// 日志级别应严格接受四个旧值并返回兼容错误。
#[test]
fn log_level_parser_matches_go_values() {
    assert_eq!(parse_log_level("debug"), Ok(LogLevel::Debug));
    assert_eq!(parse_log_level("info"), Ok(LogLevel::Info));
    assert_eq!(parse_log_level("warn"), Ok(LogLevel::Warn));
    assert_eq!(parse_log_level("error"), Ok(LogLevel::Error));
    let error = parse_log_level("verbose").expect_err("verbose is unsupported");
    assert!(error.contains("invalid log level"));
}

/// 未知 flag 应保留 usage 提示，但首行使用 Cobra 的 `Error:` 大小写。
#[test]
fn unknown_flag_uses_cobra_error_prefix() {
    let error = parse_from(
        "dev",
        ["environment-runner", "poll", "--definitely-unknown"],
    )
    .expect_err("unknown flag must fail");
    assert_eq!(error.kind(), CliErrorKind::Usage);
    assert_eq!(error.exit_code(), 1);
    assert!(error.rendered().starts_with("Error:"));
    assert!(error.rendered().contains("Usage:"));
}

/// Cobra 应为编辑距离不超过二的未知根命令输出建议，并在两次错误中重复建议块。
#[test]
fn close_unknown_command_uses_cobra_suggestion_transcript() {
    let error = parse_from("dev", ["environment-runner", "pll"])
        .expect_err("nearby root command must fail with a suggestion");

    assert_eq!(error.kind(), CliErrorKind::Usage);
    assert_eq!(error.exit_code(), 1);
    assert_eq!(
        error.rendered(),
        concat!(
            "Error: unknown command \"pll\" for \"environment-runner\"\n\n",
            "Did you mean this?\n",
            "\tpoll\n\n",
            "Run 'environment-runner --help' for usage.\n",
            "Error: unknown command \"pll\" for \"environment-runner\"\n\n",
            "Did you mean this?\n",
            "\tpoll\n\n",
        )
    );
}

/// Cobra 的前缀建议应使用 Go 注册顺序，并排除隐藏的 `code-sign` 命令。
#[test]
fn root_command_suggestions_preserve_cobra_filtering_and_order() {
    let prefix = parse_from("dev", ["environment-runner", "p"])
        .expect_err("ambiguous prefix must remain an unknown command");
    let rendered = prefix.rendered();
    let first_block = concat!(
        "Did you mean this?\n",
        "\tprovision-packages\n",
        "\tpoll\n",
        "\tprint-sandbox-settings\n",
    );
    assert_eq!(rendered.matches(first_block).count(), 2);

    let hidden = parse_from("dev", ["environment-runner", "code-sig"])
        .expect_err("hidden code-sign typo must remain unknown");
    assert!(
        !hidden.rendered().contains("Did you mean this?"),
        "Cobra IsAvailableCommand excludes hidden commands from suggestions"
    );

    let help = parse_from("dev", ["environment-runner", "hlp"])
        .expect_err("default help command typo must remain unknown");
    assert!(
        !help.rendered().contains("Did you mean this?"),
        "Cobra treats the generated help command as unavailable for suggestions"
    );

    let uppercase = parse_from("dev", ["environment-runner", "PLL"])
        .expect_err("command matching is case-sensitive before suggestions");
    assert_eq!(uppercase.rendered().matches("\tpoll\n").count(), 2);

    let unicode_simple_lowercase = parse_from("dev", ["environment-runner", "PİLL"])
        .expect_err("Unicode command matching remains case-sensitive");
    assert_eq!(
        unicode_simple_lowercase
            .rendered()
            .matches("\tpoll\n")
            .count(),
        2,
        "Go strings.ToLower uses simple rather than expanding lowercase mappings"
    );
}

/// Cobra 会让不可运行的 completion 父命令为未知 shell 打印帮助并以零退出。
#[test]
fn unknown_completion_shell_falls_back_to_parent_help() {
    let error = parse_from("dev", ["environment-runner", "completion", "bsh"])
        .expect_err("completion parent help is represented as a successful early exit");

    assert_eq!(error.kind(), CliErrorKind::Help);
    assert_eq!(error.exit_code(), 0);
    assert_eq!(error.rendered(), cobra_compat::render_completion_help(None));
}

/// 未知 completion shell 后仍有非法 flag 时，Cobra 应先报告父命令 flag 错误。
#[test]
fn unknown_completion_shell_with_flag_uses_completion_error() {
    let error = parse_from("dev", ["environment-runner", "completion", "bsh", "--bad"])
        .expect_err("unknown completion flag must fail");

    assert_eq!(error.kind(), CliErrorKind::Usage);
    assert_eq!(error.exit_code(), 1);
    assert_eq!(
        error.rendered(),
        cobra_compat::render_runtime_error(CobraCommand::Completion, "unknown flag: --bad")
    );

    let delimited = parse_from(
        "dev",
        ["environment-runner", "completion", "bsh", "--", "--bad"],
    )
    .expect_err("flags after -- remain completion parent arguments");
    assert_eq!(delimited.kind(), CliErrorKind::Help);
    assert_eq!(delimited.exit_code(), 0);
    assert_eq!(
        delimited.rendered(),
        cobra_compat::render_completion_help(None)
    );

    let shorthand = parse_from("dev", ["environment-runner", "completion", "bsh", "-x"])
        .expect_err("unknown shorthand must fail");
    assert_eq!(shorthand.exit_code(), 1);
    assert_eq!(
        shorthand.rendered(),
        cobra_compat::render_runtime_error(
            CobraCommand::Completion,
            "unknown shorthand flag: 'x' in -x",
        )
    );

    let help_false = parse_from(
        "dev",
        ["environment-runner", "completion", "bsh", "--help=false"],
    )
    .expect_err("non-runnable completion parent still prints help");
    assert_eq!(help_false.kind(), CliErrorKind::Help);
    assert_eq!(help_false.exit_code(), 0);
    assert_eq!(
        help_false.rendered(),
        cobra_compat::render_completion_help(None)
    );

    let lone_dash = parse_from("dev", ["environment-runner", "completion", "bsh", "-"])
        .expect_err("single dash remains a positional completion argument");
    assert_eq!(lone_dash.kind(), CliErrorKind::Help);
    assert_eq!(lone_dash.exit_code(), 0);

    let malformed_long = parse_from("dev", ["environment-runner", "completion", "bsh", "---bad"])
        .expect_err("three leading dashes are invalid pflag syntax");
    assert_eq!(
        malformed_long.rendered(),
        cobra_compat::render_runtime_error(CobraCommand::Completion, "bad flag syntax: ---bad",)
    );

    let attached_bool = parse_from(
        "dev",
        ["environment-runner", "completion", "bsh", "-hfalse"],
    )
    .expect_err("characters after the bool shorthand form another shorthand cluster");
    assert_eq!(
        attached_bool.rendered(),
        cobra_compat::render_runtime_error(
            CobraCommand::Completion,
            "unknown shorthand flag: 'f' in -false",
        )
    );

    let repeated_help = parse_from("dev", ["environment-runner", "completion", "bsh", "-hh"])
        .expect_err("repeating the bool help shorthand remains valid");
    assert_eq!(repeated_help.kind(), CliErrorKind::Help);
    assert_eq!(repeated_help.exit_code(), 0);

    let help_then_unknown = parse_from("dev", ["environment-runner", "completion", "bsh", "-hx"])
        .expect_err("pflag must report only the unconsumed shorthand suffix");
    assert_eq!(
        help_then_unknown.rendered(),
        cobra_compat::render_runtime_error(
            CobraCommand::Completion,
            "unknown shorthand flag: 'x' in -x",
        )
    );

    let empty_long_name = parse_from("dev", ["environment-runner", "completion", "bsh", "--=x"])
        .expect_err("a long flag cannot have an empty name");
    assert_eq!(
        empty_long_name.rendered(),
        cobra_compat::render_runtime_error(CobraCommand::Completion, "bad flag syntax: --=x")
    );
}

/// 无参数打印命令只产生显式调用标记，不在解析测试中写 stdout。
#[test]
fn print_sandbox_settings_is_a_real_invocation() {
    let invocation = parse_from("dev", ["environment-runner", "print-sandbox-settings"])
        .expect("print command must parse");
    assert_eq!(
        invocation,
        Invocation::PrintSandboxSettings(PrintSandboxSettingsArgs)
    );
}

/// 对应 Go `TestParseLogLevelAndMarkRequiredFlags`。
///
/// Rust 版本通过映射后的 `mark_required_flags` 修改 Clap 参数定义；除了验证缺失
/// `--session` 会在执行层之前成为 usage error，也验证不存在的参数仍会立即 panic。
#[test]
fn TestParseLogLevelAndMarkRequiredFlags() {
    for (value, expected) in [
        ("debug", LogLevel::Debug),
        ("info", LogLevel::Info),
        ("warn", LogLevel::Warn),
        ("error", LogLevel::Error),
    ] {
        assert_eq!(parse_log_level(value), Ok(expected));
    }
    let error = parse_log_level("verbose").expect_err("verbose must be rejected");
    assert!(error.contains("invalid log level"));

    let command = mark_required_flags(
        clap::Command::new("test").arg(clap::Arg::new("required").long("required")),
        &["required"],
    );
    let required = command
        .get_arguments()
        .find(|argument| argument.get_id().as_str() == "required")
        .expect("registered argument must remain present");
    assert!(required.is_required_set());

    let missing_flag =
        std::panic::catch_unwind(|| mark_required_flags(clap::Command::new("test"), &["missing"]));
    assert!(missing_flag.is_err(), "missing argument must panic");

    let error = parse_from("dev", ["environment-runner", "task-run"])
        .expect_err("task-run without --session must fail before dispatch");
    assert_eq!(error.kind(), CliErrorKind::Usage);
    assert!(error.rendered().contains("--session"));
}

/// 对应 Go `TestLoadServiceKeyReadsFileOrEnvironment`，包括空文件回退环境变量。
#[test]
fn TestLoadServiceKeyReadsFileOrEnvironment() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::TestLoadServiceKeyReadsFileOrEnvironment"
    )) {
        return;
    }
    // 环境变量属于整个测试进程；同步测试使用 blocking_lock 与异步用例共享
    // 同一把 crate 级锁，确保读写期间没有并行子进程观察到临时配置。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.blocking_lock();
    let mut environment = ScopedEnvironment::new();
    environment.remove("ENVIRONMENT_SERVICE_KEY");
    assert_eq!(load_service_key(None).unwrap(), "");

    environment.set("ENVIRONMENT_SERVICE_KEY", OsStr::new("env-key"));
    assert_eq!(load_service_key(None).unwrap(), "env-key");

    let directory = TempDir::new().unwrap();
    let key_path = directory.path().join("service-key");
    fs::write(&key_path, b" file-key \n").unwrap();
    assert_eq!(
        load_service_key(Some(&key_path)).unwrap(),
        "file-key",
        "文件 service key 必须 trim 首尾空白"
    );

    let empty_path = directory.path().join("empty-key");
    fs::write(&empty_path, b" \n").unwrap();
    assert_eq!(
        load_service_key(Some(&empty_path)).unwrap(),
        "env-key",
        "setup 与 poll 不同：空文件必须继续回退环境变量"
    );

    let missing = directory.path().join("missing-key");
    let error = load_service_key(Some(&missing)).expect_err("missing file must fail");
    assert!(error.contains("failed to read service key file"));
    assert!(error.contains(&format!("{:?}", missing.display().to_string())));
}

/// 对应 Go `TestRunAPIHealthcheck`，验证 whoami 路径、认证头和失败包装。
#[tokio::test(flavor = "current_thread")]
async fn TestRunAPIHealthcheck() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/environments/whoami"))
        .and(header("Authorization", "Bearer service-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "type": "environment_whoami",
            "organization_uuid": "org-1",
            "environment_id": "env-1"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let logger = cmd_test_logger();
    run_api_healthcheck(
        &CancellationToken::new(),
        &logger,
        &server.uri(),
        "service-key",
    )
    .await
    .expect("healthy whoami response must pass");
    server.verify().await;

    let failing = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/environments/whoami"))
        .respond_with(ResponseTemplate::new(401).set_body_string("nope\n"))
        .expect(1)
        .mount(&failing)
        .await;
    let error = run_api_healthcheck(
        &CancellationToken::new(),
        &default_orchestrator_logger(),
        &failing.uri(),
        "service-key",
    )
    .await
    .expect_err("non-200 whoami response must fail");
    assert!(error.contains("API connectivity healthcheck failed"));
    failing.verify().await;
}

/// 对应 Go `TestRunSetupSkipInstallationsAndPreflight`。
///
/// 把 PATH 清空可以证明两个安装都 skip 时不会偷偷运行 npm；非法日志值在 Rust 中
/// 由 Clap value parser 提前拒绝，等价于 Go `runSetup` 的首个检查。
#[tokio::test(flavor = "current_thread")]
async fn TestRunSetupSkipInstallationsAndPreflight() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::TestRunSetupSkipInstallationsAndPreflight"
    )) {
        return;
    }
    // PATH 是进程全局状态；即使本测试证明不会启动 npm，清空期间也不能
    // 让其他并行测试的 Git/Claude 子进程观察到该临时值。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let mut environment = ScopedEnvironment::new();
    environment.remove("ENVIRONMENT_SERVICE_KEY");
    environment.set("PATH", OsStr::new(""));
    let arguments = SetupArgs {
        log_level: LogLevel::Info,
        claude_code_version: "latest".to_owned(),
        sandbox_runtime_version: "latest".to_owned(),
        skip_claude_code: true,
        skip_sandbox_runtime: true,
        api_url: "https://api.example.test".to_owned(),
        service_key_file: None,
    };
    run_setup(&arguments, &CancellationToken::new())
        .await
        .expect("skip-everything setup must not require npm");

    let error = parse_from(
        "dev",
        [
            "environment-runner",
            "setup",
            "--skip-claude-code",
            "--skip-sandbox-runtime",
            "--log-level",
            "bad",
        ],
    )
    .expect_err("invalid log level must fail parsing");
    assert!(error.rendered().contains("invalid log level"));
}

/// 对应 Go `TestCheckNpmAvailableUsesPath`。
#[tokio::test(flavor = "current_thread")]
async fn TestCheckNpmAvailableUsesPath() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::TestCheckNpmAvailableUsesPath"
    )) {
        return;
    }
    // fake npm 的安装、两次 PATH 切换和守卫恢复必须属于同一全局串行区间。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let directory = TempDir::new().unwrap();
    let npm = directory.path().join("npm");
    fs::write(&npm, b"#!/bin/sh\nprintf '9.9.9\\n'\n").unwrap();
    fs::set_permissions(&npm, fs::Permissions::from_mode(0o700)).unwrap();

    let mut environment = ScopedEnvironment::new();
    environment.set("PATH", directory.path().as_os_str());
    check_npm_available(&CancellationToken::new(), &default_orchestrator_logger())
        .await
        .expect("fake npm on PATH must pass");

    let missing_path = TempDir::new().unwrap();
    environment.set("PATH", missing_path.path().as_os_str());
    let error = check_npm_available(&CancellationToken::new(), &default_orchestrator_logger())
        .await
        .expect_err("PATH without npm must fail");
    assert!(error.contains("npm not found in PATH"));
}

/// 对应 Go `TestInstallSandboxRuntimeCurrentSkipsInstallation`。
#[tokio::test(flavor = "current_thread")]
async fn TestInstallSandboxRuntimeCurrentSkipsInstallation() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    install_sandbox_runtime(&cancellation, &default_orchestrator_logger(), "current")
        .await
        .expect("current must skip npm even when already cancelled");
}

/// 对应 Go `TestAddSetupCommandRegistersAndRunsSkipMode`。
#[tokio::test(flavor = "current_thread")]
async fn TestAddSetupCommandRegistersAndRunsSkipMode() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::TestAddSetupCommandRegistersAndRunsSkipMode"
    )) {
        return;
    }
    // setup 会读取 service key 环境变量；即使该分支不执行外部命令，也必须与
    // 其他修改 PATH/HOME 的测试串行，满足 Rust 2024 对进程环境访问的约束。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    assert_eq!(
        command("dev")
            .find_subcommand("setup")
            .expect("setup must be registered")
            .get_name(),
        "setup"
    );

    let mut environment = ScopedEnvironment::new();
    environment.remove("ENVIRONMENT_SERVICE_KEY");
    let Invocation::Setup(arguments) = parse_from(
        "dev",
        [
            "environment-runner",
            "setup",
            "--skip-claude-code",
            "--skip-sandbox-runtime",
            "--log-level",
            "debug",
        ],
    )
    .expect("setup skip mode must parse") else {
        panic!("unexpected invocation");
    };
    run_setup(&arguments, &CancellationToken::new())
        .await
        .expect("setup skip mode must run");
}

/// 对应 Go `TestAddPollCommandPrintsWorkAndNull`。
///
/// 这里使用精确 stdout 快照，额外覆盖 Go map key 排序、float64 数字舍入和 HTML
/// 转义；仅断言包含字段无法发现这些迁移差异。
#[tokio::test(flavor = "current_thread")]
async fn TestAddPollCommandPrintsWorkAndNull() {
    let directory = TempDir::new().unwrap();
    let key_path = write_service_key(&directory, "service-key\n");
    let server = MockServer::start().await;
    mount_whoami(&server, "service-key", "org-1", "env-1", 1).await;
    Mock::given(method("GET"))
        .and(path("/v1/environments/env-1/work/poll"))
        .and(header("Authorization", "Bearer service-key"))
        .and(header("Anthropic-Worker-ID", "worker-1"))
        .and(query_param("reclaim_older_than_ms", "123"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"z":"<\u2028&>","type":"work","state":"available","secret":"secret","number":1.0,"id":"work-1","environment_id":"env-1","data":{"type":"session","id":"session-1"},"big":9007199254740993}"#,
            "application/json",
        ))
        .expect(1)
        .mount(&server)
        .await;

    let mut arguments = poll_arguments(&server, key_path.clone());
    arguments.worker_id = Some("worker-1".to_owned());
    arguments.reclaim_older_than_ms = 123;
    let mut output = Vec::new();
    run_poll(&arguments, &CancellationToken::new(), &mut output)
        .await
        .expect("work poll must succeed");
    assert_eq!(
        execute_command_with_stdout(output),
        concat!(
            "{\n",
            "  \"big\": 9007199254740992,\n",
            "  \"data\": {\n",
            "    \"id\": \"session-1\",\n",
            "    \"type\": \"session\"\n",
            "  },\n",
            "  \"environment_id\": \"env-1\",\n",
            "  \"id\": \"work-1\",\n",
            "  \"number\": 1,\n",
            "  \"secret\": \"secret\",\n",
            "  \"state\": \"available\",\n",
            "  \"type\": \"work\",\n",
            "  \"z\": \"\\u003c\\u2028\\u0026\\u003e\"\n",
            "}\n"
        )
    );
    server.verify().await;

    let empty = MockServer::start().await;
    mount_whoami(&empty, "service-key", "org-1", "env-1", 1).await;
    Mock::given(method("GET"))
        .and(path("/v1/environments/env-1/work/poll"))
        .respond_with(ResponseTemplate::new(200).set_body_raw("null", "application/json"))
        .expect(1)
        .mount(&empty)
        .await;
    let mut output = Vec::new();
    run_poll(
        &poll_arguments(&empty, key_path),
        &CancellationToken::new(),
        &mut output,
    )
    .await
    .expect("empty poll must succeed");
    assert_eq!(execute_command_with_stdout(output), "null\n");
    empty.verify().await;
}

/// 对应 Go `TestAddPollCommandServiceKeyAndIdentityErrors`。
#[tokio::test(flavor = "current_thread")]
async fn TestAddPollCommandServiceKeyAndIdentityErrors() {
    let directory = TempDir::new().unwrap();
    let empty_key = write_service_key(&directory, " \n");
    let mut output = Vec::new();
    let error = run_poll(
        &PollArgs {
            api_url: "https://api.example.test".to_owned(),
            organization_id: None,
            environment_id: None,
            worker_id: None,
            service_key_file: Some(empty_key),
            reclaim_older_than_ms: 0,
            log_level: LogLevel::Info,
        },
        &CancellationToken::new(),
        &mut output,
    )
    .await
    .expect_err("empty explicit key file must not fall back to the environment");
    assert!(error.contains("service key is required"));

    let key_path = write_service_key(&directory, "service-key\n");
    let server = MockServer::start().await;
    mount_whoami(&server, "service-key", "org-real", "env-real", 1).await;
    let mut arguments = poll_arguments(&server, key_path);
    arguments.organization_id = Some("org-other".to_owned());
    let error = run_poll(&arguments, &CancellationToken::new(), &mut Vec::new())
        .await
        .expect_err("organization mismatch must stop before poll");
    assert!(error.contains("does not match token's organization"));
    server.verify().await;
}

/// 对应 Go `TestPollCommandFallsBackToProvidedIdentityWhenWhoamiFails`。
#[tokio::test(flavor = "current_thread")]
async fn TestPollCommandFallsBackToProvidedIdentityWhenWhoamiFails() {
    let directory = TempDir::new().unwrap();
    let key_path = write_service_key(&directory, "file-key\n");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/environments/whoami"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable\n"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/environments/env-fallback/work/poll"))
        .and(header("Authorization", "Bearer file-key"))
        .respond_with(ResponseTemplate::new(200).set_body_raw("null", "application/json"))
        .expect(1)
        .mount(&server)
        .await;

    let mut arguments = poll_arguments(&server, key_path);
    arguments.organization_id = Some("org-fallback".to_owned());
    arguments.environment_id = Some("env-fallback".to_owned());
    let mut output = Vec::new();
    run_poll(&arguments, &CancellationToken::new(), &mut output)
        .await
        .expect("explicit identity must permit whoami fallback");
    assert_eq!(output, b"null\n");
    server.verify().await;
}

/// 对应 Go `TestPollCommandReportsInvalidWorkJSON`。
#[tokio::test(flavor = "current_thread")]
async fn TestPollCommandReportsInvalidWorkJSON() {
    let directory = TempDir::new().unwrap();
    let key_path = write_service_key(&directory, "service-key\n");
    let server = MockServer::start().await;
    mount_whoami(&server, "service-key", "org-1", "env-1", 1).await;
    Mock::given(method("GET"))
        .and(path("/v1/environments/env-1/work/poll"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"id":"work-1","type":"work","environment_id":"env-1","state":"available","data":{"type":"session","id":"session-1"},"secret":"secret","extra":"#,
            "application/json",
        ))
        .expect(1)
        .mount(&server)
        .await;
    let error = run_poll(
        &poll_arguments(&server, key_path),
        &CancellationToken::new(),
        &mut Vec::new(),
    )
    .await
    .expect_err("poller must reject invalid JSON before raw printing");
    assert!(error.contains("failed to unmarshal work response"));
    server.verify().await;
}

/// code-sign 的原始 argv 解析测试，特别保护 Cobra 会保留的字面 `--`。
#[test]
fn TestRunCodeSignParsesSSHArguments() {
    let Invocation::CodeSign(arguments) = parse_from(
        "dev",
        [
            "environment-runner",
            "code-sign",
            "-Y",
            "sign",
            "-n",
            "git",
            "-f",
            "/tmp/key.pub",
            "payload",
        ],
    )
    .expect("SSH argv must bypass Clap flag parsing") else {
        panic!("unexpected invocation");
    };
    let CodeSignOperation::SshSign(sign) = arguments.parse_operation().unwrap();
    assert_eq!(sign.file_path, PathBuf::from("payload"));
    assert_eq!(sign.key_file, Some(PathBuf::from("/tmp/key.pub")));
    assert_eq!(sign.namespace, Some(OsString::from("git")));

    let Invocation::CodeSign(arguments) = parse_from(
        "dev",
        [
            "environment-runner",
            "code-sign",
            "-Y",
            "sign",
            "--",
            "payload",
        ],
    )
    .expect("literal -- must be retained") else {
        panic!("unexpected invocation");
    };
    assert_eq!(
        arguments.raw_args,
        ["-Y", "sign", "--", "payload"].map(OsString::from)
    );
    assert_eq!(
        arguments.parse_operation().unwrap_err(),
        "no file specified to sign",
        "Go treats -- as an unknown flag and consumes its following value"
    );
}

/// code-sign 指数退避的边界测试；抖动只能增加基础值，不能超过 30%。
#[test]
fn TestCalculateBackoff() {
    let config = RetryConfig {
        max_retries: 2,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_millis(200),
        backoff_factor: 2.0,
    };
    assert_eq!(calculate_backoff(0, config), Duration::from_millis(100));
    for _ in 0..32 {
        let first = calculate_backoff(1, config);
        assert!(first >= Duration::from_millis(100));
        assert!(first < Duration::from_millis(130));
        let capped = calculate_backoff(10, config);
        assert!(capped >= Duration::from_millis(200));
        assert!(capped < Duration::from_millis(260));
    }
}

/// code-sign 配置优先读取成对环境变量，并保持 `fmt.Fscanf("%d")` 的前缀语义。
#[test]
fn TestReadCodeSignConfigFromEnvironment() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::TestReadCodeSignConfigFromEnvironment"
    )) {
        return;
    }
    // code-sign 配置是进程全局环境，使用 crate 级锁覆盖设置、读取和恢复全过程。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.blocking_lock();
    let mut environment = ScopedEnvironment::new();
    environment.set("CODESIGN_MCP_PORT", OsStr::new("12345trailing"));
    environment.set("CODESIGN_MCP_TOKEN", OsStr::new("temporary-token"));
    let config = read_code_sign_config().expect("valid env config must win");
    assert_eq!(config.port, 12_345);
    assert_eq!(config.bearer_token, "temporary-token");
    assert_eq!(scan_positive_integer("  +9199 rest"), Some(9199));
    assert_eq!(scan_positive_integer("0"), None);
    assert_eq!(scan_positive_integer("-1"), None);
    assert_eq!(scan_positive_integer("not-a-port"), None);
}

/// 单次 MCP HTTP 调用必须验证 header、状态码、JSON-RPC error 和 result 存在性。
#[tokio::test(flavor = "current_thread")]
async fn TestDoSingleAttemptValidatesJSONRPC() {
    let server = MockServer::start().await;
    for (endpoint, body) in [
        (
            "/result",
            r#"{"jsonrpc":"2.0","id":"request-1","result":{"content":null,"isError":null}}"#,
        ),
        ("/explicit-null", r#"{"result":null}"#),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .and(header("Content-Type", "application/json"))
            .and(header("Authorization", "Bearer bearer-token"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/rpc-error"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "error": {"code": null, "message": "denied"}
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/response-null"))
        .respond_with(ResponseTemplate::new(200).set_body_raw("null", "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/status"))
        .respond_with(ResponseTemplate::new(503).set_body_string("busy"))
        .expect(1)
        .mount(&server)
        .await;

    let client = reqwest::Client::new();
    let result = do_single_attempt(
        &client,
        &format!("{}/result", server.uri()),
        b"{}",
        "bearer-token",
    )
    .await
    .expect("result object must pass");
    assert_eq!(
        serde_json::from_slice::<Value>(&result).unwrap(),
        json!({"content": null, "isError": null})
    );

    let explicit_null = do_single_attempt(
        &client,
        &format!("{}/explicit-null", server.uri()),
        b"{}",
        "bearer-token",
    )
    .await
    .expect("explicit result:null is present");
    assert_eq!(explicit_null, b"null");

    let error = do_single_attempt(
        &client,
        &format!("{}/rpc-error", server.uri()),
        b"{}",
        "bearer-token",
    )
    .await
    .unwrap_err();
    assert_eq!(error, "MCP error: denied");
    let error = do_single_attempt(
        &client,
        &format!("{}/response-null", server.uri()),
        b"{}",
        "bearer-token",
    )
    .await
    .unwrap_err();
    assert_eq!(error, "no result in MCP response");
    let error = do_single_attempt(
        &client,
        &format!("{}/status", server.uri()),
        b"{}",
        "bearer-token",
    )
    .await
    .unwrap_err();
    assert_eq!(error, "MCP server returned status 503: busy");
    server.verify().await;
}

/// 重试必须总共尝试 `max_retries + 1` 次，并在第三次成功后返回 result。
#[tokio::test(flavor = "current_thread")]
async fn TestDoAttemptsWithRetry() {
    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(move |_request: &Request| {
            if observed.fetch_add(1, Ordering::SeqCst) < 2 {
                ResponseTemplate::new(503).set_body_string("not ready")
            } else {
                ResponseTemplate::new(200).set_body_json(json!({"result": {"ok": true}}))
            }
        })
        .expect(3)
        .mount(&server)
        .await;
    let client = reqwest::Client::new();
    let result = do_attempts_with_retry(
        &client,
        &format!("{}/mcp", server.uri()),
        b"{}",
        "token",
        RetryConfig {
            max_retries: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            backoff_factor: 1.0,
        },
    )
    .await
    .expect("third attempt must succeed");
    assert_eq!(
        serde_json::from_slice::<Value>(&result).unwrap(),
        json!({"ok": true})
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    server.verify().await;
}

/// 端到端 code-sign 单测：请求协议、tool error 和 `.sig` 副作用都必须与 Go 一致。
#[tokio::test(flavor = "current_thread")]
async fn TestHandleSSHSignCallsServerAndChecksSignature() {
    // 父测试进程只启动隔离子进程，不在自身读写全局环境。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::TestHandleSSHSignCallsServerAndChecksSignature"
    )) {
        return;
    }
    // `run_code_sign` 会通过 PATH 执行 Git 探测 object format，因此与修改 PATH 的
    // 测试使用同一把 crate 级锁，保持默认 sha1 分支的真实成因。
    let _process_state = crate::internal::PROCESS_STATE_TEST_LOCK.lock().await;
    let server = MockServer::start().await;
    let mut environment = ScopedEnvironment::new();
    environment.set(
        "CODESIGN_MCP_PORT",
        OsStr::new(&server.address().port().to_string()),
    );
    environment.set("CODESIGN_MCP_TOKEN", OsStr::new("signing-token"));

    let directory = TempDir::new().unwrap();
    let payload = directory.path().join("payload");
    fs::write(&payload, b"payload").unwrap();
    let signature = PathBuf::from(format!("{}.sig", payload.display()));
    fs::write(&signature, b"signature").unwrap();
    let expected_request = json!({
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "sign_file",
            "arguments": {"file_path": payload.to_string_lossy()}
        }
    });
    mount_code_sign_response(
        &server,
        &expected_request,
        json!({"result": {"content": [], "isError": false}}),
    )
    .await;
    let arguments = code_sign_arguments(&payload);
    run_code_sign(&arguments)
        .await
        .expect("successful tool call plus signature file must pass");
    server.verify().await;

    server.reset().await;
    mount_code_sign_response(
        &server,
        &expected_request,
        json!({"result": {"content": [null], "isError": true}}),
    )
    .await;
    let error = run_code_sign(&arguments)
        .await
        .expect_err("tool-level failure must win over existing signature");
    assert_eq!(error, "signing failed: unknown error");
    server.verify().await;

    server.reset().await;
    fs::remove_file(&signature).unwrap();
    mount_code_sign_response(&server, &expected_request, json!({"result": null})).await;
    let error = run_code_sign(&arguments)
        .await
        .expect_err("result:null is zero tool result, then missing signature must fail");
    assert_eq!(
        error,
        format!("signature file not created: {}", signature.display())
    );
    server.verify().await;
}

/// 所有修改进程环境的 cmd 测试共享一把锁，并在 scope 结束时逆序恢复原值。
///
/// Rust 2024 把进程环境写入标成 unsafe；这些测试只在 current-thread runtime 中
/// 修改，并通过该 scope 串行化本模块的所有相关写入。
struct ScopedEnvironment {
    _lock: MutexGuard<'static, ()>,
    previous: Vec<(&'static str, Option<OsString>)>,
}

impl ScopedEnvironment {
    /// 获取全局环境锁；poison 只表示前一个测试 panic，恢复后仍可继续清理环境。
    fn new() -> Self {
        Self {
            _lock: PROCESS_ENVIRONMENT_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            previous: Vec::new(),
        }
    }

    /// 设置变量，并且只在第一次改动该 key 时记录原值。
    fn set(&mut self, key: &'static str, value: &OsStr) {
        self.remember(key);
        // SAFETY: `ScopedEnvironment` 持有本模块唯一的进程环境写锁，测试使用
        // current-thread runtime，且 Drop 会在释放锁前恢复原值。PATH 的调用点还会
        // 在整个 scope 内持有 crate 级 `PROCESS_STATE_TEST_LOCK`。
        unsafe { std::env::set_var(key, value) };
    }

    /// 删除变量，并在 scope 结束时恢复。
    fn remove(&mut self, key: &'static str) {
        self.remember(key);
        // SAFETY: 与 `set` 相同，写入期间持有独占测试锁。
        unsafe { std::env::remove_var(key) };
    }

    fn remember(&mut self, key: &'static str) {
        if self.previous.iter().all(|(known, _)| *known != key) {
            self.previous.push((key, std::env::var_os(key)));
        }
    }
}

impl Drop for ScopedEnvironment {
    fn drop(&mut self) {
        for (key, previous) in self.previous.drain(..).rev() {
            // SAFETY: 恢复仍发生在独占锁释放之前。
            unsafe {
                if let Some(value) = previous {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }
}

static PROCESS_ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

/// 创建测试 service key 文件；TempDir 生命周期覆盖整个请求。
fn write_service_key(directory: &TempDir, contents: &str) -> PathBuf {
    let path = directory.path().join(format!(
        "service-key-{}",
        directory.path().read_dir().unwrap().count()
    ));
    fs::write(&path, contents).unwrap();
    path
}

/// 对应 Go `executeCommandWithStdout`，把命令写入的原始 stdout 缓冲转成文本快照。
///
/// Rust 命令层把 writer 显式注入 `run_poll`，因此无需替换进程全局 stdout；仍在同一
/// helper 统一执行 UTF-8 校验，确保测试不会用有损转换掩盖字节差异。
fn execute_command_with_stdout(output: Vec<u8>) -> String {
    String::from_utf8(output).expect("command stdout must be UTF-8")
}

/// 对应 Go `cmdTestLogger`，创建与生产 orchestrator 相同的确定性测试 logger。
fn cmd_test_logger() -> crate::internal::logger::Logger {
    default_orchestrator_logger()
}

/// 构造最小 poll 参数，测试按需覆盖身份、worker 和 reclaim 字段。
fn poll_arguments(server: &MockServer, service_key_file: PathBuf) -> PollArgs {
    PollArgs {
        api_url: server.uri(),
        organization_id: None,
        environment_id: None,
        worker_id: None,
        service_key_file: Some(service_key_file),
        reclaim_older_than_ms: 0,
        log_level: LogLevel::Info,
    }
}

/// 挂载一次成功 whoami；独立 helper 让 poll 测试只描述各自差异。
async fn mount_whoami(
    server: &MockServer,
    service_key: &str,
    organization_id: &str,
    environment_id: &str,
    expected_calls: u64,
) {
    Mock::given(method("GET"))
        .and(path("/v1/environments/whoami"))
        .and(header(
            "Authorization",
            format!("Bearer {service_key}").as_str(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "type": "environment_whoami",
            "organization_uuid": organization_id,
            "environment_id": environment_id
        })))
        .expect(expected_calls)
        .mount(server)
        .await;
}

/// 生成完整的 SSH sign 原始参数，使用绝对临时文件避免 current_dir 副作用。
fn code_sign_arguments(payload: &std::path::Path) -> CodeSignArgs {
    CodeSignArgs {
        raw_args: vec![
            OsString::from("-Y"),
            OsString::from("sign"),
            OsString::from("-n"),
            OsString::from("git"),
            OsString::from("-f"),
            OsString::from("ignored-key.pub"),
            payload.as_os_str().to_owned(),
        ],
    }
}

/// 挂载一个带严格认证与核心请求体匹配的 code-sign MCP 响应。
async fn mount_code_sign_response(server: &MockServer, request: &Value, response: Value) {
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .and(header("Authorization", "Bearer signing-token"))
        .and(header("Content-Type", "application/json"))
        .and(body_partial_json(request))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(1)
        .mount(server)
        .await;
}
