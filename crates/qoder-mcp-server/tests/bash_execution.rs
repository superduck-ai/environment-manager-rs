//! Regression coverage for the Go Bash execution-result contract.
mod common;
use qoder_mcp_server::{
    Config, Registry, Server, ToolResult,
    execution::{ExecutionResult, ExecutionStatus, output_schema},
    plugins::Manager,
};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

fn server(dir: &Path) -> Arc<Server> {
    Arc::new(Server::new(
        Arc::new(Registry::builtin(dir.into(), None)),
        Arc::new(Manager::default()),
        Config::default(),
    ))
}
fn execution(result: &ToolResult) -> &ExecutionResult {
    let e = result
        .structured_content
        .as_ref()
        .expect("structured execution result");
    assert_eq!(result.content.len(), 2);
    assert_eq!(
        serde_json::from_str::<Value>(&result.content[1].text).unwrap(),
        serde_json::to_value(e).unwrap()
    );
    e
}
async fn bash(s: &Server, args: Value) -> ToolResult {
    s.registry
        .get("bash")
        .unwrap()
        .handle(CancellationToken::new(), args)
        .await
        .unwrap()
}
async fn wait_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("command started");
}
fn cleanup(e: &ExecutionResult) {
    for path in [&e.output_file, &e.stdout_file, &e.stderr_file] {
        if !path.is_empty() {
            let _ = std::fs::remove_file(path);
        }
    }
}
fn kill_group(pid: u32) {
    use nix::{
        sys::signal::{Signal, kill},
        unistd::Pid,
    };
    let _ = kill(Pid::from_raw(-(pid as i32)), Signal::SIGKILL);
}

#[tokio::test]
async fn streams_exit_codes_signals_and_legacy_text() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    for (command, stdout, stderr, code, error, legacy) in [
        (
            "printf out; printf err >&2; exit 7",
            "out",
            "err",
            7,
            true,
            "",
        ),
        ("kill -TERM $$", "", "", -1, true, ""),
        ("printf x >&2", "", "x", 0, false, "x"),
        ("printf x", "x", "", 0, false, "x"),
        ("true", "", "", 0, false, "(no output)"),
        (
            "printf '(no output)'",
            "(no output)",
            "",
            0,
            false,
            "(no output)",
        ),
        ("printf ' \\n'; printf '\\t' >&2", " \n", "\t", 0, false, ""),
    ] {
        let r = bash(&s, json!({"command":command})).await;
        let e = execution(&r);
        assert_eq!(
            (&e.stdout, &e.stderr),
            (&stdout.to_owned(), &stderr.to_owned()),
            "{command}"
        );
        assert_eq!(e.exit_code, Some(code));
        assert_eq!(e.status, ExecutionStatus::Completed);
        assert_eq!(r.is_error, error);
        assert_eq!(
            (e.stdout_bytes, e.stderr_bytes),
            (stdout.len(), stderr.len())
        );
        assert!(!e.stdout_truncated && !e.stderr_truncated && !e.timed_out && !e.cancelled);
        assert!(e.stdout_file.is_empty() && e.stderr_file.is_empty() && e.output_file.is_empty());
        assert!(e.error.is_empty());
        assert!(e.pid.is_some());
        if command == "kill -TERM $$" {
            assert_eq!(e.signal, "terminated");
        }
        if !legacy.is_empty() {
            assert_eq!(r.content[0].text, legacy);
        }
    }
}

#[tokio::test]
async fn failures_before_execution_have_not_started_results() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    for args in [
        json!({}),
        json!({"command":"vim"}),
        json!({"command":"true","on_timeout":"invalid"}),
        json!({"command":"true","env":{"X":7}}),
        json!({"command":"true","work_dir":dir.path().join("missing")}),
    ] {
        let r = bash(&s, args).await;
        let e = execution(&r);
        assert!(r.is_error);
        assert_eq!(e.status, ExecutionStatus::NotStarted);
        assert_eq!(e.exit_code, None);
        assert_eq!(e.pid, None);
        assert_eq!(e.duration_ms, 0);
        assert!(!e.error.is_empty());
        assert!(e.stdout.is_empty() && e.stderr.is_empty());
    }
    let token = CancellationToken::new();
    token.cancel();
    let r = s
        .registry
        .get("bash")
        .unwrap()
        .handle(token, json!({"command":"touch forbidden"}))
        .await
        .unwrap();
    assert_eq!(execution(&r).status, ExecutionStatus::NotStarted);
    assert!(!dir.path().join("forbidden").exists());
}

#[tokio::test]
async fn timeout_and_cancellation_preserve_partial_streams() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let r = bash(
        &s,
        json!({"command":"printf started; printf warning >&2; sleep 30", "timeout":1}),
    )
    .await;
    let e = execution(&r);
    assert!(r.is_error && e.timed_out && !e.cancelled);
    assert_eq!(e.status, ExecutionStatus::TimedOut);
    assert_eq!(e.stdout, "started");
    assert_eq!(e.stderr, "warning");
    assert!(e.exit_code.is_some());
    assert!(e.duration_ms >= 900);
    let tool = s.registry.get("bash").unwrap();
    let token = CancellationToken::new();
    let cancel = token.clone();
    let task = tokio::spawn(async move {
        tool.handle(
            token,
            json!({"command":"printf started; printf warning >&2; touch ready; sleep 30"}),
        )
        .await
        .unwrap()
    });
    wait_file(&dir.path().join("ready")).await;
    cancel.cancel();
    let r = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    let e = execution(&r);
    assert!(r.is_error && e.cancelled && !e.timed_out);
    assert_eq!(e.status, ExecutionStatus::Cancelled);
    assert_eq!(e.stdout, "started");
    assert_eq!(e.stderr, "warning");
    assert!(e.exit_code.is_some());
}

#[tokio::test]
async fn streams_are_truncated_independently_and_full_files_retained() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    for command in [
        "printf '%030001d' 0; printf err >&2",
        "printf out; printf '%030001d' 0 >&2",
    ] {
        let r = bash(&s, json!({"command":command})).await;
        let e = execution(&r);
        assert!(!r.is_error);
        assert!(!e.output_file.is_empty());
        let (text, bytes, path, small) = if e.stdout_truncated {
            assert!(!e.stderr_truncated);
            (&e.stdout, e.stdout_bytes, &e.stdout_file, &e.stderr)
        } else {
            assert!(e.stderr_truncated);
            (&e.stderr, e.stderr_bytes, &e.stderr_file, &e.stdout)
        };
        assert_eq!(text.len(), 30000);
        assert_eq!(bytes, 30001);
        assert_eq!(small.len(), 3);
        let full = std::fs::read_to_string(path).unwrap();
        assert_eq!(full.len(), 30001);
        assert!(full.ends_with(text));
        assert!(r.content[0].text.contains("[output truncated:"));
        cleanup(e);
    }
}

#[tokio::test]
async fn background_snapshot_keeps_streams_and_finishes_after_request_returns() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let r = bash(&s, json!({"command":"printf out; printf err >&2; sleep 2; printf done; exit 4", "timeout":1,"on_timeout":"background"})).await;
    let e = execution(&r);
    assert!(!r.is_error && !e.timed_out && !e.cancelled);
    assert_eq!(e.status, ExecutionStatus::Running);
    assert_eq!(e.exit_code, None);
    assert_eq!((&e.stdout[..], &e.stderr[..]), ("out", "err"));
    assert!(e.pid.is_some());
    assert!(!e.output_file.is_empty() && !e.stdout_file.is_empty() && !e.stderr_file.is_empty());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if std::fs::read_to_string(&e.output_file)
                .unwrap()
                .ends_with("\n[exit code: 4]\n")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(std::fs::read_to_string(&e.stdout_file).unwrap(), "outdone");
    assert_eq!(std::fs::read_to_string(&e.stderr_file).unwrap(), "err");
    cleanup(e);
}

#[tokio::test]
async fn inherited_output_pipes_have_a_bounded_wait_and_capture_error() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        bash(&s, json!({"command":"sleep 30 & printf ready"})),
    )
    .await
    .expect("must not hang on inherited pipe");
    let e = execution(&r);
    kill_group(e.pid.unwrap());
    assert!(r.is_error);
    assert_eq!(e.exit_code, Some(0));
    assert!(!e.error.is_empty());
    assert!(e.duration_ms >= 1900);
    assert_eq!(e.stdout, "ready");
    assert_eq!(e.stderr, "");
    cleanup(e);
}

async fn dispatch(s: &Arc<Server>, request: Value) -> Value {
    common::rpc(s.clone(), request).await
}
#[tokio::test]
async fn wire_schema_aliases_and_validation() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let list = dispatch(&s, json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).await;
    for definition in list["result"]["tools"].as_array().unwrap() {
        let name = definition["name"].as_str().unwrap();
        if !matches!(name, "bash" | "run_in_terminal") {
            assert!(definition.get("outputSchema").is_none());
            continue;
        }
        let schema = &definition["outputSchema"];
        assert_eq!(schema, &output_schema());
        assert_eq!(schema["properties"].as_object().unwrap().len(), 17);
        for args in [
            json!({}),
            json!({"command":"printf out; printf err >&2; exit 9"}),
        ] {
            let r = dispatch(&s, json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":name,"arguments":args}})).await;
            let result = &r["result"];
            assert_eq!(result["isError"], true);
            let e = &result["structuredContent"];
            assert_eq!(
                serde_json::from_str::<Value>(result["content"][1]["text"].as_str().unwrap())
                    .unwrap(),
                *e
            );
            assert_eq!(schema["additionalProperties"], false);
            for key in schema["required"].as_array().unwrap() {
                assert!(e.get(key.as_str().unwrap()).is_some());
            }
            for (key, value) in e.as_object().unwrap() {
                let property = &schema["properties"][key];
                assert!(!property.is_null(), "undeclared {key}");
                match property["type"].as_str() {
                    Some("string") => assert!(value.is_string()),
                    Some("boolean") => assert!(value.is_boolean()),
                    Some("integer") => assert!(value.is_number()),
                    _ => assert!(value.is_null() || value.is_i64()),
                }
            }
            if args.get("command").is_none() {
                assert_eq!(e["status"], "not_started");
                assert!(e["exit_code"].is_null());
            } else {
                assert_eq!(e["stdout"], "out");
                assert_eq!(e["stderr"], "err");
                assert_eq!(e["exit_code"], 9);
            }
        }
    }
}

#[tokio::test]
async fn background_preview_is_bounded_and_utf8_tail_starts_on_a_character() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let r = bash(&s, json!({"command":"printf '%03001d' 0; printf '%03002d' 0 >&2; sleep 30", "timeout":1,"on_timeout":"background"})).await;
    let e = execution(&r);
    kill_group(e.pid.unwrap());
    assert!(!r.is_error);
    assert_eq!(e.status, ExecutionStatus::Running);
    assert_eq!((e.stdout.len(), e.stderr.len()), (2000, 2000));
    assert_eq!((e.stdout_bytes, e.stderr_bytes), (3001, 3002));
    assert!(e.stdout_truncated && e.stderr_truncated);
    // Wait until the detached waiter has closed all stream files.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !std::fs::read_to_string(&e.output_file)
            .unwrap()
            .contains("[killed by signal")
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    cleanup(e);
    let r = bash(
        &s,
        json!({"command":"printf '中%.0s' {1..10001}; printf x"}),
    )
    .await;
    let e = execution(&r);
    assert!(e.stdout_truncated);
    assert!(e.stdout.ends_with("中x"));
    assert_eq!(e.stdout.len(), 29998);
    assert_eq!(e.stdout_bytes, 30004);
    assert!(!e.stdout.contains('\u{fffd}'));
    cleanup(e);
}

#[tokio::test]
async fn json_fallback_uses_go_escaping_and_execution_field_order() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path());
    let r = bash(&s, json!({"command":"printf '<>&'"})).await;
    let e = execution(&r);
    assert_eq!(e.stdout, "<>&");
    assert!(r.content[1].text.starts_with(r#"{"stdout":"\u003c\u003e\u0026","stderr":"","exit_code":0,"status":"completed","duration_ms":"#));
}
