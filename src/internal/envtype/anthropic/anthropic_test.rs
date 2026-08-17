//! Anthropic 环境恢复行为的定向测试。

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use serde_json::Value;
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    sync::{MutexGuard, Notify},
};
use tokio_util::sync::CancellationToken;

use crate::internal::{
    PROCESS_STATE_TEST_LOCK,
    api::{LogCategory, LogLevel, SessionError},
    auth::new_auth_context_with_session_id,
    config::{SessionMode, StartupContext},
    session::{ActivityRecorder, ActivityRecorderError},
    util::Stopper,
};

use super::{
    AnthropicEnvironmentType, Config, Language,
    anthropic::{InitActivityStopGuard, copy_dir},
    decode_config,
    types::HookConfig,
};

/// 测试活动记录器保存的可观察调用。
#[derive(Default)]
struct RecordingActivityRecorder {
    activities: Mutex<Vec<String>>,
    long_running_messages: Mutex<Vec<String>>,
    failures: Mutex<Vec<String>>,
    stop_count: Arc<AtomicUsize>,
}

#[async_trait]
impl ActivityRecorder for RecordingActivityRecorder {
    /// 保存普通活动消息。
    fn record_activity(
        &self,
        _level: LogLevel,
        _category: LogCategory,
        message: String,
        _extra: BTreeMap<String, Value>,
    ) {
        self.activities.lock().unwrap().push(message);
    }

    /// 保存长任务消息并返回可计数 stopper。
    fn record_long_running_activity(
        &self,
        _category: LogCategory,
        message: String,
    ) -> Arc<dyn Stopper> {
        self.long_running_messages.lock().unwrap().push(message);
        Arc::new(RecordingStopper(Arc::clone(&self.stop_count)))
    }

    /// 保存失败结果的用户可见文案。
    async fn record_failure_result(
        &self,
        session_error: &(dyn SessionError + Send + Sync),
    ) -> Result<(), ActivityRecorderError> {
        self.failures
            .lock()
            .unwrap()
            .push(session_error.get_user_message());
        Ok(())
    }
}

/// drop/stop 时递增计数的测试 stopper。
struct RecordingStopper(Arc<AtomicUsize>);

#[async_trait]
impl Stopper for RecordingStopper {
    /// 记录一次同步停止请求。
    fn request_stop(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// 记录完整 stop；该 fixture 没有后台任务，因此同步请求后即可返回。
    async fn stop(&self) {
        self.request_stop();
    }
}

/// 可控地阻塞完整 Stop 的测试 stopper，用于区分同步取消和异步等待两个阶段。
#[derive(Default)]
struct BlockingStopper {
    request_count: AtomicUsize,
    stop_entered: Notify,
    release_stop: Notify,
    completed_count: AtomicUsize,
}

#[async_trait]
impl Stopper for BlockingStopper {
    /// 记录同步取消已传播；该方法不得等待测试放行信号。
    fn request_stop(&self) {
        self.request_count.fetch_add(1, Ordering::SeqCst);
    }

    /// 发出取消后阻塞到 fixture 放行，以证明 guard 的显式清理会等待。
    async fn stop(&self) {
        self.request_stop();
        self.stop_entered.notify_one();
        self.release_stop.notified().await;
        self.completed_count.fetch_add(1, Ordering::SeqCst);
    }
}

/// initialize 所属 future 被 abort 时，guard 必须同步请求停止长任务。
#[tokio::test]
async fn init_activity_guard_future_drop_requests_stop() {
    let stopper = Arc::new(BlockingStopper::default());
    let task_stopper: Arc<dyn Stopper> = stopper.clone();
    let guard_installed = Arc::new(Notify::new());
    let task_guard_installed = Arc::clone(&guard_installed);
    let task = tokio::spawn(async move {
        let mut guard = InitActivityStopGuard::empty();
        guard.set(task_stopper);
        task_guard_installed.notify_one();
        std::future::pending::<()>().await;
        drop(guard);
    });

    guard_installed.notified().await;
    task.abort();
    assert!(
        task.await
            .expect_err("abort 后任务必须以 JoinError 结束")
            .is_cancelled()
    );
    assert_eq!(stopper.request_count.load(Ordering::SeqCst), 1);
    assert_eq!(stopper.completed_count.load(Ordering::SeqCst), 0);
}

/// 正常 initialize 返回路径调用显式 Stop 时，必须等待 stopper 的异步清理完成。
#[tokio::test]
async fn init_activity_guard_explicit_stop_waits_for_completion() {
    let stopper = Arc::new(BlockingStopper::default());
    let guard_stopper: Arc<dyn Stopper> = stopper.clone();
    let mut guard = InitActivityStopGuard::empty();
    guard.set(guard_stopper);
    let stop_task = tokio::spawn(guard.stop());

    stopper.stop_entered.notified().await;
    assert_eq!(stopper.request_count.load(Ordering::SeqCst), 1);
    assert_eq!(stopper.completed_count.load(Ordering::SeqCst), 0);
    assert!(!stop_task.is_finished(), "未放行异步清理前 Stop 不得返回");

    stopper.release_stop.notify_one();
    tokio::time::timeout(Duration::from_secs(1), stop_task)
        .await
        .expect("放行后 Stop 应在一秒内完成")
        .expect("Stop task 不应 panic");
    assert_eq!(stopper.completed_count.load(Ordering::SeqCst), 1);
}

/// 在作用域结束时恢复 CWD 和 HOME。
struct ProcessStateGuard {
    original_cwd: PathBuf,
    original_home: Option<OsString>,
}

impl ProcessStateGuard {
    /// 锁定全局状态，并把 HOME 指向测试目录。
    async fn enter(home: &Path) -> (MutexGuard<'static, ()>, Self) {
        let lock = PROCESS_STATE_TEST_LOCK.lock().await;
        let guard = Self {
            original_cwd: std::env::current_dir().unwrap(),
            original_home: std::env::var_os("HOME"),
        };
        // SAFETY: crate 级 PROCESS_STATE_TEST_LOCK 串行化所有环境类型测试的全局环境访问。
        unsafe { std::env::set_var("HOME", home) };
        (lock, guard)
    }
}

impl Drop for ProcessStateGuard {
    /// 恢复测试进入前的进程状态。
    fn drop(&mut self) {
        std::env::set_current_dir(&self.original_cwd).unwrap();
        // SAFETY: guard 持有 crate 级锁，恢复动作与 enter 使用相同串行边界。
        unsafe {
            if let Some(home) = &self.original_home {
                std::env::set_var("HOME", home);
            } else {
                std::env::remove_var("HOME");
            }
        }
    }
}

/// 构造无上下文的测试环境。
fn environment(config: Option<Config>) -> AnthropicEnvironmentType {
    AnthropicEnvironmentType {
        config,

        startup_context: None,
        auth_context: None,
        session_mode: SessionMode::New,
    }
}

/// 验证完整配置字段解码。
#[test]
fn test_decode_config() {
    let config = decode_config(
        br#"{
            "environment_type":"anthropic",
            "version":"1",
            "cwd":"/workspace/project",
            "init_script":"echo setup",
            "environment":{"FOO":"bar"},
            "languages":[{"name":"node","version":"22"}],
            "allowed_tools":"Bash",
            "disallowed_tools":"WebFetch"
        }"#,
    )
    .unwrap();
    assert_eq!(config.environment_type, "anthropic");
    assert_eq!(config.cwd, "/workspace/project");
    assert_eq!(config.environment["FOO"], "bar");
    assert_eq!(
        config.languages,
        vec![Language {
            name: "node".to_owned(),
            version: "22".to_owned(),
        }]
    );
    assert_eq!(config.allowed_tools, "Bash");
    assert_eq!(config.disallowed_tools, "WebFetch");
}

/// 验证无效 JSON、字段类型和正式字段名约束。
#[test]
fn test_decode_config_errors() {
    assert!(
        decode_config(b"{")
            .unwrap_err()
            .to_string()
            .starts_with("failed to deserialize anthropic config:")
    );
    assert_eq!(
        decode_config(br#"{"environment_type":"anthropic"}"#)
            .unwrap_err()
            .to_string(),
        "anthropic config missing required cwd"
    );
    let config = decode_config(
        br#"{
            "CWD":"/ignored",
            "cwd":"/formal",
            "unknown":true
        }"#,
    )
    .unwrap();
    assert_eq!(config.cwd, "/formal");

    for data in [
        b"null".as_slice(),
        br#"{"environment_type":null,"cwd":"/tmp/x"}"#.as_slice(),
        br#"{"cwd":"/one","cwd":"/two"}"#.as_slice(),
        br#"{"cwd":"/tmp/x","languages":[null]}"#.as_slice(),
    ] {
        assert!(
            decode_config(data)
                .unwrap_err()
                .to_string()
                .starts_with("failed to deserialize anthropic config:")
        );
    }
}

/// 验证构造器与三个生命周期 setter 保存调用方提供的值。
#[test]
fn test_new_and_accessors() {
    let mut environment = super::new(
        br#"{"environment_type":"anthropic","cwd":"/workspace/project","environment":{"FOO":"bar"}}"#,
    )
    .unwrap();
    assert_eq!(environment.get_cwd(), "/workspace/project");
    let mut copied = environment.get_claude_environment_variables();
    copied.insert("FOO".to_owned(), "changed".to_owned());
    assert_eq!(environment.get_claude_environment_variables()["FOO"], "bar");
    environment.set_startup_context(Some(StartupContext {
        environment_sub_type: "baku".to_owned(),
        ..StartupContext::default()
    }));
    let mut auth_context = crate::internal::auth::AuthContext::default();
    auth_context.set_session_id("session-1");
    environment.set_auth_context(Some(auth_context));
    environment.set_session_mode(SessionMode::Resume);
    assert_eq!(
        environment
            .auth_context
            .as_ref()
            .map(crate::internal::auth::AuthContext::get_session_id),
        Some("session-1")
    );
    assert_eq!(environment.session_mode, SessionMode::Resume);
}

/// 验证 Claude 环境变量 getter 返回独立副本，调用方修改不会污染配置源。
#[test]
fn test_get_claude_environment_variables_returns_copy() {
    let environment = environment(Some(Config {
        environment: [("FOO".to_owned(), "bar".to_owned())].into_iter().collect(),
        ..Config::default()
    }));

    let mut copied = environment.get_claude_environment_variables();
    copied.insert("FOO".to_owned(), "changed".to_owned());
    copied.insert("NEW".to_owned(), "value".to_owned());

    let source = &environment.config.as_ref().unwrap().environment;
    assert_eq!(source.get("FOO").map(String::as_str), Some("bar"));
    assert!(!source.contains_key("NEW"));
}

/// 验证没有 startup context 时不是 Baku，subtype 为 baku 时才返回 true。
#[test]
fn test_is_baku_environment() {
    let mut environment = environment(Some(Config::default()));
    assert!(!environment.is_baku_environment());
    environment.set_startup_context(Some(StartupContext {
        environment_sub_type: "baku".to_owned(),
        ..StartupContext::default()
    }));
    assert!(environment.is_baku_environment());
}

/// 验证 `None` 配置 返回稳定错误。
#[tokio::test]
async fn test_initialize_missing_config_returns_error() {
    let mut environment = environment(None);
    let error = environment
        .initialize(&CancellationToken::new(), None)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "environment configuration is required");
}

/// 验证 setup 路径创建 CWD、运行脚本并完整停止活动。
#[tokio::test]
async fn test_initialize_runs_setup_path() {
    // initialize 会修改 HOME 与进程 CWD，必须在独立 libtest 子进程中完成并恢复。
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_initialize_runs_setup_path"
    )) {
        return;
    }
    let temp = TempDir::new().unwrap();
    let home = temp.path().join("home");
    fs::create_dir(&home).unwrap();
    let (_lock, _guard) = ProcessStateGuard::enter(&home).await;
    let workspace = temp.path().join("workspace");
    let mut environment = environment(Some(Config {
        cwd: workspace.to_string_lossy().into_owned(),
        init_script: "#!/bin/sh\nprintf initialized > marker.txt\n".to_owned(),
        ..Config::default()
    }));
    let recorder = RecordingActivityRecorder::default();
    environment
        .initialize(&CancellationToken::new(), Some(&recorder))
        .await
        .unwrap();
    assert_eq!(
        fs::read(workspace.join("marker.txt")).unwrap(),
        b"initialized"
    );
    assert_eq!(
        *recorder.long_running_messages.lock().unwrap(),
        ["Running task setup script"]
    );
    assert_eq!(recorder.stop_count.load(Ordering::SeqCst), 1);
}

/// 验证 resume-cached 跳过语言与 init script setup。
#[tokio::test]
async fn test_initialize_skips_setup_on_resume_cached() {
    if !crate::internal::enter_isolated_process_test(concat!(
        module_path!(),
        "::test_initialize_skips_setup_on_resume_cached"
    )) {
        return;
    }
    let temp = TempDir::new().unwrap();
    let home = temp.path().join("home");
    fs::create_dir(&home).unwrap();
    let (_lock, _guard) = ProcessStateGuard::enter(&home).await;
    let workspace = temp.path().join("workspace");
    let mut environment = environment(Some(Config {
        cwd: workspace.to_string_lossy().into_owned(),
        init_script: "#!/bin/sh\nprintf should-not-run > marker.txt\n".to_owned(),
        languages: vec![Language {
            name: "unsupported".to_owned(),
            version: "1".to_owned(),
        }],
        ..Config::default()
    }));
    environment.session_mode = SessionMode::ResumeCached;
    environment
        .initialize(&CancellationToken::new(), None)
        .await
        .unwrap();
    assert!(!workspace.join("marker.txt").exists());
}

/// 验证 hook 字节、创建 mode 与已有文件保留语义。
#[tokio::test]
async fn test_bootstrap_hooks_under_dir_writes_files_and_preserves_existing() {
    let temp = TempDir::new().unwrap();
    let environment = environment(Some(Config::default()));
    let config = HookConfig {
        settings_data: br#"{"hooks":true}"#.to_vec(),
        hook_name: "stop-hook-test.sh".to_owned(),
        hook_data: b"#!/bin/sh\nexit 0\n".to_vec(),
    };
    environment
        .bootstrap_hooks_under_dir(&CancellationToken::new(), temp.path(), &config)
        .await
        .unwrap();
    let settings = temp.path().join(".claude/settings.json");
    let hook = temp.path().join(".claude/stop-hook-test.sh");
    assert_eq!(fs::read(&settings).unwrap(), config.settings_data);
    assert_eq!(fs::read(&hook).unwrap(), config.hook_data);
    assert_eq!(
        fs::metadata(&settings).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(&hook).unwrap().permissions().mode() & 0o777,
        0o755
    );
    fs::write(&settings, b"existing-settings").unwrap();
    fs::write(&hook, b"existing-hook").unwrap();
    environment
        .bootstrap_hooks_under_dir(&CancellationToken::new(), temp.path(), &config)
        .await
        .unwrap();
    assert_eq!(fs::read(settings).unwrap(), b"existing-settings");
    assert_eq!(fs::read(hook).unwrap(), b"existing-hook");
}

/// 验证 Supabase 两份环境文件的字节和 mode。
#[tokio::test]
async fn test_write_supabase_env_files_writes_project_env_files() {
    let auth = new_auth_context_with_session_id(
        br#"[{"type":"supabase","token":"{\"project_ref\":\"project-ref\",\"anon_key\":\"anon-key\",\"db_pass\":\"db-pass\",\"pat\":\"pat-token\"}"}]"#,
        "session-id",
    )
    .unwrap();
    let mut environment = environment(Some(Config::default()));
    environment.auth_context = Some(auth);
    let temp = TempDir::new().unwrap();
    environment
        .write_supabase_env_files(&CancellationToken::new(), temp.path())
        .await
        .unwrap();
    assert_eq!(
        fs::read(temp.path().join(".env")).unwrap(),
        b"VITE_SUPABASE_URL=https://project-ref.supabase.co\nVITE_SUPABASE_ANON_KEY=anon-key\n"
    );
    assert_eq!(
        fs::read(temp.path().join(".env.local")).unwrap(),
        b"DATABASE_URL=postgresql://postgres:db-pass@db.project-ref.supabase.co:5432/postgres\n"
    );
    for path in [temp.path().join(".env"), temp.path().join(".env.local")] {
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

/// 验证初始化脚本合并捕获 stdout 与 stderr。
#[tokio::test]
async fn test_run_init_script_success() {
    let environment = environment(Some(Config::default()));
    let output = environment
        .run_init_script(
            &CancellationToken::new(),
            "#!/bin/sh\necho hello\nprintf err >&2\n",
        )
        .await
        .unwrap();
    assert!(output.contains("hello"));
    assert!(output.contains("err"));
}

/// 验证初始化脚本非零退出时同时保留退出码包装和已捕获输出。
#[tokio::test]
async fn test_run_init_script_failure() {
    let environment = environment(Some(Config::default()));
    let error = environment
        .run_init_script(
            &CancellationToken::new(),
            "#!/bin/sh\necho before failure\nexit 7\n",
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("init script failed (exit code 7)")
    );
    assert!(error.output.contains("before failure"));
    assert_eq!(error.exit_code, Some(7));
}

/// 验证空脚本无法执行时返回 execute 包装且不虚构输出。
#[tokio::test]
async fn test_run_init_script_execute_error() {
    let environment = environment(Some(Config::default()));
    let empty = environment
        .run_init_script(&CancellationToken::new(), "")
        .await
        .unwrap_err();
    assert!(empty.to_string().contains("failed to execute init script"));
    assert!(empty.output.is_empty());
    assert_eq!(empty.exit_code, None);
}

/// 验证环形输出缓冲只保留最后 4096 字节，并留下最终 `tail` 标记。
#[tokio::test]
async fn test_run_init_script_keeps_last_output_chunk() {
    // init 脚本依赖 PATH 查找 head/tr；防止其他测试临时清空 PATH 造成假失败。
    let _process_state = PROCESS_STATE_TEST_LOCK.lock().await;
    let environment = environment(Some(Config::default()));
    let output = environment
        .run_init_script(
            &CancellationToken::new(),
            "#!/bin/sh\nhead -c 5000 /dev/zero | tr '\\000' a\nprintf tail\n",
        )
        .await
        .unwrap();
    assert_eq!(output.len(), 4096);
    assert!(output.ends_with("tail"));
}

/// 验证取消只终止脚本 shell，重定向 pipe 的孙进程仍存活。
#[tokio::test]
async fn cancellation_kills_only_direct_script_child() {
    // fixture 的 sleep/mv 都由 shell 经 PATH 解析；锁覆盖孙进程启动和最终清理。
    let _process_state = PROCESS_STATE_TEST_LOCK.lock().await;
    let environment = environment(Some(Config::default()));
    let temp = TempDir::new().unwrap();
    let pid_file = temp.path().join("grandchild.pid");
    let script = format!(
        "#!/bin/sh\nsleep 30 >/dev/null 2>&1 &\nprintf '%s' \"$!\" > '{0}.tmp'\nmv '{0}.tmp' '{0}'\nwhile :; do :; done\n",
        pid_file.display(),
    );
    let cancellation = CancellationToken::new();
    let child_token = cancellation.clone();
    let task = tokio::spawn(async move {
        environment
            .run_init_script(&child_token, &script)
            .await
            .unwrap_err()
    });
    for _ in 0..100 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pid: i32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kill(Pid::from_raw(pid), None::<Signal>), Ok(()));
    let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
}

/// 验证空语言列表直接成功，不启动任何安装子进程。
#[tokio::test]
async fn test_install_languages_with_no_languages() {
    let environment = environment(Some(Config::default()));
    environment
        .install_languages(&CancellationToken::new())
        .await
        .unwrap();
}

/// 验证未知语言被明确视为可成功跳过的分支。
#[tokio::test]
async fn test_install_language_skips_unsupported_language() {
    let environment = environment(Some(Config::default()));
    environment
        .install_language(
            &CancellationToken::new(),
            &Language {
                name: "java".to_owned(),
                version: "21".to_owned(),
            },
        )
        .await
        .unwrap();
}

/// 验证目录、文件 mode 和相对 symlink 目标均被复制。
#[test]
fn test_copy_dir_copies_files_directories_and_symlinks() {
    let source = TempDir::new().unwrap();
    let destination_parent = TempDir::new().unwrap();
    let destination = destination_parent.path().join("dst");
    fs::create_dir(source.path().join("nested")).unwrap();
    let file = source.path().join("nested/file.txt");
    fs::write(&file, b"hello").unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o640)).unwrap();
    std::os::unix::fs::symlink("nested/file.txt", source.path().join("link.txt")).unwrap();
    copy_dir(source.path(), &destination).unwrap();
    assert_eq!(
        fs::read(destination.join("nested/file.txt")).unwrap(),
        b"hello"
    );
    assert_eq!(
        fs::metadata(destination.join("nested/file.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o640
    );
    assert_eq!(
        fs::read_link(destination.join("link.txt")).unwrap(),
        PathBuf::from("nested/file.txt")
    );
}

/// 验证源目录遍历失败时保留 `failed to copy directory` 上下文包装。
#[test]
fn test_copy_dir_wraps_walk_error() {
    let source = TempDir::new().unwrap();
    let destination_parent = TempDir::new().unwrap();
    let error = copy_dir(
        &source.path().join("missing"),
        &destination_parent.path().join("missing-dst"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("failed to copy directory"));
}

/// 验证 Git 初始化顺序、提交主题和本地配置。
#[tokio::test]
async fn test_init_git_repo_initializes_repository() {
    let _process_state = PROCESS_STATE_TEST_LOCK.lock().await;
    if Command::new("git").arg("--version").output().is_err() {
        return;
    }
    let temp = TempDir::new().unwrap();
    fs::write(temp.path().join("README.md"), b"# Baku\n").unwrap();
    let environment = environment(Some(Config::default()));
    environment
        .init_git_repo(&CancellationToken::new(), temp.path())
        .await
        .unwrap();
    assert_eq!(
        git_output(temp.path(), &["log", "--format=%s", "-1"]),
        "Initial project from Baku template"
    );
    assert_eq!(
        git_output(temp.path(), &["config", "--get", "user.email"]),
        "claude@anthropic.com"
    );
    assert_eq!(
        git_output(temp.path(), &["config", "--get", "user.name"]),
        "Claude"
    );
    assert_eq!(
        git_output(temp.path(), &["config", "--get", "gc.auto"]),
        "0"
    );
    assert_eq!(
        git_output(temp.path(), &["config", "--get", "commit.gpgsign"]),
        "false"
    );
}

/// 运行单条测试 Git 查询并返回去空白文本。
fn git_output(directory: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(directory)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

/// 验证 9199 开放时 supervisord 探针返回 true。
#[tokio::test]
async fn test_is_supervisord_running_detects_open_port() {
    let listener = match TcpListener::bind("127.0.0.1:9199").await {
        Ok(listener) => listener,
        Err(_) => return,
    };
    let environment = environment(Some(Config::default()));
    assert!(environment.is_supervisord_running().await);
    drop(listener);
}

/// 验证 supervisord 已监听时开发服务器启动路径不会再创建子进程。
#[tokio::test]
async fn test_start_dev_server_skips_when_supervisord_already_running() {
    let listener = match TcpListener::bind("127.0.0.1:9199").await {
        Ok(listener) => listener,
        Err(_) => return,
    };
    let environment = environment(Some(Config::default()));
    environment
        .start_dev_server(&CancellationToken::new(), TempDir::new().unwrap().path())
        .await
        .unwrap();
    drop(listener);
}
