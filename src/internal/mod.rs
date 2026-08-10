//! Go `internal` 目录对应的 Rust 模块根。
//!
//! 这里保持原包名和目录关系，跨模块交互通过显式类型和 trait 完成；迁移阶段不会为了
//! 追求新的目录分层而打散 Go 文件，从而确保后续可以按路径和符号直接对照。

pub mod api;
pub mod auth;
pub mod claude;
pub mod config;
pub mod envtype;
pub mod gitproxy;
pub mod input;
pub mod logger;
pub mod manager;
pub mod mcp;
pub mod o11y;
pub mod orchestrator;
pub mod packages;
pub mod podmonitor;
pub mod process;
pub mod sandbox;
pub mod session;
pub mod sources;
pub mod tunnel;
pub mod util;

/// 测试中修改进程全局 CWD/HOME/PATH 等状态时使用的跨模块串行锁。
///
/// Rust 测试默认并行运行；各模块若各自持锁仍会互相删除当前工作目录，造成与业务逻辑
/// 无关的随机失败。该锁只在测试构建中存在，不进入生产二进制或运行时热路径。
#[cfg(test)]
pub(crate) static PROCESS_STATE_TEST_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

/// 把会修改进程环境或当前目录的单元测试放到独立测试进程中执行。
///
/// Rust 2024 要求 `set_var`、`remove_var` 等操作不得与其他线程读取环境并发；仅用
/// Mutex 串行“写测试”并不充分，因为普通测试启动子进程时也会读取并复制整个环境。
/// 调用方应在任何全局状态读取或写入之前调用本函数，并传入
/// `concat!(module_path!(), "::测试函数名")`。
///
/// 父测试进程会用当前 libtest 可执行文件和 `--exact` 只启动该测试，并通过私有
/// marker 防止递归；子进程返回 `true`，表示调用方可以继续执行测试主体。父进程在
/// 子测试成功后返回 `false`，调用方应立即 `return`。失败时 stdout/stderr 会随 panic
/// 回传，保留原始断言位置和诊断信息。
///
/// # 参数
/// - `qualified_test_name`：包含 crate 名的完整测试路径，通常由 `module_path!()` 生成。
///
/// # 返回
/// 子进程内返回 `true`；已经代为运行子测试的父进程返回 `false`。
///
/// # 副作用
/// 父进程会启动当前测试二进制并等待结束；不修改父进程环境或当前目录。子进程只会
/// 继承 marker，测试主体对环境和目录的改动不会泄漏回默认并行 libtest 进程。
#[cfg(test)]
#[track_caller]
pub(crate) fn enter_isolated_process_test(qualified_test_name: &str) -> bool {
    use std::ffi::OsStr;
    use std::process::Command;

    const MARKER: &str = "ENVIRONMENT_MANAGER_ISOLATED_TEST";
    const CONFIRMATION: &str = "ENVIRONMENT_MANAGER_ISOLATED_TEST_CONFIRMATION";
    let crate_prefix = concat!(env!("CARGO_CRATE_NAME"), "::");
    let test_name = qualified_test_name
        .strip_prefix(crate_prefix)
        .expect("隔离测试名称必须以当前 crate 名开头");

    match std::env::var_os(MARKER) {
        Some(active) if active == OsStr::new(test_name) => {
            let confirmation =
                std::env::var_os(CONFIRMATION).expect("隔离测试子进程必须收到握手文件路径");
            std::fs::write(confirmation, test_name.as_bytes())
                .expect("隔离测试子进程必须写入握手文件");
            return true;
        }
        Some(active) => {
            panic!("isolated test marker mismatch: active={active:?}, requested={test_name:?}")
        }
        None => {}
    }

    let executable = std::env::current_exe().expect("必须定位当前 libtest 可执行文件");
    // 单凭 libtest 的成功退出码无法区分“测试通过”和“过滤器没有匹配任何测试”。
    // 握手文件由真正进入测试主体的子进程写入，防止路径拼错时产生假阳性。
    let confirmation = tempfile::NamedTempFile::new().expect("必须创建隔离测试握手文件");
    let output = Command::new(executable)
        .args([
            "--exact",
            test_name,
            "--nocapture",
            "--include-ignored",
            "--test-threads=1",
        ])
        .env(MARKER, test_name)
        .env(CONFIRMATION, confirmation.path())
        .output()
        .expect("必须启动隔离单元测试子进程");
    assert!(
        output.status.success(),
        "隔离单元测试失败：{test_name}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let observed = std::fs::read(confirmation.path()).unwrap_or_else(|error| {
        panic!(
            "无法读取隔离测试握手：{test_name}: {error}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
    });
    if observed != test_name.as_bytes() {
        panic!(
            "隔离测试过滤器没有执行预期用例：{test_name}\nobserved ACK: {:?}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&observed),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    false
}
