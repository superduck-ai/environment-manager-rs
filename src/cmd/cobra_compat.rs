//! Cobra 命令行文本与错误顺序兼容层。
//!
//! 对应 Go `main.go` 以及 `cmd/cmd_*.go` 中由 Cobra/pflag 自动生成的外部文本。
//! Clap 仍负责类型化解析，但它的 help、usage、completion 与错误措辞不是稳定的
//! Cobra 协议，因此这里使用固定 Go 1.25.7 ARM64 oracle 生成的文本资产统一渲染。
//! 资产由 `scripts/compat/generate_cobra_assets.py` 生成，不在运行时读取测试文件。

use std::ffi::OsString;

use clap::error::ErrorKind;

use super::completion::CompletionShell;
use super::error::{CliError, RuntimeError};

const ROOT_HELP: &str = include_str!("cobra_assets/root_help.txt");
const COMPLETION_HELP: &str = include_str!("cobra_assets/completion_help.txt");
const ORCHESTRATOR_HELP: &str = include_str!("cobra_assets/orchestrator_help.txt");
const TASK_RUN_HELP: &str = include_str!("cobra_assets/task_run_help.txt");
const POLL_HELP: &str = include_str!("cobra_assets/poll_help.txt");
const SETUP_HELP: &str = include_str!("cobra_assets/setup_help.txt");
const PRINT_SANDBOX_SETTINGS_HELP: &str =
    include_str!("cobra_assets/print_sandbox_settings_help.txt");
const COMPLETION_BASH_HELP: &str = include_str!("cobra_assets/completion_bash_help.txt");
const COMPLETION_FISH_HELP: &str = include_str!("cobra_assets/completion_fish_help.txt");
const COMPLETION_POWERSHELL_HELP: &str =
    include_str!("cobra_assets/completion_powershell_help.txt");
const COMPLETION_ZSH_HELP: &str = include_str!("cobra_assets/completion_zsh_help.txt");
const WORKING_DIRECTORY_MARKER: &str = "{{WORKING_DIRECTORY}}";
const CODE_SIGN_USAGE: &str =
    "Usage:\n  environment-runner code-sign [flags]\n\nFlags:\n  -h, --help   help for code-sign\n";
const COBRA_SUGGESTIONS_MINIMUM_DISTANCE: usize = 2;
// 这里必须保留 Go `main.go` 调用 `Add*Command` 的注册顺序。Cobra 的
// `SuggestionsFor` 直接遍历内部命令切片，不会像帮助模板那样先按名称排序；默认
// completion 命令在 `ExecuteC` 中最后注入，因此也必须放在数组末尾。
const ROOT_SUGGESTION_COMMANDS: [&str; 6] = [
    "orchestrator",
    "task-run",
    "poll",
    "print-sandbox-settings",
    "setup",
    "completion",
];

/// 可由 Cobra 在错误路径自动打印 usage 的命令范围。
///
/// 该枚举同时供二进制入口使用，避免入口用字符串选择 usage 时出现拼写漂移。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CobraCommand {
    /// 根命令。
    Root,
    /// Cobra 自动注入的 completion 父命令。
    Completion,
    /// orchestrator 子命令。
    Orchestrator,
    /// task-run 子命令。
    TaskRun,
    /// poll 子命令。
    Poll,
    /// setup 子命令。
    Setup,
    /// print-sandbox-settings 子命令。
    PrintSandboxSettings,
    /// 隐藏的 code-sign 子命令。
    CodeSign,
}

/// 返回由 Go oracle 固化的命令帮助文本。
///
/// task-run 的默认工作目录来自进程 `HOME`，不能把生成资产时的目录写死；只有该
/// 占位符会在运行时替换，其余空格、tab、换行和 flag 顺序全部保持 oracle 原样。
#[must_use]
pub fn render_help(command: CobraCommand) -> String {
    let template = match command {
        CobraCommand::Root => ROOT_HELP,
        CobraCommand::Completion => COMPLETION_HELP,
        CobraCommand::Orchestrator => ORCHESTRATOR_HELP,
        CobraCommand::TaskRun => TASK_RUN_HELP,
        CobraCommand::Poll => POLL_HELP,
        CobraCommand::Setup => SETUP_HELP,
        CobraCommand::PrintSandboxSettings => PRINT_SANDBOX_SETTINGS_HELP,
        // code-sign 设置了 DisableFlagParsing；`code-sign --help` 会作为普通参数进入
        // RunE，所以公开路径没有独立 help，只在错误时使用 usage。
        CobraCommand::CodeSign => CODE_SIGN_USAGE,
    };
    replace_working_directory(template)
}

/// 返回 completion 父命令或 shell 叶子命令的 Cobra 帮助文本。
#[must_use]
pub fn render_completion_help(shell: Option<CompletionShell>) -> String {
    let template = match shell {
        None => COMPLETION_HELP,
        Some(CompletionShell::Bash) => COMPLETION_BASH_HELP,
        Some(CompletionShell::Fish) => COMPLETION_FISH_HELP,
        Some(CompletionShell::PowerShell) => COMPLETION_POWERSHELL_HELP,
        Some(CompletionShell::Zsh) => COMPLETION_ZSH_HELP,
    };
    template.to_owned()
}

/// 把一个 RunE 错误包装成 Cobra `Execute` 加 Go `main` 的完整 stderr。
///
/// Cobra 先打印 `Error:`、usage，随后把同一错误返回给 `main.go`；main 又打印一次
/// `Error:`。这个看似重复的顺序已经属于外部协议，不能为了美观而合并。错误文本
/// 可以含多行，首尾均原样保留，仅在三个阶段之间补 Cobra 固定换行。
#[must_use]
pub fn render_runtime_error(command: CobraCommand, error: &str) -> String {
    let usage = render_usage(command);
    format!("Error: {error}\n{usage}\nError: {error}\n")
}

/// 把可能不是 UTF-8 的 RunE 错误包装成 Cobra `Execute` 与 Go `main` 的完整 stderr。
///
/// 输出顺序与 [`render_runtime_error`] 完全一致：Cobra 首次错误、对应子命令 usage、
/// `main.go` 再次错误。错误正文会出现两次且逐字节复制；usage 是仓库内固定 UTF-8
/// 资产。该函数不做终端转义，也不会把 NUL 或无效 UTF-8 替换掉。
#[must_use]
pub fn render_runtime_error_bytes(command: CobraCommand, error: &[u8]) -> Vec<u8> {
    let usage = render_usage(command);
    let mut rendered = Vec::with_capacity(error.len() * 2 + usage.len() + 25);
    rendered.extend_from_slice(b"Error: ");
    rendered.extend_from_slice(error);
    rendered.push(b'\n');
    rendered.extend_from_slice(usage.as_bytes());
    rendered.extend_from_slice(b"\nError: ");
    rendered.extend_from_slice(error);
    rendered.push(b'\n');
    rendered
}

/// 生成 task-run 运行时错误的最终 stderr 字节与退出码。
///
/// 普通错误继续使用 Cobra 的双 `Error:`/usage 外壳并以 1 退出。V0 缺失 startup
/// context 的受控 Go runtime panic 已经包含完整 transcript，必须直接返回且以 2
/// 退出；该分支绝不能经过字符串格式化或追加 Cobra 文本。
#[must_use]
pub fn render_task_run_runtime_error(error: &RuntimeError) -> (Vec<u8>, i32) {
    let rendered = if error.writes_directly_to_stderr() {
        error.as_bytes().to_vec()
    } else {
        render_runtime_error_bytes(CobraCommand::TaskRun, error.as_bytes())
    };
    (rendered, error.exit_code())
}

/// 在 Clap 解析前识别 Cobra 的帮助、无子命令和 completion 父命令提前退出。
///
/// `code-sign` 使用 DisableFlagParsing，因此即使参数中出现 `--help` 也不能在这里
/// 截获。其他已知命令只要显式请求 `-h/--help` 就返回对应 oracle 文本；`help`
/// 自动子命令也映射到同一资产。
pub(super) fn requested_help(argv: &[OsString]) -> Option<String> {
    let arguments = lossy_arguments(argv);
    if arguments.is_empty() {
        return Some(ROOT_HELP.to_owned());
    }
    if arguments[0] == "code-sign" {
        return None;
    }
    if arguments[0] == "help" {
        return help_command_target(&arguments[1..]);
    }
    if arguments.len() == 1 && arguments[0] == "completion" {
        return Some(render_completion_help(None));
    }
    if !arguments
        .iter()
        .any(|argument| argument == "--help" || argument == "-h")
    {
        return None;
    }
    match arguments[0].as_str() {
        "--help" | "-h" => Some(ROOT_HELP.to_owned()),
        "completion" => completion_help_target(arguments.get(1).map(String::as_str)),
        "orchestrator" => Some(render_help(CobraCommand::Orchestrator)),
        "task-run" => Some(render_help(CobraCommand::TaskRun)),
        "poll" => Some(render_help(CobraCommand::Poll)),
        "setup" => Some(render_help(CobraCommand::Setup)),
        "print-sandbox-settings" => Some(render_help(CobraCommand::PrintSandboxSettings)),
        _ => None,
    }
}

/// 在 Clap 解析前识别根命令版本 flag，并使用 Cobra 的 `version` 固定措辞。
pub(super) fn requested_version(version: &str, argv: &[OsString]) -> Option<String> {
    let arguments = lossy_arguments(argv);
    arguments
        .first()
        .is_some_and(|argument| argument == "--version" || argument == "-v")
        .then(|| format!("environment-runner version {version}\n"))
}

/// 把 Clap 参数错误翻译为 Cobra/pflag 的错误、usage 与双 Error 顺序。
///
/// raw argv 用于恢复 pflag 报错中的原值和 flag 名；绝不从 Clap 的彩色终端文本中
/// 猜测用户参数。无法安全分类的分支仍保留 Clap 的首行消息，但也使用正确命令的
/// Cobra usage，确保未知新分支显式出现在差分报告中而不是被吞掉。
pub(super) fn render_clap_error(argv: &[OsString], error: &clap::Error) -> CliError {
    let arguments = lossy_arguments(argv);
    let command = command_for_arguments(&arguments);
    let message = match error.kind() {
        ErrorKind::InvalidSubcommand => {
            // Cobra 对“有子命令但自身不可运行”的 completion 父命令有一个容易忽略的
            // 回退：未知 shell 会作为普通位置参数留给父命令，随后因父命令没有 Run
            // 而打印父帮助并成功退出。Clap 会更早报 InvalidSubcommand，因此必须在
            // 错误翻译层恢复 Cobra 的成功型帮助，不能把 `completion` 误报成根未知命令。
            if arguments
                .first()
                .is_some_and(|argument| argument == "completion")
            {
                if let Some(message) = completion_unknown_flag_message(&arguments) {
                    return CliError::usage_rendered(render_runtime_error(
                        CobraCommand::Completion,
                        &message,
                    ));
                }
                return CliError::help(render_completion_help(None));
            }
            let unknown = arguments.first().map_or("", String::as_str);
            let suggestions = render_root_command_suggestions(unknown);
            let message =
                format!("unknown command {unknown:?} for \"environment-runner\"{suggestions}");
            // Cobra 的首轮打印额外附加 usage 提示，但 Execute 返回给 main 的 error
            // 不含该提示；建议块却属于 error 本身，因此前后两个 Error 都必须带上。
            // 建议块本身以换行结束，PrintErrln/Fprintf 还会各追加一次换行；这里故意
            // 保留建议与 Run 行之间以及最终 EOF 前的空行，不能把它们 trim 掉。
            return CliError::usage_rendered(format!(
                "Error: {message}\nRun 'environment-runner --help' for usage.\nError: {message}\n"
            ));
        }
        ErrorKind::UnknownArgument => unknown_argument_message(&arguments, error),
        ErrorKind::MissingRequiredArgument if command == CobraCommand::TaskRun => {
            "required flag(s) \"session\" not set".to_owned()
        }
        ErrorKind::ValueValidation => value_validation_message(&arguments, error),
        ErrorKind::DisplayHelp | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => {
            return CliError::help(requested_help(argv).unwrap_or_else(|| render_help(command)));
        }
        ErrorKind::DisplayVersion => {
            return CliError::version(error.to_string());
        }
        _ => first_clap_error_line(error),
    };
    CliError::usage_rendered(render_runtime_error(command, &message))
}

/// 复刻 Cobra v1.9.1 根命令 `findSuggestions` 生成的建议后缀。
///
/// 对应 Go：`github.com/spf13/cobra/command.go` 中的 `findSuggestions` 和
/// `SuggestionsFor`。候选只包含 Go 根命令中 `IsAvailableCommand()` 为真的命令；
/// 隐藏的 `code-sign` 不得泄露，自动注入且公开的 `completion` 必须保留；默认 help
/// 虽会显示在帮助模板中，但因它等于父命令的 `helpCommand` 而被明确排除。
///
/// # 参数
/// - `typed_name`：用户传入的首个未知根子命令，保持原始大小写。
///
/// # 返回
/// 没有候选时返回空字符串；存在候选时返回以两个换行开头、并以换行结尾的 Cobra
/// 建议块。调用方必须把它拼入 error 本身，使 Cobra 和 `main.go` 的两次打印都包含
/// 相同建议。
fn render_root_command_suggestions(typed_name: &str) -> String {
    let typed_name_lower = cobra_simple_lowercase(typed_name);
    let suggestions: Vec<&str> = ROOT_SUGGESTION_COMMANDS
        .iter()
        .copied()
        .filter(|candidate| {
            let candidate_lower = cobra_simple_lowercase(candidate);
            cobra_levenshtein_distance(typed_name_lower.as_bytes(), candidate_lower.as_bytes())
                <= COBRA_SUGGESTIONS_MINIMUM_DISTANCE
                || candidate_lower.starts_with(&typed_name_lower)
        })
        .collect();

    if suggestions.is_empty() {
        return String::new();
    }

    let mut rendered = String::from("\n\nDid you mean this?\n");
    for suggestion in suggestions {
        rendered.push('\t');
        rendered.push_str(suggestion);
        rendered.push('\n');
    }
    rendered
}

/// 按 Go `strings.ToLower` 使用的 Unicode simple lowercase 规则转换字符串。
///
/// Cobra 的 `ld(..., true)` 和前缀建议都调用 Go `strings.ToLower`；该函数为每个输入
/// rune 只产生一个小写 rune。Rust `str::to_lowercase` 使用 full lowercase，像 `İ`
/// 会展开成 `i` 加组合点，从而把 `PİLL` 到 `poll` 的字节距离错误增大。这里从每个
/// Rust lowercase iterator 只取首个字符，以复刻固定 Go 1.25.7 oracle 的 simple
/// mapping，再由距离函数按 UTF-8 字节比较。
///
/// # 参数
/// - `value`：Cobra 要参与不区分大小写比较的有效 UTF-8 字符串。
///
/// # 返回
/// 每个输入 Unicode scalar 对应至多一个小写 scalar 的新字符串。
fn cobra_simple_lowercase(value: &str) -> String {
    value
        .chars()
        .map(|character| character.to_lowercase().next().unwrap_or(character))
        .collect()
}

/// 计算 Cobra v1.9.1 `ld` 使用的逐 UTF-8 字节 Levenshtein 距离。
///
/// Cobra 先对两个字符串做小写转换，再按 Go 字符串的字节索引执行动态规划，而不是
/// 按 Unicode 字符或 grapheme 计算。调用方已经完成小写转换；本函数只接收字节切片，
/// 以免未来重构时无意改用 Rust `char` 并改变非 ASCII 输入的候选边界。
///
/// # 参数
/// - `left`：小写后的用户输入 UTF-8 字节。
/// - `right`：小写后的候选命令 UTF-8 字节。
///
/// # 返回
/// 把 `left` 通过单字节插入、删除或替换转换为 `right` 所需的最少操作次数。
fn cobra_levenshtein_distance(left: &[u8], right: &[u8]) -> usize {
    // Cobra 保存完整二维矩阵；这里只保留相邻两行以降低分配量，递推关系和字节索引
    // 完全相同，不会改变距离或 SuggestionsMinimumDistance=2 的边界。
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    let mut current = vec![0; right.len() + 1];

    for (left_index, left_byte) in left.iter().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_byte) in right.iter().enumerate() {
            let substitution_cost = usize::from(left_byte != right_byte);
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + substitution_cost);
        }
        std::mem::swap(&mut previous, &mut current);
    }

    previous[right.len()]
}

/// 从固定帮助资产提取 Cobra `UsageString()` 的准确后缀。
fn render_usage(command: CobraCommand) -> String {
    if command == CobraCommand::CodeSign {
        return CODE_SIGN_USAGE.to_owned();
    }
    let help = render_help(command);
    help.find("Usage:\n")
        .map_or(help.clone(), |position| help[position..].to_owned())
}

/// 用当前 HOME 语义替换 task-run 资产中的唯一动态默认值。
fn replace_working_directory(template: &str) -> String {
    if !template.contains(WORKING_DIRECTORY_MARKER) {
        return template.to_owned();
    }
    template.replace(
        WORKING_DIRECTORY_MARKER,
        &super::task_run::get_default_working_directory().to_string_lossy(),
    )
}

/// 将 `argv[0]` 之后的参数做仅用于 CLI 元数据判断的有损展示转换。
fn lossy_arguments(argv: &[OsString]) -> Vec<String> {
    argv.iter()
        .skip(1)
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect()
}

/// 解析 Cobra 自动 `help [command]` 的目标。
fn help_command_target(arguments: &[String]) -> Option<String> {
    match arguments.first().map(String::as_str) {
        None => Some(ROOT_HELP.to_owned()),
        Some("completion") => completion_help_target(arguments.get(1).map(String::as_str)),
        Some("orchestrator") => Some(render_help(CobraCommand::Orchestrator)),
        Some("task-run") => Some(render_help(CobraCommand::TaskRun)),
        Some("poll") => Some(render_help(CobraCommand::Poll)),
        Some("setup") => Some(render_help(CobraCommand::Setup)),
        Some("print-sandbox-settings") => Some(render_help(CobraCommand::PrintSandboxSettings)),
        _ => None,
    }
}

/// 选择 completion 父命令或具体 shell 的帮助资产。
fn completion_help_target(shell: Option<&str>) -> Option<String> {
    match shell {
        None | Some("--help" | "-h") => Some(render_completion_help(None)),
        Some("bash") => Some(render_completion_help(Some(CompletionShell::Bash))),
        Some("fish") => Some(render_completion_help(Some(CompletionShell::Fish))),
        Some("powershell") => Some(render_completion_help(Some(CompletionShell::PowerShell))),
        Some("zsh") => Some(render_completion_help(Some(CompletionShell::Zsh))),
        _ => None,
    }
}

/// 根据首个子命令选择发生错误时应显示的 usage。
fn command_for_arguments(arguments: &[String]) -> CobraCommand {
    match arguments.first().map(String::as_str) {
        Some("completion") => CobraCommand::Completion,
        Some("orchestrator") => CobraCommand::Orchestrator,
        Some("task-run") => CobraCommand::TaskRun,
        Some("poll") => CobraCommand::Poll,
        Some("setup") => CobraCommand::Setup,
        Some("print-sandbox-settings") => CobraCommand::PrintSandboxSettings,
        Some("code-sign") => CobraCommand::CodeSign,
        _ => CobraCommand::Root,
    }
}

/// 在 Clap 把未知 shell 分类为子命令错误时，优先恢复 Cobra 的父命令 flag 错误。
///
/// Cobra 会先把 `bsh` 留作 completion 父命令的位置参数，再解析随后 flag；因此
/// `completion bsh --bad` 不是成功帮助，而是 completion usage 包裹的 unknown flag。
/// `-h/--help` 是唯一允许继续走成功帮助的 flag，`--` 则只是位置参数分隔符。短
/// flag 必须按 pflag 的簇语义逐个消费：`-hh` 合法，而 `-hfalse` 在消费 `h` 后应
/// 把剩余的 `-false` 报为未知 shorthand，不能直接把整个参数交给 Clap 解释。
fn completion_unknown_flag_message(arguments: &[String]) -> Option<String> {
    for argument in arguments.iter().skip(1) {
        // pflag 遇到 `--` 后停止 flag 解析；后续以连字符开头的字节也只是普通参数。
        if argument == "--" {
            break;
        }
        // 单独的 `-` 在 pflag 中是普通位置参数，不是一个空 shorthand。
        if argument == "-" {
            continue;
        }
        if argument == "-h" || argument == "--help" {
            continue;
        }
        if let Some(value) = argument
            .strip_prefix("--help=")
            .or_else(|| argument.strip_prefix("-h="))
        {
            // pflag 的 bool flag 接受 strconv.ParseBool 的全部拼写；无论 true 还是
            // false，completion 父命令最终都会因自身不可运行而打印帮助。
            if is_go_boolean(value) {
                continue;
            }
            return Some(format!(
                "invalid argument {value:?} for \"-h, --help\" flag: strconv.ParseBool: parsing {value:?}: invalid syntax"
            ));
        }
        if let Some(long_flag) = argument.strip_prefix("--") {
            // 对应 pflag parseLongArg：第三个字节仍是 `-`，或名称以 `=` 开头，
            // 都属于语法错误；它们不是一个名称分别为 `-bad`/空串的未知 flag。
            if long_flag.starts_with(['-', '=']) {
                return Some(format!("bad flag syntax: {argument}"));
            }
            let flag = argument.split('=').next().unwrap_or(argument);
            return Some(format!("unknown flag: {flag}"));
        }
        if let Some(mut shorthand_cluster) = argument.strip_prefix('-') {
            while !shorthand_cluster.is_empty() {
                let shorthand = shorthand_cluster
                    .chars()
                    .next()
                    .expect("非空 shorthand 簇必须含有首字符");
                if shorthand == 'h' {
                    // help 是 completion 父命令唯一已注册的 shorthand，并且是
                    // NoOptDefVal=true 的 bool flag，所以不会吞掉簇中剩余字符。
                    shorthand_cluster = &shorthand_cluster[shorthand.len_utf8()..];
                    continue;
                }
                return Some(format!(
                    "unknown shorthand flag: {shorthand:?} in -{shorthand_cluster}"
                ));
            }
        }
    }
    None
}

/// 判断文本是否属于 Go `strconv.ParseBool` 接受的固定集合。
///
/// 该函数只用于 Cobra/pflag 帮助 flag 的兼容分流，不把其他大小写或空白宽松接受，
/// 避免非法值被误判成成功帮助并改变退出码。
fn is_go_boolean(value: &str) -> bool {
    matches!(
        value,
        "1" | "t" | "T" | "TRUE" | "true" | "True" | "0" | "f" | "F" | "FALSE" | "false" | "False"
    )
}

/// 恢复 Cobra 对未知 flag 使用的 `unknown flag` 措辞。
fn unknown_argument_message(arguments: &[String], error: &clap::Error) -> String {
    if let Some(flag) = arguments.iter().find(|argument| {
        argument.starts_with('-')
            && *argument != "-h"
            && *argument != "--help"
            && *argument != "-v"
            && *argument != "--version"
    }) {
        let flag = flag.split('=').next().unwrap_or(flag);
        return format!("unknown flag: {flag}");
    }
    first_clap_error_line(error)
}

/// 恢复首批高价值 pflag value 错误及运行期 log-level 校验文本。
fn value_validation_message(arguments: &[String], error: &clap::Error) -> String {
    if let Some((flag, value)) = argument_value(
        arguments,
        &[
            "--poll-timeout",
            "--loop-timeout",
            "--timeout-hook-timeout",
            "--execute-hook-timeout",
            "--poll-hook-timeout",
        ],
    ) {
        return format!(
            "invalid argument {value:?} for {flag:?} flag: time: invalid duration {value:?}"
        );
    }
    if let Some((flag, value)) = argument_value(
        arguments,
        &["--reclaim-older-than-ms", "--max-poll-failures"],
    ) {
        return format!(
            "invalid argument {value:?} for {flag:?} flag: strconv.ParseInt: parsing {value:?}: invalid syntax"
        );
    }
    if let Some((_, value)) = argument_value(arguments, &["--log-level"]) {
        return format!("invalid log level: {value} (valid: debug, info, warn, error)");
    }
    first_clap_error_line(error)
}

/// 从 `--flag value` 或 `--flag=value` 中提取 pflag 报错需要的原始值。
fn argument_value(arguments: &[String], flags: &[&str]) -> Option<(String, String)> {
    for (index, argument) in arguments.iter().enumerate() {
        for flag in flags {
            if argument == flag {
                return arguments
                    .get(index + 1)
                    .map(|value| ((*flag).to_owned(), value.clone()));
            }
            if let Some(value) = argument.strip_prefix(&format!("{flag}=")) {
                return Some(((*flag).to_owned(), value.to_owned()));
            }
        }
    }
    None
}

/// 去掉 Clap 首行的小写 `error:`，供尚未专门建模的新错误显式退化。
fn first_clap_error_line(error: &clap::Error) -> String {
    let rendered = error.to_string();
    rendered
        .lines()
        .next()
        .unwrap_or("unknown CLI parsing error")
        .strip_prefix("error: ")
        .unwrap_or_else(|| {
            rendered
                .lines()
                .next()
                .unwrap_or("unknown CLI parsing error")
        })
        .to_owned()
}
