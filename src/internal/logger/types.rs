//! 日志记录、级别、handler trait 与轻量 logger facade。
//!
//! 类型采用 owned 数据，确保记录可以安全地跨线程、缓存和 fan-out；属性值使用
//! `serde_json::Value`，从而能无损写入 JSONL 文件而无需 `Any` 或运行时 downcast。

use std::{
    collections::HashMap,
    error::Error,
    panic::Location,
    path::Path,
    sync::{Arc, OnceLock},
    time::SystemTime,
};

/// 构建时固化的 Go/Rust 符号对照表。
///
/// `std::panic::Location` 能稳定提供调用文件和行号，却没有公开调用函数名。迁移
/// 已经要求维护这份符号表，因此在二进制中复用它解析函数，而不依赖 release 构建
/// 会剥离的 DWARF/符号表，也不需要在每个业务日志调用点改成宏。
const GO_RUST_SYMBOLS: &str = include_str!("../../../compat/go-rust-symbols.csv");

/// 固定 Go 基线中每个生产日志调用点的 `slog.Source` 映射。
///
/// 该文件由 `scripts/compat/generate_log_source_map.py` 生成并由 Linux ARM64
/// 新鲜度门禁校验。运行时只接受固定 Go 1.25.7 已确认的 `mapped-oracle` 行；任何
/// 静态预测或未决歧义都不会悄悄退回 Rust 路径，从而避免把近似结果误称为精确兼容。
const GO_LOG_SOURCE_MAP: &str = include_str!("../../../compat/go-rust-log-sources.tsv");

/// 按 Rust 文件分组的函数起始行索引；值直接借用编译期嵌入的 CSV 文本。
static CALLER_SYMBOL_INDEX: OnceLock<HashMap<&'static str, Vec<(u32, &'static str)>>> =
    OnceLock::new();

/// 按 Rust runtime caller 文件分组的 Go 日志 source 规则。
static GO_LOG_SOURCE_INDEX: OnceLock<HashMap<String, Vec<GoLogSourceRule>>> = OnceLock::new();

/// 一条从 Rust runtime caller 到固定 Go `slog.Source` 的判别规则。
#[derive(Debug)]
struct GoLogSourceRule {
    /// `Location::caller` 返回的一基行号。
    runtime_line: u32,
    /// 空字符串表示任意级别，否则必须与 `LogLevel::as_str` 相等。
    level: String,
    /// 消息的匹配方式：`exact`、`suffix` 或 `any`。
    match_kind: String,
    /// 已经从 TSV JSON string 解码的消息或后缀。
    message: String,
    /// 必须存在的属性键；值不参与判断。
    required_attribute_keys: Vec<String>,
    /// 必须与记录属性完全相等的判别值。
    required_attribute_values: LogAttributes,
    /// 固定 Go 1.25.7 产物报告的函数名。
    function: String,
    /// `-trimpath` 后固定 Go module 路径形式的文件名。
    file: String,
    /// Go logger 调用表达式的一基行号。
    line: u32,
}

/// handler 失败的线程安全动态错误。
pub type LogHandlerError = Box<dyn Error + Send + Sync + 'static>;

/// 稳定排序的结构化日志级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogLevel {
    /// 极细粒度诊断。
    Trace,
    /// 开发调试信息。
    Debug,
    /// 正常运行信息。
    Info,
    /// 可恢复异常。
    Warn,
    /// 操作失败。
    Error,
}

impl LogLevel {
    /// 返回与 Go slog JSON 输出一致的大写级别名。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

/// 保留调用顺序的结构化日志属性。
///
/// Go `slog.JSONHandler` 按 `LogAttrs` 的传入顺序输出属性；文件 handler 随后会把
/// 同一批属性放进 `map[string]any`，在那里才按 key 排序。这里因此使用启用了
/// `preserve_order` 的 `serde_json::Map`，由两个下游分别实现各自的顺序语义。
pub type LogAttributes = serde_json::Map<String, serde_json::Value>;

/// 与 Go `slog.Source` JSON 形状一致的调用位置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSource {
    /// 生产记录为固定 Go runtime 函数名；测试记录为 Rust 函数名。
    pub function: String,
    /// 生产记录为 Go `-trimpath` 文件名；测试记录为 Rust 编译时源文件路径。
    pub file: String,
    /// 对应语言中 logger 调用表达式的一基行号。
    pub line: u32,
}

/// 一条拥有全部数据的结构化日志记录。
#[derive(Debug, Clone)]
pub struct LogRecord {
    /// 事件产生时间，而非文件写入时间。
    pub time: SystemTime,
    /// 事件级别。
    pub level: LogLevel,
    /// 人类可读消息。
    pub message: String,
    /// 由 logger facade 捕获的原始业务调用位置；手工构造的记录可以没有来源。
    pub source: Option<LogSource>,
    /// 机器可读属性。
    pub attributes: LogAttributes,
}

impl LogRecord {
    /// 以当前时间创建一条记录。
    pub fn new(level: LogLevel, message: impl Into<String>, attributes: LogAttributes) -> Self {
        Self {
            time: SystemTime::now(),
            level,
            message: message.into(),
            source: None,
            attributes,
        }
    }

    /// 把 `#[track_caller]` 传递下来的调用位置解析成 Go `slog.Source`。
    ///
    /// # 参数
    /// - `location`：Rust 编译器捕获的实际 facade 调用文件与行号。
    /// - `source_attributes`：当前记录与 `Logger::with_attrs` 绑定属性的只读合并视图；
    ///   仅用于选择 source，不会写回记录或改变最终 JSON 属性。
    ///
    /// # 兼容性
    /// 生产源码必须命中经过审计的 Go 映射。未命中或仍有歧义时写入显眼的
    /// `<unmapped-go-log-source>`，绝不退回看似合理但错误的 Rust source。测试源码
    /// 不属于 Go 外部行为，仍使用 Rust caller 方便定位失败断言。
    fn set_caller(
        &mut self,
        location: &'static Location<'static>,
        source_attributes: &LogAttributes,
    ) {
        self.source = Some(resolve_log_source(
            location.file(),
            location.line(),
            self.level,
            &self.message,
            source_attributes,
        ));
    }
}

/// 根据 runtime caller、级别、消息和属性解析唯一 Go source。
///
/// 匹配优先级依次为精确消息、消息后缀和通配；同类规则中，级别和属性谓词越
/// 具体优先级越高。若最高优先级仍指向两个不同 Go 调用点，则按未映射处理，
/// 因为任选其一会把生成器遗漏的歧义隐藏到生产日志中。
fn resolve_log_source(
    file: &str,
    line: u32,
    level: LogLevel,
    message: &str,
    attributes: &LogAttributes,
) -> LogSource {
    let rust_path = normalized_rust_path(file);
    if !is_production_rust_source(rust_path) {
        return LogSource {
            function: caller_function(file, line),
            file: absolute_source_file(file),
            line,
        };
    }

    let mut best_score = None;
    let mut selected: Option<&GoLogSourceRule> = None;
    let mut ambiguous = false;
    if let Some(rules) = go_log_source_index().get(rust_path) {
        for rule in rules {
            let Some(score) = rule.match_score(line, level, message, attributes) else {
                continue;
            };
            match best_score {
                None => {
                    best_score = Some(score);
                    selected = Some(rule);
                    ambiguous = false;
                }
                Some(previous) if score > previous => {
                    best_score = Some(score);
                    selected = Some(rule);
                    ambiguous = false;
                }
                Some(previous) if score == previous => {
                    if selected.is_some_and(|current| !current.same_destination(rule)) {
                        ambiguous = true;
                    }
                }
                Some(_) => {}
            }
        }
    }

    if let Some(rule) = selected.filter(|_| !ambiguous) {
        return LogSource {
            function: rule.function.clone(),
            file: rule.file.clone(),
            line: rule.line,
        };
    }

    LogSource {
        function: "<unmapped-go-log-source>".to_owned(),
        file: format!("unmapped://{rust_path}"),
        line,
    }
}

impl GoLogSourceRule {
    /// 判断当前记录是否满足规则，并返回用于选择最具体规则的稳定分数。
    fn match_score(
        &self,
        line: u32,
        level: LogLevel,
        message: &str,
        attributes: &LogAttributes,
    ) -> Option<usize> {
        if self.runtime_line != line || (!self.level.is_empty() && self.level != level.as_str()) {
            return None;
        }
        let message_score = match self.match_kind.as_str() {
            "exact" if message == self.message => 300,
            "suffix" if message.ends_with(&self.message) => 200,
            "any" => 100,
            _ => return None,
        };
        if !self
            .required_attribute_keys
            .iter()
            .all(|key| attributes.contains_key(key))
        {
            return None;
        }
        if !self
            .required_attribute_values
            .iter()
            .all(|(key, expected)| attributes.get(key) == Some(expected))
        {
            return None;
        }

        Some(
            message_score
                + usize::from(!self.level.is_empty()) * 10
                + self.required_attribute_keys.len()
                + self.required_attribute_values.len() * 2,
        )
    }

    /// 判断两条同优先级规则是否最终指向完全相同的 Go source。
    fn same_destination(&self, other: &Self) -> bool {
        self.function == other.function && self.file == other.file && self.line == other.line
    }
}

/// 判断 caller 是否来自会出现在发布二进制中的手写生产源码。
fn is_production_rust_source(rust_path: &str) -> bool {
    rust_path.starts_with("src/")
        && !rust_path.ends_with("_test.rs")
        && !rust_path.starts_with("src/gen/")
}

/// 首次写日志时解析嵌入 TSV，并缓存所有已经完成审计的规则。
fn go_log_source_index() -> &'static HashMap<String, Vec<GoLogSourceRule>> {
    GO_LOG_SOURCE_INDEX.get_or_init(|| {
        let mut index: HashMap<String, Vec<GoLogSourceRule>> = HashMap::new();
        for row in GO_LOG_SOURCE_MAP.lines().skip(1) {
            let columns = row.split('\t').collect::<Vec<_>>();
            if columns.len() != 14 || columns[0] != "rule" || columns[10] != "mapped-oracle" {
                continue;
            }
            let (Ok(runtime_line), Ok(line)) =
                (columns[2].parse::<u32>(), columns[9].parse::<u32>())
            else {
                continue;
            };
            let Ok(message) = serde_json::from_str::<String>(columns[5]) else {
                continue;
            };
            let (required_attribute_keys, required_attribute_values) =
                parse_attribute_predicate(columns[6]);
            index
                .entry(columns[1].to_owned())
                .or_default()
                .push(GoLogSourceRule {
                    runtime_line,
                    level: columns[3].to_owned(),
                    match_kind: columns[4].to_owned(),
                    message,
                    required_attribute_keys,
                    required_attribute_values,
                    function: columns[7].to_owned(),
                    file: columns[8].to_owned(),
                    line,
                });
        }
        index
    })
}

/// 解析生成器写入的属性谓词。
///
/// JSON object 表示键和值都必须匹配；JSON string array 或逗号列表只要求键存在。
/// 当前静态规则多数为空，但该格式用于区分合并 wrapper 中消息相同的 Go 分支。
fn parse_attribute_predicate(value: &str) -> (Vec<String>, LogAttributes) {
    if value.is_empty() {
        return (Vec::new(), LogAttributes::new());
    }
    if let Ok(values) = serde_json::from_str::<LogAttributes>(value) {
        return (Vec::new(), values);
    }
    if let Ok(values) = serde_json::from_str::<Vec<String>>(value) {
        return (values, LogAttributes::new());
    }
    (
        value
            .split(',')
            .filter(|key| !key.is_empty())
            .map(str::to_owned)
            .collect(),
        LogAttributes::new(),
    )
}

/// 把编译器返回的相对源路径扩展为与 Go runtime caller 相同类别的绝对路径。
fn absolute_source_file(file: &str) -> String {
    if Path::new(file).is_absolute() {
        file.to_owned()
    } else {
        format!("{}/{file}", env!("CARGO_MANIFEST_DIR"))
    }
}

/// 根据调用文件和行号解析完整 Rust 函数名。
fn caller_function(file: &str, line: u32) -> String {
    let rust_path = normalized_rust_path(file);
    let module = rust_module_path(rust_path);
    let symbol = caller_symbol_index().get(rust_path).and_then(|symbols| {
        // 每个文件中的条目在初始化时已按起始行排序；`partition_point` 找到
        // 最后一个不晚于调用行的函数，正好对应包含该日志表达式的外层函数。
        let next = symbols.partition_point(|(start_line, _)| *start_line <= line);
        next.checked_sub(1).map(|index| symbols[index].1)
    });

    match symbol {
        Some(symbol) => format!("environment_manager::{module}::{symbol}"),
        None => format!("environment_manager::{module}::<caller_at_line_{line}>"),
    }
}

/// 统一编译器可能给出的相对路径和容器内绝对路径。
fn normalized_rust_path(file: &str) -> &str {
    if let Some(index) = file.rfind("/src/") {
        &file[index + 1..]
    } else {
        file.trim_start_matches("./")
    }
}

/// 把 `src/foo/bar.rs` 转为 Rust crate 内的模块路径。
fn rust_module_path(rust_path: &str) -> String {
    let path = rust_path
        .strip_prefix("src/")
        .unwrap_or(rust_path)
        .strip_suffix(".rs")
        .unwrap_or(rust_path);
    let path = path.strip_suffix("/mod").unwrap_or(path);
    path.replace('/', "::")
}

/// 首次写日志时解析嵌入的符号 CSV，并缓存按文件分组的有序索引。
fn caller_symbol_index() -> &'static HashMap<&'static str, Vec<(u32, &'static str)>> {
    CALLER_SYMBOL_INDEX.get_or_init(|| {
        let mut index: HashMap<&'static str, Vec<(u32, &'static str)>> = HashMap::new();
        for row in GO_RUST_SYMBOLS.lines().skip(1) {
            // 前八列的 path、kind、symbol、line 和 status 都不会包含逗号；detail
            // 允许包含逗号，因此仅限制为九段，避免无意义地解析最后一列。
            let columns = row.splitn(9, ',').collect::<Vec<_>>();
            if columns.len() < 8 || columns[7] != "migrated" {
                continue;
            }
            let rust_path = columns[4];
            let Ok(rust_line) = columns[5].parse::<u32>() else {
                continue;
            };
            let rust_symbol = columns[6];
            if rust_path.is_empty() || rust_symbol.is_empty() {
                continue;
            }
            index
                .entry(rust_path)
                .or_default()
                .push((rust_line, rust_symbol));
        }
        for symbols in index.values_mut() {
            symbols.sort_unstable_by_key(|(line, _)| *line);
        }
        index
    })
}

/// 可组合的结构化日志处理器。
pub trait LogHandler: Send + Sync {
    /// 判断给定级别是否值得构造和处理记录。
    fn enabled(&self, level: LogLevel) -> bool;

    /// 处理一条已经构造好的记录。
    fn handle(&self, record: &LogRecord) -> Result<(), LogHandlerError>;

    /// 派生一个自动附加固定属性的 handler。
    fn with_attrs(&self, attributes: LogAttributes) -> Arc<dyn LogHandler>;

    /// 派生一个把后续属性置于命名 JSON group 下的 handler。
    fn with_group(&self, name: &str) -> Arc<dyn LogHandler>;
}

/// 与 `slog.Logger` 对应的可克隆 facade。
#[derive(Clone)]
pub struct Logger {
    handler: Arc<dyn LogHandler>,
    /// 由 `with_attrs` 绑定、但不会重复写入 `LogRecord` 的 source 判别上下文。
    source_attributes: LogAttributes,
}

impl Logger {
    /// 从任意 handler 构造 logger。
    pub fn new(handler: Arc<dyn LogHandler>) -> Self {
        Self {
            handler,
            source_attributes: LogAttributes::new(),
        }
    }

    /// 判断给定级别当前是否启用。
    pub fn enabled(&self, level: LogLevel) -> bool {
        self.handler.enabled(level)
    }

    /// 向底层 handler 发送一条结构化记录。
    ///
    /// 与 Go `slog.Logger` 一致，handler 错误不会让业务流程失败，也不会由 logger
    /// 再次写入 stderr；需要观察错误的调用方应直接调用 `LogHandler::handle`。
    #[track_caller]
    pub fn log(&self, level: LogLevel, message: impl Into<String>, attributes: LogAttributes) {
        if !self.handler.enabled(level) {
            return;
        }
        let mut record = LogRecord::new(level, message, attributes);
        // Go 的 `logger.With(...)` 属性已经存在于最终 handler 输出，却不属于当前
        // `slog.Record.Attrs`。source 映射仍需看到这些属性，才能区分复用同一 Rust
        // helper 的两个 Go 方法；因此只在判别副本中合并，不能重复写入 record。
        let mut source_attributes = self.source_attributes.clone();
        source_attributes.extend(record.attributes.clone());
        record.set_caller(Location::caller(), &source_attributes);
        let _ = self.handler.handle(&record);
    }

    /// 记录 debug 事件。
    #[track_caller]
    pub fn debug(&self, message: impl Into<String>, attributes: LogAttributes) {
        self.log(LogLevel::Debug, message, attributes);
    }

    /// 记录 info 事件。
    #[track_caller]
    pub fn info(&self, message: impl Into<String>, attributes: LogAttributes) {
        self.log(LogLevel::Info, message, attributes);
    }

    /// 记录 warning 事件。
    #[track_caller]
    pub fn warn(&self, message: impl Into<String>, attributes: LogAttributes) {
        self.log(LogLevel::Warn, message, attributes);
    }

    /// 记录 error 事件。
    #[track_caller]
    pub fn error(&self, message: impl Into<String>, attributes: LogAttributes) {
        self.log(LogLevel::Error, message, attributes);
    }

    /// 返回绑定固定属性的新 logger。
    pub fn with_attrs(&self, attributes: LogAttributes) -> Self {
        let mut source_attributes = self.source_attributes.clone();
        source_attributes.extend(attributes.clone());
        Self {
            handler: self.handler.with_attrs(attributes),
            source_attributes,
        }
    }

    /// 返回进入指定属性 group 的新 logger。
    pub fn with_group(&self, name: &str) -> Self {
        Self {
            handler: self.handler.with_group(name),
            source_attributes: self.source_attributes.clone(),
        }
    }

    /// 暴露 handler 供组合器复用，不转移 logger 所有权。
    pub fn handler(&self) -> Arc<dyn LogHandler> {
        Arc::clone(&self.handler)
    }
}
