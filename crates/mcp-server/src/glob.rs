//! 自定义 glob 语法和 rg 文件遍历；支持递归、POSIX 字符类与 extglob。
use super::ToolResult;
use crate::util;
use serde_json::Value;
use std::{
    collections::{BTreeSet, HashMap},
    path::Path,
};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
enum Node {
    Literal(char),
    Any,
    Star,
    Class {
        negated: bool,
        items: Vec<ClassItem>,
    },
    Group(char, Vec<Vec<Node>>),
}
#[derive(Clone, Debug)]
enum ClassItem {
    Range(char, char),
    Posix(String),
}
struct Parser {
    input: Vec<char>,
    pos: usize,
}
impl Parser {
    /// 解析一段不跨越目录分隔符的 glob 节点。
    fn sequence(&mut self, group: bool) -> Result<Vec<Node>, String> {
        let mut out = Vec::new();
        while self.pos < self.input.len() {
            let c = self.input[self.pos];
            if group && matches!(c, '|' | ')') {
                break;
            }
            if c == '\\' {
                self.pos += 1;
                if self.pos >= self.input.len() {
                    return Err("trailing escape".into());
                }
                out.push(Node::Literal(self.input[self.pos]));
                self.pos += 1;
                continue;
            }
            if "?*+@!".contains(c) && self.input.get(self.pos + 1) == Some(&'(') {
                self.pos += 2;
                let mut alts = Vec::new();
                loop {
                    alts.push(self.sequence(true)?);
                    if self.pos >= self.input.len() {
                        return Err(format!("unclosed extglob {c}("));
                    }
                    let end = self.input[self.pos];
                    self.pos += 1;
                    if end == ')' {
                        break;
                    }
                }
                out.push(Node::Group(c, alts));
                continue;
            }
            self.pos += 1;
            out.push(match c {
                '*' => Node::Star,
                '?' => Node::Any,
                '[' => self.class()?,
                _ => Node::Literal(c),
            });
        }
        Ok(out)
    }
    /// 解析字符范围和 ASCII POSIX 字符类。
    fn class(&mut self) -> Result<Node, String> {
        let negated = self
            .input
            .get(self.pos)
            .is_some_and(|c| matches!(c, '!' | '^'));
        if negated {
            self.pos += 1;
        }
        let mut tokens: Vec<(ClassItem, bool)> = Vec::new();
        while self.pos < self.input.len() {
            let mut c = self.input[self.pos];
            if c == ']' && !tokens.is_empty() {
                self.pos += 1;
                let mut items = Vec::new();
                let mut i = 0;
                while i < tokens.len() {
                    if i + 2 < tokens.len()
                        && let (
                            ClassItem::Range(start, _),
                            ClassItem::Range('-', '-'),
                            ClassItem::Range(end, _),
                        ) = (&tokens[i].0, &tokens[i + 1].0, &tokens[i + 2].0)
                        && !tokens[i + 1].1
                    {
                        if start > end {
                            return Err(format!(
                                "invalid character range {}-{}",
                                quote_char(*start),
                                quote_char(*end)
                            ));
                        }
                        items.push(ClassItem::Range(*start, *end));
                        i += 3;
                        continue;
                    }
                    items.push(tokens[i].0.clone());
                    i += 1;
                }
                return Ok(Node::Class { negated, items });
            }
            if c == '[' && self.input.get(self.pos + 1) == Some(&':') {
                let start = self.pos + 2;
                let mut end = start;
                while end + 1 < self.input.len()
                    && (self.input[end] != ':' || self.input[end + 1] != ']')
                {
                    end += 1;
                }
                if end + 1 >= self.input.len() {
                    return Err("invalid POSIX character class".into());
                }
                let name: String = self.input[start..end].iter().collect();
                if ![
                    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower",
                    "print", "punct", "space", "upper", "word", "xdigit",
                ]
                .contains(&name.as_str())
                {
                    return Err("invalid POSIX character class".into());
                }
                tokens.push((ClassItem::Posix(name), false));
                self.pos = end + 2;
                continue;
            }
            let escaped = c == '\\';
            if escaped {
                self.pos += 1;
                if self.pos >= self.input.len() {
                    return Err("trailing escape in character class".into());
                }
                c = self.input[self.pos];
            }
            tokens.push((ClassItem::Range(c, c), escaped));
            self.pos += 1;
        }
        Err("unclosed character class".into())
    }
}
/// 错误文本中的字符使用单引号表示。
fn quote_char(c: char) -> String {
    format!("'{c}'")
}
/// 匹配 POSIX 字符类的 ASCII 范围。
fn posix(name: &str, c: char) -> bool {
    match name {
        "alnum" => c.is_ascii_alphanumeric(),
        "alpha" => c.is_ascii_alphabetic(),
        "ascii" => c.is_ascii(),
        "blank" => matches!(c, ' ' | '\t'),
        "cntrl" => c.is_ascii_control(),
        "digit" => c.is_ascii_digit(),
        "graph" => c.is_ascii_graphic(),
        "lower" => c.is_ascii_lowercase(),
        "print" => (' '..='~').contains(&c),
        "punct" => c.is_ascii_punctuation(),
        "space" => " \t\r\n\u{b}\u{c}".contains(c),
        "upper" => c.is_ascii_uppercase(),
        "word" => c.is_ascii_alphanumeric() || c == '_',
        "xdigit" => c.is_ascii_hexdigit(),
        _ => false,
    }
}
/// 计算节点序列从指定位置可到达的所有结束位置。
fn sequence(nodes: &[Node], input: &[char], start: usize) -> BTreeSet<usize> {
    let mut positions = BTreeSet::from([start]);
    for n in nodes {
        let mut next = BTreeSet::new();
        for p in positions {
            next.extend(node(n, input, p));
        }
        positions = next;
        if positions.is_empty() {
            break;
        }
    }
    positions
}
/// 合并 extglob 各分支的匹配结束位置。
fn alternatives(alts: &[Vec<Node>], input: &[char], start: usize) -> BTreeSet<usize> {
    alts.iter()
        .flat_map(|a| sequence(a, input, start))
        .collect()
}
/// 计算单节点匹配，包括不会死循环的重复 extglob。
fn node(n: &Node, input: &[char], start: usize) -> BTreeSet<usize> {
    match n {
        Node::Literal(c) => {
            if input.get(start) == Some(c) {
                BTreeSet::from([start + 1])
            } else {
                BTreeSet::new()
            }
        }
        Node::Any => {
            if start < input.len() {
                BTreeSet::from([start + 1])
            } else {
                BTreeSet::new()
            }
        }
        Node::Star => (start..=input.len()).collect(),
        Node::Class { negated, items } => {
            let matched = input.get(start).is_some_and(|c| {
                items.iter().any(|i| match i {
                    ClassItem::Range(a, b) => a <= c && c <= b,
                    ClassItem::Posix(name) => posix(name, *c),
                }) != *negated
            });
            if matched {
                BTreeSet::from([start + 1])
            } else {
                BTreeSet::new()
            }
        }
        Node::Group(op, alts) => {
            let mut one = alternatives(alts, input, start);
            match op {
                '@' => one,
                '?' => {
                    one.insert(start);
                    one
                }
                '!' => (start..=input.len()).filter(|p| !one.contains(p)).collect(),
                '*' | '+' => {
                    let mut out = BTreeSet::new();
                    if *op == '*' {
                        out.insert(start);
                    }
                    let mut queue = vec![start];
                    let mut visited = BTreeSet::new();
                    while let Some(p) = queue.pop() {
                        if !visited.insert(p) {
                            continue;
                        }
                        for e in alternatives(alts, input, p) {
                            out.insert(e);
                            if e > p {
                                queue.push(e);
                            }
                        }
                    }
                    out
                }
                _ => BTreeSet::new(),
            }
        }
    }
}
struct Glob {
    negated: bool,
    segments: Vec<Option<Vec<Node>>>,
}
/// 拆分前导逻辑否定，extglob 的 !( 保持原样。
fn negation(mut p: &str) -> (&str, bool) {
    let mut neg = false;
    while p.starts_with('!') && !p.starts_with("!(") {
        neg = !neg;
        p = &p[1..];
    }
    (p, neg)
}
impl Glob {
    /// 编译 glob，每个 ** 目录段独立处理。
    fn compile(pattern: &str) -> Result<Self, String> {
        let mut p = pattern;
        while let Some(rest) = p.strip_prefix("./") {
            p = rest;
        }
        let (p, negated) = negation(p);
        if p.is_empty() {
            return Err("empty glob pattern after negation".into());
        }
        let mut segments = Vec::new();
        for part in p.split('/') {
            if part == "**" {
                segments.push(None);
            } else {
                let mut parser = Parser {
                    input: part.chars().collect(),
                    pos: 0,
                };
                segments.push(Some(parser.sequence(false)?));
            }
        }
        Ok(Self { negated, segments })
    }
    /// 通过目录层级记忆化避免 ** 产生重复递归。
    fn matches(&self, mut candidate: &str) -> bool {
        while let Some(rest) = candidate.strip_prefix("./") {
            candidate = rest;
        }
        let parts: Vec<Vec<char>> = candidate.split('/').map(|s| s.chars().collect()).collect();
        self.match_at(&parts, 0, 0, &mut HashMap::new())
    }
    /// 在目录段之间进行递归匹配。
    fn match_at(
        &self,
        parts: &[Vec<char>],
        i: usize,
        j: usize,
        memo: &mut HashMap<(usize, usize), bool>,
    ) -> bool {
        if let Some(v) = memo.get(&(i, j)) {
            return *v;
        }
        let v = if i == self.segments.len() {
            j == parts.len()
        } else if let Some(nodes) = &self.segments[i] {
            j < parts.len()
                && sequence(nodes, &parts[j], 0).contains(&parts[j].len())
                && self.match_at(parts, i + 1, j + 1, memo)
        } else {
            self.match_at(parts, i + 1, j, memo)
                || j < parts.len() && self.match_at(parts, i, j + 1, memo)
        };
        memo.insert((i, j), v);
        v
    }
}
/// 提取下一对可展开花括号，忽略转义及不可识别的范围。
fn brace(p: &str) -> Result<Option<(usize, usize, Vec<String>)>, String> {
    let b = p.as_bytes();
    let mut escaped = false;
    for (open, &c) in b.iter().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == b'\\' {
            escaped = true;
            continue;
        }
        if c != b'{' {
            continue;
        }
        let mut depth = 1;
        let mut start = open + 1;
        let mut alts = Vec::new();
        let mut escape = false;
        for i in open + 1..b.len() {
            if escape {
                escape = false;
                continue;
            }
            match b[i] {
                b'\\' => escape = true,
                b'{' => depth += 1,
                b',' if depth == 1 => {
                    alts.push(p[start..i].to_owned());
                    start = i + 1;
                }
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        if !alts.is_empty() {
                            alts.push(p[start..i].to_owned());
                            return Ok(Some((open, i, alts)));
                        }
                        if let Some(values) = numeric(&p[start..i])? {
                            return Ok(Some((open, i, values)));
                        }
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(None)
}
/// 展开数字范围，保留补零宽度和方向。
fn numeric(s: &str) -> Result<Option<Vec<String>>, String> {
    let parts: Vec<_> = s.split("..").collect();
    if !matches!(parts.len(), 2 | 3) {
        return Ok(None);
    }
    let (Ok(start), Ok(end)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>()) else {
        return Ok(None);
    };
    let mut step = if parts.len() == 3 {
        let Ok(n) = parts[2].parse::<i64>() else {
            return Ok(None);
        };
        n
    } else {
        1
    };
    if step == 0 {
        return Err("numeric brace range step cannot be zero".into());
    }
    step = step.wrapping_abs();
    if start > end {
        step = step.wrapping_neg();
    }
    let a = parts[0].trim_start_matches('-');
    let b = parts[1].trim_start_matches('-');
    let width = if a.len() > 1 && a.starts_with('0') || b.len() > 1 && b.starts_with('0') {
        a.len().max(b.len())
    } else {
        0
    };
    let mut out = Vec::new();
    let mut v = start;
    loop {
        if out.len() >= 256 {
            return Err("brace expansion exceeds 256 patterns".into());
        }
        out.push(if width == 0 {
            v.to_string()
        } else if v < 0 {
            format!("-{:0width$}", v.wrapping_abs())
        } else {
            format!("{v:0width$}")
        });
        if v == end
            || step > 0 && v > end.wrapping_sub(step)
            || step < 0 && v < end.wrapping_sub(step)
        {
            break;
        }
        v = v.wrapping_add(step);
    }
    Ok(Some(out))
}
/// 深度优先展开花括号，最多生成 256 个模式。
fn expand(p: &str, out: &mut Vec<String>) -> Result<(), String> {
    if let Some((open, close, alts)) = brace(p)? {
        for a in alts {
            expand(&format!("{}{a}{}", &p[..open], &p[close + 1..]), out)?;
        }
    } else {
        if out.len() >= 256 {
            return Err("brace expansion exceeds 256 patterns".into());
        }
        out.push(p.to_owned());
    }
    Ok(())
}
/// 找到首个未转义的模式元字符。
fn first_meta(p: &str) -> Option<usize> {
    let mut escaped = false;
    for (i, c) in p.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if "*?[{".contains(c) || "+@!".contains(c) && p.as_bytes().get(i + 1) == Some(&b'(') {
            return Some(i);
        }
    }
    None
}
/// 将绝对模式和含点目录模式转换成搜索根与相对模式。
fn normalize(work: &Path, pattern: &str) -> (std::path::PathBuf, String) {
    let (p, neg) = negation(pattern);
    let mut dir = work.to_owned();
    let mut p = p.to_owned();
    let restore = |p: String| if neg { format!("!{p}") } else { p };
    if !Path::new(&p).is_absolute() {
        if !p.split('/').any(|s| matches!(s, "." | "..")) {
            return (dir, restore(p));
        }
        p = util::clean(work.join(&p)).to_string_lossy().into_owned();
    }
    if let Some(rel) = util::relative(&dir, Path::new(&p))
        && rel != "."
        && rel != ".."
        && !rel.starts_with("../")
        && !Path::new(&rel).is_absolute()
    {
        return (dir, restore(rel));
    }
    let split = first_meta(&p).and_then(|i| p[..i].rfind('/'));
    if first_meta(&p).is_none() {
        dir = Path::new(&p).parent().unwrap_or(Path::new(".")).to_owned();
        p = Path::new(&p)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
    } else if let Some(i) = split {
        dir = if i == 0 { "/".into() } else { p[..i].into() };
        p = p[i + 1..].into();
    }
    (dir, restore(p))
}
/// 使用 rg 的修改时间升序文件列表，再应用完整 glob 匹配。
pub async fn run(work: &Path, cancel: CancellationToken, a: &Value) -> ToolResult {
    let pattern = util::string(a, "pattern");
    if pattern.is_empty() {
        return ToolResult::error("pattern is required");
    }
    let dir = util::resolve(work, util::string(a, "path"));
    let (dir, pattern) = normalize(&dir, pattern);
    let mut patterns = Vec::new();
    if let Err(e) = expand(&pattern, &mut patterns) {
        return ToolResult::error(format!("glob pattern: {e}"));
    }
    let mut matchers = Vec::new();
    for p in &patterns {
        match Glob::compile(p) {
            Ok(m) => matchers.push(m),
            Err(e) => {
                return ToolResult::error(format!(
                    "glob pattern {}: {e}",
                    serde_json::to_string(&pattern).unwrap()
                ));
            }
        }
    }
    let full = patterns
        .iter()
        .any(|p| negation(p).1 || p.contains('[') || has_extglob(p));
    let executable = match util::executable("rg").await {
        Ok(p) => p,
        Err(e) => return ToolResult::error(format!("glob (rg) err: {e}: ")),
    };
    let mut cmd = Command::new(&executable);
    cmd.arg("--files");
    if !full {
        for p in &patterns {
            cmd.arg("-g").arg(p);
        }
    }
    cmd.args(["--sort=modified", "--no-ignore", "--hidden"])
        .current_dir(&dir)
        .kill_on_drop(true);
    let result = tokio::select! {_=cancel.cancelled()=>return ToolResult::error("glob (rg) err: signal: killed: "),v=cmd.output()=>v};
    let out = match result {
        Ok(o) => o,
        Err(e) => {
            return ToolResult::error(format!(
                "glob (rg) err: {}: ",
                if !dir.exists() {
                    util::io_error("chdir", &dir, &e)
                } else {
                    format!("fork/exec {}: {}", executable.display(), util::errno(&e))
                }
            ));
        }
    };
    let code = out.status.code().unwrap_or(-1);
    if code != 0 && code != 1 {
        return ToolResult::error(format!(
            "glob (rg) err: exit status {code}: {}",
            crate::util::lossy(&out.stderr)
        ));
    }
    let text = crate::util::lossy(&out.stdout);
    let mut files: Vec<_> = text
        .trim_end_matches('\n')
        .split('\n')
        .filter(|s| !s.is_empty())
        .filter(|s| matches_set(&matchers, s))
        .collect();
    let total = files.len();
    files.truncate(100);
    if files.is_empty() {
        return ToolResult::text("No files found");
    }
    let mut text = files.join("\n");
    if total > 100 {
        text.push_str("\n(Results are truncated. Consider using a more specific path or pattern.)");
    }
    ToolResult::text(util::truncate(text, "glob").await)
}
/// 判断是否包含未转义的 extglob 起始标记。
fn has_extglob(p: &str) -> bool {
    let mut escaped = false;
    for (i, c) in p.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
        } else if "?*+@!".contains(c) && p.as_bytes().get(i + 1) == Some(&b'(') {
            return true;
        }
    }
    false
}
/// 负模式优先排除，其余正模式取并集。
fn matches_set(gs: &[Glob], s: &str) -> bool {
    let (mut positive, mut found) = (false, false);
    for g in gs {
        let matched = g.matches(s);
        if g.negated && matched {
            return false;
        }
        if !g.negated {
            positive = true;
            found |= matched;
        }
    }
    !positive || found
}
