#!/usr/bin/env python3
"""生成 Rust 日志记录到 Go ``slog.Source`` 的可审计映射。

该脚本只读取固定 Go 基线和 Rust 源码。它不会执行目标程序，也不会改写 Go
工作树。输出既是 Rust release 二进制嵌入的运行时映射，也是 Linux ARM64 门禁
检查调用点新鲜度和未决歧义的唯一清单。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable


MODULE_PATH = "github.com/anthropics/anthropic/api-go/environment-manager"
HEADER = (
    "kind",
    "runtime_path",
    "runtime_line",
    "level",
    "match_kind",
    "message_json",
    "attribute_keys",
    "go_function",
    "go_file",
    "go_line",
    "status",
    "origin_path",
    "origin_line",
    "detail_json",
)
# release runtime 与门禁都只接受固定 Go 1.25.7 `-trimpath` oracle 确认行；静态
# 猜测仅保留在 audit 清单中，绝不能被二进制 loader 当成可用 source。
RUNTIME_STATUSES = frozenset({"mapped-oracle"})
COMPLETE_STATUSES = frozenset({"mapped-oracle"})

# 这些文件的 `self.log(...)` 是显式 `#[track_caller]` 转发层。Rust 编译器会把
# `Location::caller()` 一直传播到具体业务调用表达式，因此运行时规则必须使用外层
# `self.log` 行号，而不能误用 wrapper 内部的 `logger.log` 行号。清单刻意很小；新增
# wrapper 必须先审核注解和调用链，不能只因方法同名就自动放宽 source 证明。
TRACK_CALLER_LOG_METHOD_PATHS = frozenset(
    {
        "src/internal/gitproxy/handler.rs",
        "src/internal/gitproxy/manager.rs",
        "src/internal/gitproxy/server.rs",
        "src/internal/podmonitor/lease_manager.rs",
        "src/internal/sources/git.rs",
    }
)


@dataclass(frozen=True)
class Call:
    """一个已经拆分参数的日志或日志 wrapper 调用。"""

    path: str
    line: int
    end_line: int
    receiver: str
    method: str
    arguments: tuple[str, ...]
    message_expression: str
    messages: tuple[str, ...]
    level: str


@dataclass(frozen=True)
class GoFunction:
    """Go 声明或函数 literal 的源码范围及 runtime 名称。"""

    start: int
    end: int
    function: str
    is_literal: bool


@dataclass(frozen=True)
class GoSite:
    """Go logger 调用及生产 ``slog.Source`` 三字段。"""

    path: str
    line: int
    end_line: int
    level: str
    messages: tuple[str, ...]
    function: str
    file: str
    in_literal: bool
    source_line: int
    oracle_confirmed: bool


def json_field(value: str) -> str:
    """把可能含 tab、换行或反斜线的字段编码成单行 JSON string。"""

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def split_arguments(source: str) -> tuple[str, ...]:
    """在字符串和嵌套括号之外按逗号拆分调用参数。"""

    result: list[str] = []
    start = 0
    depth = 0
    quote = ""
    raw_quote = False
    escaped = False
    index = 0
    while index < len(source):
        character = source[index]
        if quote:
            if raw_quote:
                if character == quote:
                    quote = ""
                    raw_quote = False
            elif escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = ""
        elif character in {'"', "'", "`"}:
            quote = character
            raw_quote = character == "`"
        elif character in "([{":
            depth += 1
        elif character in ")]}":
            depth -= 1
        elif character == "," and depth == 0:
            result.append(source[start:index].strip())
            start = index + 1
        index += 1
    result.append(source[start:].strip())
    return tuple(result)


def matching_parenthesis(source: str, opening: int) -> int | None:
    """返回调用左括号对应的右括号；忽略字符串内部的括号。"""

    depth = 0
    quote = ""
    raw_quote = False
    escaped = False
    index = opening
    while index < len(source):
        character = source[index]
        if quote:
            if raw_quote:
                if character == quote:
                    quote = ""
                    raw_quote = False
            elif escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = ""
        elif character in {'"', "'", "`"}:
            quote = character
            raw_quote = character == "`"
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return None


def decoded_strings(expression: str, language: str) -> tuple[str, ...]:
    """提取消息表达式内可确定的字符串分支，保持源码顺序并去重。"""

    if language == "go":
        pattern = re.compile(r'"(?:\\.|[^"\\])*"|`[^`]*`', re.DOTALL)
    else:
        pattern = re.compile(
            r'(?:br?|cr?)?r(?P<hashes>#{0,16})".*?"(?P=hashes)'
            r'|(?:b|c)?"(?:\\.|[^"\\])*"',
            re.DOTALL,
        )
    values: list[str] = []
    for match in pattern.finditer(expression):
        token = match.group(0)
        try:
            if language == "go" and token.startswith("`"):
                value = token[1:-1]
            elif "r" in token[: token.find('"')]:
                first_quote = token.find('"')
                hashes = token[:first_quote].count("#")
                value = token[first_quote + 1 : -(hashes + 1)]
            else:
                first_quote = token.find('"')
                value = json.loads(token[first_quote:])
        except (ValueError, json.JSONDecodeError):
            continue
        if value not in values:
            values.append(value)
    return tuple(values)


def normalized_message_template(value: str, language: str) -> str:
    """把 Rust format 字段和 Go fmt verb 归一化为同一占位符。"""

    if language == "rust":
        # 先保护转义花括号；`{name}`、`{}` 和 `{:?}` 都只代表一个 runtime 值。
        value = value.replace("{{", "\x00").replace("}}", "\x01")
        value = re.sub(r"\{[^{}]*\}", "{value}", value)
        return value.replace("\x00", "{").replace("\x01", "}")
    # 覆盖 fmt 常见的索引、flags、宽度、精度和 verb；`%%` 是字面百分号。
    value = value.replace("%%", "\x00")
    value = re.sub(
        r"%(?:\[[0-9]+\])?[+#\- 0]*(?:\*|[0-9]+)?"
        r"(?:\.(?:\*|[0-9]+))?(?:\[[0-9]+\])?[vTtbcdoOqxXUeEfFgGsp]",
        "{value}",
        value,
    )
    return value.replace("\x00", "%")


def messages_equivalent(rust_message: str, go_message: str) -> bool:
    """判断静态字面量或跨语言格式模板是否描述同一条消息。"""

    return rust_message == go_message or normalized_message_template(
        rust_message, "rust"
    ) == normalized_message_template(go_message, "go")


def rust_message_is_formatted(call: Call) -> bool:
    """判断调用消息会在 runtime 展开，不能再用模板文本做 exact 匹配。"""

    return re.search(r"\bformat!\s*\(", call.message_expression) is not None


def rust_attribute_keys(call: Call) -> tuple[str, ...]:
    """从常用属性 macro/array 中提取一定会存在的键。"""

    if call.method == "log":
        index = 2
    elif call.method in {"log_event", "log_with_logger"}:
        index = 3
    else:
        index = 1
    if len(call.arguments) <= index:
        return ()
    expression = call.arguments[index]
    keys = set(re.findall(r'\(\s*"([^"\\]+)"\s*,', expression))
    keys.update(re.findall(r'"([^"\\]+)"\s*=>', expression))
    return tuple(sorted(keys))


def encoded_attribute_keys(keys: Iterable[str]) -> str:
    """把属性键写为无歧义 JSON array，供 Rust resolver 做存在性匹配。"""

    values = sorted(set(keys))
    return json.dumps(values, ensure_ascii=False, separators=(",", ":")) if values else ""


def call_level(method: str, arguments: tuple[str, ...], language: str) -> str:
    """恢复固定日志级别；动态 ``Log`` 调用返回空字符串通配。"""

    normalized = method.removesuffix("Context")
    fixed = {
        "Debug": "DEBUG",
        "Info": "INFO",
        "Warn": "WARN",
        "Error": "ERROR",
        "debug": "DEBUG",
        "info": "INFO",
        "warn": "WARN",
        "error": "ERROR",
        "log_info": "INFO",
        "log_warn": "WARN",
        "log_warning": "WARN",
        "log_error": "ERROR",
        "log_instrument_error": "ERROR",
    }
    if normalized in fixed:
        return fixed[normalized]
    level_index = 0 if language == "rust" else 1
    if len(arguments) <= level_index:
        return ""
    value = arguments[level_index]
    for token, level in (
        ("Debug", "DEBUG"),
        ("Info", "INFO"),
        ("Warn", "WARN"),
        ("Error", "ERROR"),
    ):
        if re.search(rf"(?:Level|LogLevel)(?:::|\.){token}\b", value):
            return level
    return ""


def message_index(method: str, language: str) -> int:
    """返回各 logger API 的消息参数下标。"""

    if language == "go":
        if method in {"Log", "LogAttrs"}:
            return 2
        return 1 if method.endswith("Context") else 0
    if method in {"log", "log_event", "log_with_logger"}:
        return 1 if method == "log" else 2
    if method == "log_configured":
        return 1
    if method == "log_instrument_error":
        return 0
    return 0


def scan_method_calls(path: Path, root: Path, language: str) -> list[Call]:
    """扫描 selector 方法调用；该扫描器不把 macro 或函数声明误认为 logger。"""

    source = path.read_text(encoding="utf-8")
    if language == "go":
        methods = (
            "DebugContext|InfoContext|WarnContext|ErrorContext|"
            "LogAttrs|Debug|Info|Warn|Error|Log"
        )
    else:
        methods = (
            "debug|info|warn|error|log|log_info|log_warn|log_warning|"
            "log_error|log_instrument_error"
        )
    # Go 大量使用 `clientLogger(c).InfoContext`、`d.log().Warn` 这类返回 logger
    # 的 helper；Rust 也可能出现等价链。receiver 因此允许无嵌套参数的函数调用，
    # 但仍不跨行吞噬任意表达式，避免把前一条语句误当 selector receiver。
    pattern = re.compile(
        rf"(?P<receiver>[A-Za-z_][A-Za-z0-9_]*"
        rf"(?:\([^()\n]*\)|\.[A-Za-z_][A-Za-z0-9_]*(?:\([^()\n]*\))?)*)"
        rf"\s*\.(?P<method>{methods})\s*\("
    )
    result: list[Call] = []
    for match in pattern.finditer(source):
        # receiver 可能自身含 `()`；regex 的最后一个字符才是 logger 方法左括号。
        opening = match.end() - 1
        closing = matching_parenthesis(source, opening)
        if closing is None:
            continue
        arguments = split_arguments(source[opening + 1 : closing])
        index = message_index(match.group("method"), language)
        expression = arguments[index] if len(arguments) > index else ""
        result.append(
            Call(
                path=path.relative_to(root).as_posix(),
                line=source.count("\n", 0, match.start()) + 1,
                end_line=source.count("\n", 0, closing) + 1,
                receiver=match.group("receiver"),
                method=match.group("method"),
                arguments=arguments,
                message_expression=expression,
                messages=decoded_strings(expression, language),
                level=call_level(match.group("method"), arguments, language),
            )
        )
    return result


def scan_named_calls(path: Path, root: Path) -> list[Call]:
    """补充没有 selector 的少量 Rust 日志 wrapper 调用。"""

    source = path.read_text(encoding="utf-8")
    names = "log_event|log_with_logger|log_configured"
    pattern = re.compile(rf"(?<!fn )\b(?P<method>{names})\s*\(")
    result: list[Call] = []
    for match in pattern.finditer(source):
        opening = match.end() - 1
        closing = matching_parenthesis(source, opening)
        if closing is None:
            continue
        arguments = split_arguments(source[opening + 1 : closing])
        index = message_index(match.group("method"), "rust")
        expression = arguments[index] if len(arguments) > index else ""
        result.append(
            Call(
                path=path.relative_to(root).as_posix(),
                line=source.count("\n", 0, match.start()) + 1,
                end_line=source.count("\n", 0, closing) + 1,
                receiver="",
                method=match.group("method"),
                arguments=arguments,
                message_expression=expression,
                messages=decoded_strings(expression, "rust"),
                level=call_level(match.group("method"), arguments, "rust"),
            )
        )
    return result


def scan_tracing_macros(path: Path, root: Path) -> list[Call]:
    """扫描生产源码里的 ``tracing::*`` 宏，作为独立兼容性缺口。

    ``tracing`` 事件不会经过本项目的 Go ``slog`` 兼容 handler，也不会携带由
    ``Logger`` resolver 生成的 Go ``source``。因此这里故意只收集、不尝试把它们
    与 Go 调用自动配对；业务实现必须显式改回兼容 Logger 后门禁才能通过。
    """

    source = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r"\btracing\s*::\s*(?P<method>trace|debug|info|warn|error)\s*!\s*\("
    )
    result: list[Call] = []
    for match in pattern.finditer(source):
        opening = match.end() - 1
        closing = matching_parenthesis(source, opening)
        if closing is None:
            continue
        expression = source[opening + 1 : closing]
        arguments = split_arguments(expression)
        result.append(
            Call(
                path=path.relative_to(root).as_posix(),
                line=source.count("\n", 0, match.start()) + 1,
                end_line=source.count("\n", 0, closing) + 1,
                receiver="tracing",
                method=match.group("method"),
                arguments=arguments,
                # tracing 的字段语法不是普通函数参数；这里保留整个 token stream，
                # 只用于可读审计，不把其中字符串猜成 Logger 的 runtime matcher。
                message_expression=expression,
                messages=decoded_strings(expression, "rust"),
                level=call_level(match.group("method"), arguments, "rust"),
            )
        )
    return result


def masked_go(source: str) -> str:
    """屏蔽 Go 字符串和注释但保留换行及花括号位置。"""

    output = list(source)
    index = 0
    block_depth = 0
    quote = ""
    escaped = False
    while index < len(source):
        if block_depth:
            if source.startswith("*/", index):
                output[index : index + 2] = "  "
                block_depth = 0
                index += 2
            else:
                if source[index] not in "\r\n":
                    output[index] = " "
                index += 1
            continue
        if quote:
            character = source[index]
            if character not in "\r\n":
                output[index] = " "
            if quote != "`" and escaped:
                escaped = False
            elif quote != "`" and character == "\\":
                escaped = True
            elif character == quote:
                quote = ""
            index += 1
            continue
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = len(source) if end == -1 else end
            for cursor in range(index, end):
                output[cursor] = " "
            index = end
        elif source.startswith("/*", index):
            output[index : index + 2] = "  "
            block_depth = 1
            index += 2
        elif source[index] in {'"', "'", "`"}:
            quote = source[index]
            output[index] = " "
            index += 1
        else:
            index += 1
    return "".join(output)


def matching_brace(source: str, opening: int) -> int | None:
    """在已屏蔽文本中寻找匹配的右花括号。"""

    depth = 0
    for index in range(opening, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    return None


def go_declaration_body_open(source: str, start: int) -> int | None:
    """寻找 Go 函数签名之后的 body 左花括号。

    参数类型可以包含 `interface{}`，所以不能简单取声明后的第一个 `{`；只有圆括号
    与方括号深度均回到零后的花括号才是函数 body。
    """

    parentheses = 0
    brackets = 0
    for index in range(start, len(source)):
        character = source[index]
        if character == "(":
            parentheses += 1
        elif character == ")":
            parentheses -= 1
        elif character == "[":
            brackets += 1
        elif character == "]":
            brackets -= 1
        elif character == "{" and parentheses == 0 and brackets == 0:
            return index
        elif character == "\n" and parentheses == 0 and brackets == 0:
            # 多行签名只有在上一行仍有未闭合括号时才会跨行；到这里尚未见 body
            # 表示 regex 很可能命中了非声明文本。
            return None
    return None


def go_functions(path: Path, root: Path) -> list[GoFunction]:
    """恢复声明范围，并为内部 func literal 生成 Go runtime 常用名称。"""

    source = path.read_text(encoding="utf-8")
    masked = masked_go(source)
    package_path = f"{MODULE_PATH}/{path.relative_to(root).parent.as_posix()}"
    if path.relative_to(root).parent == Path("."):
        package_path = MODULE_PATH
    declarations: list[tuple[int, int, str]] = []
    declaration_pattern = re.compile(
        r"(?m)^func\s+(?:\((?P<receiver>[^)]*)\)\s+)?"
        r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
    )
    for match in declaration_pattern.finditer(masked):
        opening = go_declaration_body_open(masked, match.end())
        if opening is None:
            continue
        closing = matching_brace(masked, opening)
        if closing is None:
            continue
        receiver = match.group("receiver")
        if receiver:
            raw_type = receiver.strip().split()[-1]
            pointer = raw_type.startswith("*")
            receiver_type = raw_type.lstrip("*").split("[", 1)[0]
            owner = f"(*{receiver_type})" if pointer else receiver_type
            name = f"{package_path}.{owner}.{match.group('name')}"
        else:
            name = f"{package_path}.{match.group('name')}"
        declarations.append((match.start(), closing, name))

    functions: list[GoFunction] = []
    literal_pattern = re.compile(r"\bfunc\s*\(")
    for start, end, name in declarations:
        functions.append(
            GoFunction(
                start=source.count("\n", 0, start) + 1,
                end=source.count("\n", 0, end) + 1,
                function=name,
                is_literal=False,
            )
        )
        literal_ranges: list[tuple[int, int, int]] = []
        literal_index = 0
        for literal in literal_pattern.finditer(masked, start, end):
            # receiver 声明 `func (x *Type) Method` 也以 `func (` 开头，但它不是
            # literal；若把它计入会让每个方法内 closure 平白多出一个序号。
            if literal.start() == start:
                continue
            literal_index += 1
            opening = masked.find("{", literal.end(), end)
            if opening == -1:
                continue
            closing = matching_brace(masked, opening)
            if closing is None or closing > end:
                continue
            literal_ranges.append((literal.start(), closing, literal_index))
        for literal_start, literal_end, literal_index in literal_ranges:
            # Go 编译器的精确 nested literal 编号由固定 1.25.7 probe 最终确认；
            # 静态阶段仍记录候选，避免把 closure 误报成外层声明。
            functions.append(
                GoFunction(
                    start=source.count("\n", 0, literal_start) + 1,
                    end=source.count("\n", 0, literal_end) + 1,
                    function=f"{name}.func{literal_index}",
                    is_literal=True,
                )
            )
    return functions


def enclosing_go_function(functions: Iterable[GoFunction], line: int) -> GoFunction | None:
    """选择包含调用行的最窄函数范围。"""

    candidates = [function for function in functions if function.start <= line <= function.end]
    return min(candidates, key=lambda function: function.end - function.start, default=None)


def collect_go_sites(go_root: Path) -> list[GoSite]:
    """收集非测试 Go logger 调用及预测 source；closure 等待 oracle 确认。"""

    sites: list[GoSite] = []
    for path in sorted(go_root.rglob("*.go")):
        relative = path.relative_to(go_root)
        if relative.name.endswith("_test.go") or relative.parts[:2] == ("gen", "proto"):
            continue
        functions = go_functions(path, go_root)
        for call in scan_method_calls(path, go_root, "go"):
            # `.Error()`/`.Info()` 是 Go 生态常见的非日志方法（error、DirEntry、
            # http 等）。Context/LogAttrs 是 slog 专用；短方法则必须由 logger 命名
            # 或项目固定的 `.log()` accessor 证明，避免把业务方法伪装成 Go 日志缺口。
            receiver = call.receiver.lower()
            if not (
                call.method.endswith("Context")
                or call.method == "LogAttrs"
                or "logger" in receiver
                or receiver == "slog"
                or receiver.endswith(".log()")
            ):
                continue
            function = enclosing_go_function(functions, call.line)
            if function is None:
                continue
            sites.append(
                GoSite(
                    path=call.path,
                    line=call.line,
                    end_line=call.end_line,
                    level=call.level,
                    messages=call.messages,
                    function=function.function,
                    file=f"{MODULE_PATH}/{call.path}",
                    in_literal=function.is_literal,
                    source_line=call.line,
                    oracle_confirmed=False,
                )
            )
    return sites


def load_go_source_oracle(path: Path) -> dict[tuple[str, int], tuple[str, str, int]]:
    """读取固定 Go 1.25.7 构建生成的 runtime source 目录。"""

    if not path.is_file():
        return {}
    expected_header = (
        "origin_path\torigin_line\tgo_function\tgo_file\tgo_line\tevidence"
    )
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != expected_header:
        raise ValueError(f"invalid Go log source oracle header: {path}")
    result: dict[tuple[str, int], tuple[str, str, int]] = {}
    for number, value in enumerate(lines[1:], start=2):
        columns = value.split("\t")
        if len(columns) != 6 or columns[5] != "go1.25.7-trimpath-linux-arm64":
            raise ValueError(f"invalid Go log source oracle row {path}:{number}")
        try:
            origin_line = int(columns[1])
            source_line = int(columns[4])
        except ValueError as error:
            raise ValueError(
                f"invalid Go log source oracle line number {path}:{number}"
            ) from error
        key = (columns[0], origin_line)
        destination = (columns[2], columns[3], source_line)
        previous = result.setdefault(key, destination)
        if previous != destination:
            raise ValueError(f"ambiguous Go log source oracle row {path}:{number}")
    return result


def apply_go_source_oracle(
    sites: Iterable[GoSite],
    oracle: dict[tuple[str, int], tuple[str, str, int]],
) -> list[GoSite]:
    """用实测 runtime source 替换静态预测，同时保留原调用行作为关联键。"""

    result: list[GoSite] = []
    for site in sites:
        destination = oracle.get((site.path, site.line))
        if destination is None:
            result.append(site)
            continue
        function, file, source_line = destination
        result.append(
            replace(
                site,
                function=function,
                file=file,
                source_line=source_line,
                oracle_confirmed=True,
            )
        )
    return result


def is_direct_logger(call: Call) -> bool:
    """保守识别实际调用 ``Logger`` facade 的 selector。"""

    tail = call.receiver.rsplit(".", 1)[-1]
    if (
        call.path in TRACK_CALLER_LOG_METHOD_PATHS
        and call.receiver == "self"
        and call.method == "log"
    ):
        return True
    # tracked wrapper 内部的 facade 调用没有自己的 runtime location；它继承外层
    # `self.log` 调用点。无静态消息正是这些转发函数的受审计形状。
    if (
        call.path in TRACK_CALLER_LOG_METHOD_PATHS
        and call.method == "log"
        and not call.messages
    ):
        return False
    # `new_git_handler` 直接调用 track_caller 自由函数，运行时位置同样是外层这行；
    # wrapper 自身使用动态 message，已被上面的无静态消息分支排除。
    if call.method == "log_with_logger" and call.messages:
        return True
    return (
        tail == "logger"
        or tail.endswith("_logger")
        or call.receiver.endswith("default_slog_logger()")
    )


def mirrored_go_path(rust_path: str) -> str:
    """把 ``src/`` 下 Rust 镜像路径转换为 Go 相对路径。"""

    path = rust_path.removeprefix("src/")
    return path[:-3] + ".go" if path.endswith(".rs") else path


def logical_rust_calls(rust_root: Path) -> list[Call]:
    """收集生产事件表达式，用于发现 Rust 新增消息和 wrapper 漂移。"""

    result: list[Call] = []
    for path in sorted((rust_root / "src").rglob("*.rs")):
        relative = path.relative_to(rust_root).as_posix()
        if path.name.endswith("_test.rs") or relative.startswith("src/gen/"):
            continue
        for call in scan_method_calls(path, rust_root, "rust"):
            # `self.log()`/`self.log().is_some()` 是 logger accessor，不是事件。
            if call.method == "log" and len(call.arguments) < 2:
                continue
            result.append(call)
        result.extend(scan_named_calls(path, rust_root))
    return result


def direct_runtime_calls(rust_root: Path) -> list[Call]:
    """收集会成为 ``Location::caller`` 的生产 Logger facade 调用。"""

    return [
        call
        for call in logical_rust_calls(rust_root)
        if is_direct_logger(call)
        and call.method
        in {"debug", "info", "warn", "error", "log", "log_with_logger"}
        and call.path
        not in {
            "src/internal/logger/zz_recovered_types.rs",
            # `emit` 使用 track_caller，runtime location 是 Write 内两个 self.emit
            # 调用；这两个位置由 log_writer_rows 单独建规则。
            "src/internal/logger/log_writer.rs",
        }
    ]


def tracing_macro_calls(rust_root: Path) -> list[Call]:
    """收集手写生产 Rust 源码中的 ``tracing::*`` 事件。

    相邻 ``*_test.rs`` 与生成目录不属于 release 生产路径，直接排除。扫描结果不会
    进入 runtime source map：每一个位置都单独成为 blocker，直到业务实现改用能够
    产生 Go 兼容 stderr JSON 和 ``source`` 的 Logger facade。
    """

    result: list[Call] = []
    for path in sorted((rust_root / "src").rglob("*.rs")):
        relative = path.relative_to(rust_root).as_posix()
        if path.name.endswith("_test.rs") or relative.startswith("src/gen/"):
            continue
        result.extend(scan_tracing_macros(path, rust_root))
    return result


def choose_go_site(
    call: Call,
    message: str,
    go_sites: list[GoSite],
    rust_occurrence: int,
    rust_occurrence_count: int,
) -> tuple[GoSite | None, str]:
    """按镜像文件、消息、级别和稳定出现顺序选择 Go 调用。"""

    go_path = mirrored_go_path(call.path)
    candidates = [
        site
        for site in go_sites
        if site.path == go_path
        and any(messages_equivalent(message, candidate) for candidate in site.messages)
        and (not call.level or not site.level or call.level == site.level)
    ]
    if len(candidates) == 1:
        return candidates[0], "unique path/message/level"
    if len(candidates) == rust_occurrence_count and rust_occurrence < len(candidates):
        return sorted(candidates, key=lambda site: site.line)[rust_occurrence], (
            "paired by path/message/level occurrence"
        )
    if not candidates:
        return None, "Go mirror contains no same message/level call"
    return None, f"ambiguous Go calls at lines {','.join(str(site.line) for site in candidates)}"


def row(
    *,
    kind: str,
    runtime_path: str = "",
    runtime_line: int | str = "",
    level: str = "",
    match_kind: str = "",
    message: str = "",
    attribute_keys: str = "",
    go_function: str = "",
    go_file: str = "",
    go_line: int | str = "",
    status: str,
    origin_path: str,
    origin_line: int,
    detail: str,
) -> tuple[str, ...]:
    """构造一个 TSV row，并对自由文本统一 JSON 编码。"""

    return (
        kind,
        runtime_path,
        str(runtime_line),
        level,
        match_kind,
        json_field(message),
        attribute_keys,
        go_function,
        go_file,
        str(go_line),
        status,
        origin_path,
        str(origin_line),
        json_field(detail),
    )


def go_site_at(go_sites: Iterable[GoSite], path: str, line: int) -> GoSite | None:
    """按镜像文件和固定基线行号查找唯一 Go logger 调用。"""

    matches = [site for site in go_sites if site.path == path and site.line == line]
    return matches[0] if len(matches) == 1 else None


def rule_row_for_site(
    call: Call,
    site: GoSite,
    message: str,
    *,
    attribute_predicate: str = "",
    match_kind: str | None = None,
    detail: str,
) -> tuple[str, ...]:
    """把已经消歧的 Rust/Go 调用对转换为一条 runtime 规则。"""

    return row(
        kind="rule",
        runtime_path=call.path,
        runtime_line=call.line,
        level=call.level,
        match_kind=match_kind
        or ("any" if rust_message_is_formatted(call) else "exact"),
        message=message,
        attribute_keys=attribute_predicate
        or encoded_attribute_keys(rust_attribute_keys(call)),
        go_function=site.function,
        go_file=site.file,
        go_line=site.source_line,
        status="mapped-oracle"
        if site.oracle_confirmed
        else (
            "function-oracle-pending" if site.in_literal else "source-oracle-pending"
        ),
        origin_path=call.path,
        origin_line=call.line,
        detail=detail,
    )


def special_direct_rows(call: Call, go_sites: list[GoSite]) -> list[tuple[str, ...]] | None:
    """处理结构合并后仅靠消息无法区分、但 runtime 上仍可确定的调用。"""

    # heartbeat permanent-error 分支沿用 Go 的动态 `message` 变量；该 Rust 调用点经
    # track_caller 后独占同一个控制流位置，所以可用 path/line/level 的 any 规则
    # 精确恢复 source，无需给外部日志增加人为判别属性。
    if (
        call.path == "src/internal/podmonitor/lease_manager.rs"
        and call.receiver == "self"
        and call.method == "log"
        and not call.messages
        and call.message_expression.strip() == "message"
    ):
        site = go_site_at(go_sites, mirrored_go_path(call.path), 341)
        if site is not None:
            return [
                rule_row_for_site(
                    call,
                    site,
                    "",
                    match_kind="any",
                    detail="unique permanent-heartbeat branch preserves Go dynamic message source",
                )
            ]

    if len(call.messages) != 1:
        return None
    message = call.messages[0]
    go_path = mirrored_go_path(call.path)

    # Rust guard 只在 Execute 的 relay token defer 语义中创建；Destroy 走另一条
    # remove helper，因此这里明确对应 Go Execute 内的 closure，而非同文案的 Destroy。
    if (
        call.path == "src/internal/claude/claude_code_executor.rs"
        and message == "Failed to remove CCR upstream proxy token file"
    ):
        site = go_site_at(go_sites, go_path, 414)
        if site is not None:
            return [
                rule_row_for_site(
                    call,
                    site,
                    message,
                    detail="SensitiveFileGuard only represents Execute relay-token defer",
                )
            ]

    # Go 的两个 select 分支行号不同。Rust 的 signal 属性保留相同 wire 值，所以可在
    # 不增加外部字段的前提下，用 source-only 属性值谓词精确选择 361 或 363。
    if (
        call.path == "src/internal/manager/manager.rs"
        and message == "Received signal, shutting down gracefully"
    ):
        signal_site = go_site_at(go_sites, go_path, 361)
        cancellation_site = go_site_at(go_sites, go_path, 363)
        if signal_site is None or cancellation_site is None:
            return None
        expression = " ".join(call.arguments)
        if '"context canceled"' in expression:
            return [
                rule_row_for_site(
                    call,
                    cancellation_site,
                    message,
                    attribute_predicate=json.dumps(
                        {"signal": "context canceled"}, separators=(",", ":")
                    ),
                    detail="constant cancellation branch matches Go ctx.Done source",
                )
            ]
        return [
            rule_row_for_site(
                call,
                signal_site,
                message,
                attribute_predicate=json.dumps(
                    {"signal": signal}, separators=(",", ":")
                ),
                detail="runtime Unix signal value selects Go signal-channel source",
            )
            for signal in ("interrupt", "terminated")
        ] + [
            rule_row_for_site(
                call,
                cancellation_site,
                message,
                attribute_predicate=json.dumps(
                    {"signal": "context canceled"}, separators=(",", ":")
                ),
                detail="runtime cancellation value selects Go ctx.Done source",
            )
        ]

    # Rust 把 Execute/ExecuteWithStdin 的尾部记录合并到 finish_execution。后者的
    # logger 固定绑定 sandbox_enabled，前者没有该键；resolver 会优先选择更具体规则。
    hook_lines = {
        "hook execution failed": (52, 128),
        "hook execution completed": (60, 136),
    }
    if call.path == "src/internal/orchestrator/hooks.rs" and message in hook_lines:
        plain_site = go_site_at(go_sites, go_path, hook_lines[message][0])
        stdin_site = go_site_at(go_sites, go_path, hook_lines[message][1])
        if plain_site is None or stdin_site is None:
            return None
        local_keys = rust_attribute_keys(call)
        return [
            rule_row_for_site(
                call,
                plain_site,
                message,
                attribute_predicate=encoded_attribute_keys(local_keys),
                detail="finish_execution without sandbox_enabled maps to Hook.Execute",
            ),
            rule_row_for_site(
                call,
                stdin_site,
                message,
                attribute_predicate=encoded_attribute_keys(
                    (*local_keys, "sandbox_enabled")
                ),
                detail="bound sandbox_enabled identifies Hook.ExecuteWithStdin",
            ),
        ]

    return None


def log_writer_rows(
    go_root: Path,
    rust_root: Path,
    oracle: dict[tuple[str, int], tuple[str, str, int]],
) -> list[tuple[str, ...]]:
    """生成 ``LogWriter.Write`` 两个 caller 行的专用规则。"""

    rust_path = rust_root / "src/internal/logger/log_writer.rs"
    rust_source = rust_path.read_text(encoding="utf-8")
    rust_lines = [
        index
        for index, line in enumerate(rust_source.splitlines(), start=1)
        if "self.emit(" in line
    ]
    go_path = go_root / "internal/logger/log_writer.go"
    go_lines = [
        index
        for index, line in enumerate(go_path.read_text(encoding="utf-8").splitlines(), start=1)
        if ".LogAttrs(" in line
    ]
    if len(rust_lines) != 2 or len(go_lines) != 2:
        return [
            row(
                kind="audit",
                status="log-writer-shape-mismatch",
                origin_path="src/internal/logger/log_writer.rs",
                origin_line=rust_lines[0] if rust_lines else 0,
                detail=f"Rust emit lines={rust_lines}; Go LogAttrs lines={go_lines}",
            )
        ]
    function = f"{MODULE_PATH}/internal/logger.(*LogWriter).Write"
    file = f"{MODULE_PATH}/internal/logger/log_writer.go"
    complete_destination = oracle.get(("internal/logger/log_writer.go", go_lines[0]))
    truncated_destination = oracle.get(("internal/logger/log_writer.go", go_lines[1]))
    complete_function, complete_file, complete_line = complete_destination or (
        function,
        file,
        go_lines[0],
    )
    truncated_function, truncated_file, truncated_line = truncated_destination or (
        function,
        file,
        go_lines[1],
    )
    return [
        row(
            kind="rule",
            runtime_path="src/internal/logger/log_writer.rs",
            runtime_line=rust_lines[0],
            match_kind="any",
            go_function=complete_function,
            go_file=complete_file,
            go_line=complete_line,
            status="mapped-oracle" if complete_destination else "source-oracle-pending",
            origin_path="src/internal/logger/log_writer.rs",
            origin_line=rust_lines[0],
            detail="完整行；固定 Go 1.25.7 probe 仍需确认 trimpath wire",
        ),
        row(
            kind="rule",
            runtime_path="src/internal/logger/log_writer.rs",
            runtime_line=rust_lines[1],
            match_kind="suffix",
            message=" [line truncated - no newline]",
            go_function=truncated_function,
            go_file=truncated_file,
            go_line=truncated_line,
            status="mapped-oracle" if truncated_destination else "source-oracle-pending",
            origin_path="src/internal/logger/log_writer.rs",
            origin_line=rust_lines[1],
            detail="超长无换行记录；suffix 防止未来 helper 合并 caller 时误选",
        ),
    ]


def collect_rows(
    go_root: Path,
    rust_root: Path,
    oracle: dict[tuple[str, int], tuple[str, str, int]],
) -> list[tuple[str, ...]]:
    """生成运行时规则以及所有未映射生产事件的 blocker 行。"""

    go_sites = apply_go_source_oracle(collect_go_sites(go_root), oracle)
    logical_calls = logical_rust_calls(rust_root)
    runtime_calls = direct_runtime_calls(rust_root)
    rows = log_writer_rows(go_root, rust_root, oracle)

    # tracing 宏不是 Logger facade 的另一种写法：当前 release 没有把它接入 Go
    # stderr JSON handler，而且它的 callsite 也不会经过 Go source resolver。逐点列为
    # blocker，避免“消息文本碰巧相同”掩盖生产日志实际丢失。
    for call in tracing_macro_calls(rust_root):
        expression = " ".join(call.message_expression.split())
        rows.append(
            row(
                kind="audit",
                status="tracing-event-not-source-compatible",
                origin_path=call.path,
                origin_line=call.line,
                detail=f"tracing::{call.method}!({expression[:200]})",
            )
        )

    logical_occurrences: dict[tuple[str, str, str], list[Call]] = defaultdict(list)
    for call in logical_calls:
        for message in call.messages:
            logical_occurrences[(call.path, call.level, message)].append(call)

    for call in runtime_calls:
        special = special_direct_rows(call, go_sites)
        if special is not None:
            rows.extend(special)
            continue
        # 动态 wrapper 的具体消息来自外层业务调用；后续规则按同文件 Go 消息展开。
        if not call.messages:
            go_path = mirrored_go_path(call.path)
            candidates = [
                site
                for site in go_sites
                if site.path == go_path and (not call.level or site.level == call.level)
            ]
            grouped: dict[tuple[str, str], list[GoSite]] = defaultdict(list)
            for site in candidates:
                for message in site.messages:
                    grouped[(site.level, message)].append(site)
            emitted = 0
            for (level, message), sites in sorted(grouped.items()):
                if len(sites) != 1:
                    continue
                site = sites[0]
                rows.append(
                    row(
                        kind="rule",
                        runtime_path=call.path,
                        runtime_line=call.line,
                        level=level,
                        match_kind="exact",
                        message=message,
                        attribute_keys=encoded_attribute_keys(rust_attribute_keys(call)),
                        go_function=site.function,
                        go_file=site.file,
                        go_line=site.source_line,
                        status="mapped-oracle"
                        if site.oracle_confirmed
                        else (
                            "function-oracle-pending"
                            if site.in_literal
                            else "source-oracle-pending"
                        ),
                        origin_path=call.path,
                        origin_line=call.line,
                        detail="dynamic wrapper expanded from unique Go path/message/level",
                    )
                )
                emitted += 1
            if emitted == 0:
                rows.append(
                    row(
                        kind="audit",
                        status="dynamic-runtime-unmapped",
                        origin_path=call.path,
                        origin_line=call.line,
                        detail=f"dynamic expression: {call.message_expression[:160]}",
                    )
                )
            continue

        for message in call.messages:
            occurrences = logical_occurrences[(call.path, call.level, message)]
            occurrence = occurrences.index(call)
            site, detail = choose_go_site(
                call, message, go_sites, occurrence, len(occurrences)
            )
            if site is None:
                rows.append(
                    row(
                        kind="audit",
                        status="rust-event-unmapped",
                        origin_path=call.path,
                        origin_line=call.line,
                        detail=f"{message!r}: {detail}",
                    )
                )
                continue
            rows.append(
                rule_row_for_site(call, site, message, detail=detail)
            )

    # 直接比较所有静态业务消息，确保 Rust-only 日志即使经过 wrapper 也成为 blocker。
    go_messages = {
        (site.path, site.level, message)
        for site in go_sites
        for message in site.messages
    }
    seen_missing: set[tuple[str, int, str]] = set()
    for call in logical_calls:
        go_path = mirrored_go_path(call.path)
        for message in call.messages:
            identity = (call.path, call.line, message)
            if identity in seen_missing:
                continue
            if (go_path, call.level, message) in go_messages or any(
                path == go_path and messages_equivalent(message, value)
                for path, _level, value in go_messages
            ):
                continue
            seen_missing.add(identity)
            rows.append(
                row(
                    kind="audit",
                    status="rust-only-message",
                    origin_path=call.path,
                    origin_line=call.line,
                    detail=f"Go mirror has no logger message {message!r}",
                )
            )

    # 反向证明完整性：Rust→Go 的唯一映射并不能证明 Go 基线事件没有被整体遗漏。
    # 每个 Go 调用点必须由且仅由一个 Rust runtime callsite 消费。属性/消息谓词可以
    # 让同一 callsite 产生多条 resolver row，但它们仍算同一个生产事件实现。
    sites_by_destination: dict[tuple[str, str, int], list[GoSite]] = defaultdict(list)
    for site in go_sites:
        sites_by_destination[(site.function, site.file, site.source_line)].append(site)

    consumers: dict[tuple[str, int], set[tuple[str, int]]] = defaultdict(set)
    ambiguous_destinations: set[tuple[str, str, int]] = set()
    orphan_rules: set[tuple[str, int, tuple[str, str, int]]] = set()
    for value in rows:
        if value[0] != "rule":
            continue
        try:
            destination = (value[7], value[8], int(value[9]))
            runtime = (value[1], int(value[2]))
        except ValueError:
            orphan_rules.add((value[1], int(value[2] or 0), (value[7], value[8], 0)))
            continue
        matches = sites_by_destination.get(destination, [])
        if len(matches) == 1:
            site = matches[0]
            consumers[(site.path, site.line)].add(runtime)
        elif len(matches) > 1:
            ambiguous_destinations.add(destination)
        else:
            orphan_rules.add((runtime[0], runtime[1], destination))

    for destination in sorted(ambiguous_destinations):
        matching_sites = sites_by_destination[destination]
        first = min(matching_sites, key=lambda site: (site.path, site.line))
        rows.append(
            row(
                kind="audit",
                status="go-source-identity-collision",
                origin_path=first.path,
                origin_line=first.line,
                detail=(
                    f"runtime source {destination!r} belongs to Go sites "
                    f"{[(site.path, site.line) for site in matching_sites]!r}"
                ),
            )
        )
    for runtime_path, runtime_line, destination in sorted(orphan_rules):
        rows.append(
            row(
                kind="audit",
                status="runtime-rule-without-go-site",
                origin_path=runtime_path,
                origin_line=runtime_line,
                detail=f"resolver destination is absent from Go catalog: {destination!r}",
            )
        )

    for site in go_sites:
        site_consumers = consumers[(site.path, site.line)]
        detail_message = site.messages[0] if site.messages else "<dynamic message>"
        if not site_consumers:
            rows.append(
                row(
                    kind="audit",
                    status="go-event-unmapped",
                    origin_path=site.path,
                    origin_line=site.line,
                    detail=(
                        f"{site.level or '<dynamic level>'} {detail_message!r}; "
                        "no Rust Logger runtime rule consumes this Go slog site"
                    ),
                )
            )
        elif len(site_consumers) > 1:
            rows.append(
                row(
                    kind="audit",
                    status="go-event-multiple-consumers",
                    origin_path=site.path,
                    origin_line=site.line,
                    detail=(
                        f"Go slog site is consumed by multiple Rust callsites: "
                        f"{sorted(site_consumers)!r}"
                    ),
                )
            )

    # 同一个运行时 selector 必须只能选择一个 Go source；否则 message/level 仍不足以
    # 区分 wrapper 合并的业务分支，必须补属性规则或恢复 caller 传播。
    rule_groups: dict[
        tuple[str, str, str, str, str, str], set[tuple[str, str, str]]
    ] = defaultdict(set)
    for value in rows:
        if value[0] != "rule" or value[10] not in RUNTIME_STATUSES:
            continue
        key = (value[1], value[2], value[3], value[4], value[5], value[6])
        rule_groups[key].add((value[7], value[8], value[9]))
    for key, destinations in sorted(rule_groups.items()):
        if len(destinations) <= 1:
            continue
        rows.append(
            row(
                kind="audit",
                status="runtime-key-collision",
                origin_path=key[0],
                origin_line=int(key[1]),
                detail=f"key {key[2:]} selects {sorted(destinations)}",
            )
        )

    return sorted(
        set(rows),
        key=lambda value: (
            value[11],
            int(value[12] or 0),
            value[0],
            value[3],
            value[5],
            value[10],
            # 上述字段相同的规则仍可能由不同属性 selector 区分。若不追加完整行，
            # `set` 的哈希随机种子会让共享挂载和权威 `/home` 工作区生成不同顺序，
            # 从而把完全相同的源码误报为 stale。完整 tuple 只作为稳定 tie-breaker，
            # 不改变任何映射或完备性判定。
            value,
        ),
    )


def render(rows: list[tuple[str, ...]]) -> str:
    """序列化为无歧义 TSV；字段中的自由文本已经是 JSON string。"""

    lines = ["\t".join(HEADER)]
    lines.extend("\t".join(value) for value in rows)
    return "\n".join(lines) + "\n"


def print_summary(rows: list[tuple[str, ...]]) -> None:
    """打印状态计数和 blocker 的源码位置。"""

    counts = Counter(value[10] for value in rows)
    print(f"log source map rows: {len(rows)}", file=sys.stderr)
    for status, count in sorted(counts.items()):
        print(f"  {status}: {count}", file=sys.stderr)
    blockers = [value for value in rows if value[10] not in COMPLETE_STATUSES]
    for value in blockers:
        print(
            f"  blocker {value[11]}:{value[12]} [{value[10]}] "
            f"{json.loads(value[13])}",
            file=sys.stderr,
        )


def main() -> int:
    """生成或检查映射，并可拒绝任一未确认生产事件。"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--go-root",
        type=Path,
        default=Path(
            os.environ.get(
                "GO_SOURCE_ROOT", "/Users/arthur/GolandProjects/environment-manager"
            )
        ),
    )
    parser.add_argument(
        "--rust-root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument(
        "--oracle-catalog",
        type=Path,
        help="固定 Go 1.25.7 -trimpath runtime source 目录；默认读取 compat/go-log-source-oracle.tsv",
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--summary", action="store_true")
    arguments = parser.parse_args()

    go_root = arguments.go_root.resolve()
    rust_root = arguments.rust_root.resolve()
    if not (go_root / "go.mod").is_file():
        parser.error(f"Go baseline is unavailable: {go_root}")
    if not (rust_root / "src").is_dir():
        parser.error(f"Rust source root is unavailable: {rust_root}")

    oracle_catalog = arguments.oracle_catalog or (
        rust_root / "compat/go-log-source-oracle.tsv"
    )
    try:
        oracle = load_go_source_oracle(oracle_catalog)
    except ValueError as error:
        parser.error(str(error))
    rows = collect_rows(go_root, rust_root, oracle)
    content = render(rows)
    target = rust_root / "compat/go-rust-log-sources.tsv"
    if arguments.summary:
        print_summary(rows)
    if arguments.check:
        if not target.exists() or target.read_text(encoding="utf-8") != content:
            print(f"stale log source map: regenerate {target}", file=sys.stderr)
            return 1
        if arguments.require_complete:
            blockers = [value for value in rows if value[10] not in COMPLETE_STATUSES]
            if blockers:
                print_summary(rows)
                return 2
        return 0

    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=target.parent, delete=False
    ) as output:
        output.write(content)
        temporary = Path(output.name)
    temporary.replace(target)
    if arguments.require_complete:
        blockers = [value for value in rows if value[10] not in COMPLETE_STATUSES]
        if blockers:
            print_summary(rows)
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
