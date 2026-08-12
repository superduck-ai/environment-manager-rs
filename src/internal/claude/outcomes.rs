//! 对应 Go：`internal/claude/outcomes.go`。
//!
//! 本模块保存仓库到允许分支的 outcome 映射，并在 Claude 执行前把多分支配置收敛
//! 为 Go 当前支持的单分支形状。`Option<HashMap<...>>` 专门保留 Go 零值结构体中
//! `nil map` 与 `NewOutcomes` 创建的非 nil 空 map 之间的差异；写入和导出则依靠
//! Rust 所有权或显式克隆，维持 Go `append([]string(nil), branches...)` 的隔离语义。
//!
//! Rust 不允许安全代码调用 nil `*Outcomes`；`validate()` 使用进程级
//! tracing logger，显式注入的路径则便于测试捕获。

use std::{collections::HashMap, fmt};

use crate::internal::logger::{Logger, default_logger};

use super::types::claude_attrs;

/// 一个 session 允许写入的仓库及其目标分支。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Outcomes {
    repositories: Option<HashMap<String, Vec<String>>>,
}

impl Outcomes {
    /// 对应 Go：`NewOutcomes`。
    ///
    /// 创建一个已经初始化、但尚未包含任何仓库的 outcome 集合。这里必须保存为
    /// `Some(empty_map)`，不能等同于 `Outcomes::default()` 的 Go 零值 map。
    ///
    /// # 返回
    /// 返回独立的空集合；后续可通过 [`Self::add`] 添加仓库。
    #[must_use]
    pub fn new() -> Self {
        Self {
            repositories: Some(HashMap::new()),
        }
    }

    /// 对应 Go：`(*Outcomes).Add`。
    ///
    /// 添加或覆盖一个仓库的完整分支列表。`repository` 会转成 owned 字符串，
    /// `branches` 也会被移动进集合，因此调用结束后不存在可从外部修改内部列表的
    /// 共享别名；这与 Go 主动复制输入 slice 的目的相同。
    ///
    /// # 参数
    /// - `repository`：仓库的稳定标识，通常为 `owner/name`；空字符串不会在此层拒绝。
    /// - `branches`：仓库允许的分支列表；空 `Vec` 对应 Go 保存的 nil/空 slice。
    ///
    /// # 副作用
    /// 必要时把零值集合初始化为 map，并覆盖同名仓库的旧值。
    pub fn add(&mut self, repository: impl Into<String>, branches: Vec<String>) {
        self.repositories
            .get_or_insert_with(HashMap::new)
            .insert(repository.into(), branches);
    }

    /// 对应 Go：`(*Outcomes).Get`。
    ///
    /// 查询仓库的分支列表。Rust 使用 `Option<&[String]>` 表达 Go 的 nil slice：
    /// 零值 map、未知仓库以及由 `Add` 保存的空列表都返回 `None`，非空列表则返回
    /// 内部只读 slice，且不会复制数据。
    ///
    /// # 参数
    /// - `repository`：待查询的完整仓库标识。
    ///
    /// # 返回
    /// 找到非空分支列表时返回其只读视图，否则返回 `None`。
    #[must_use]
    pub fn get(&self, repository: &str) -> Option<&[String]> {
        self.repositories
            .as_ref()?
            .get(repository)
            .filter(|branches| !branches.is_empty())
            .map(Vec::as_slice)
    }

    /// 对应 Go：`(*Outcomes).GetFirst`。
    ///
    /// 返回仓库的第一个分支，用于当前只支持单目标分支的执行路径。
    ///
    /// # 参数
    /// - `repository`：待查询的完整仓库标识。
    ///
    /// # 返回
    /// 有分支时返回第一项；仓库缺失或列表为空时返回静态空字符串，保持 Go 返回
    /// `""` 的约定。
    #[must_use]
    pub fn get_first(&self, repository: &str) -> &str {
        self.get(repository)
            .and_then(|branches| branches.first())
            .map_or("", String::as_str)
    }

    /// 对应 Go：`(*Outcomes).Repositories`。
    ///
    /// 收集 map 中的全部仓库名称。结果刻意不排序：Go 遍历 map 时也不承诺顺序，
    /// 调用方不得把当前迭代次序视为外部协议的一部分。
    ///
    /// # 返回
    /// Go 零值 map 对应 `None`；已初始化的 map 返回 `Some(Vec)`，即使其中没有元素。
    #[must_use]
    pub fn repositories(&self) -> Option<Vec<String>> {
        self.repositories
            .as_ref()
            .map(|repositories| repositories.keys().cloned().collect())
    }

    /// 对应 Go：`(*Outcomes).AsMap`。
    ///
    /// 深拷贝完整仓库映射和每个分支列表，使调用方可以安全修改返回结果而不影响
    /// 当前集合。
    ///
    /// # 返回
    /// Go 零值 map 对应 `None`；否则返回一份完全独立的 owned map。
    #[must_use]
    pub fn as_map(&self) -> Option<HashMap<String, Vec<String>>> {
        self.repositories.clone()
    }

    /// 对应 Go：`(*Outcomes).Len`。
    ///
    /// # 返回
    /// 返回仓库条目数；Go 零值 map 和已初始化的空 map 均返回零。
    #[must_use]
    pub fn len(&self) -> usize {
        self.repositories.as_ref().map_or(0, HashMap::len)
    }

    /// 对应 Go：`(*Outcomes).IsEmpty`。
    ///
    /// # 返回
    /// 当 [`Self::len`] 为零时返回 `true`，否则返回 `false`。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 对应 Go：`(*Outcomes).Validate`。
    ///
    /// 使用进程级 tracing logger 验证当前集合。多分支仓库只保留第一
    /// 项，空分支列表原样保留；原集合不会被修改。
    ///
    /// # 返回
    /// 返回已经初始化的新集合，内容为验证和截断后的深拷贝。
    ///
    /// # 副作用
    /// 遇到空分支或多分支仓库时，分别尝试写出 Go 对应的 DEBUG 或 WARN 日志。
    /// 默认 logger 的 INFO 门槛会像 Go 一样过滤 DEBUG 记录。
    #[must_use]
    pub fn validate(&self) -> Self {
        self.validate_with_logger(default_logger())
    }

    /// 对应 Go：`(*Outcomes).ValidateWithLogger`。
    ///
    /// 使用调用方提供的 logger 验证 outcome。空分支列表仍保留对应仓库；多分支
    /// 列表记录完整兼容告警后仅保留第一项；单分支列表直接复制。
    ///
    /// # 参数
    /// - `logger`：接收验证日志的 concrete logger；Rust 引用不允许 Go 的 nil logger。
    ///
    /// # 返回
    /// 返回已经初始化的新集合；任何分支 `Vec` 都不会与原集合共享可变存储。
    ///
    /// # 副作用
    /// 空分支仓库写 DEBUG，属性依次为 `repository`；多分支仓库写 WARN，属性严格
    /// 依次为 `repository`、`branch_used`、`branches_ignored`。
    ///
    /// # 兼容性
    /// 遍历顺序沿用 hash map 的非确定顺序，不额外排序；这与 Go map 遍历一致。
    #[must_use]
    pub fn validate_with_logger(&self, logger: &Logger) -> Self {
        let mut validated = Self::new();
        let Some(repositories) = self.repositories.as_ref() else {
            return validated;
        };

        for (repository, branches) in repositories {
            if branches.is_empty() {
                logger.debug(
                    "Repository has no branches specified, preserving empty list",
                    claude_attrs!("repository" => repository),
                );
                validated.add(repository.clone(), Vec::new());
                continue;
            }

            if branches.len() > 1 {
                logger.warn(
                    "Repository has multiple branches, using only the first one",
                    claude_attrs!(
                        "repository" => repository,
                        "branch_used" => &branches[0],
                        "branches_ignored" => &branches[1..],
                    ),
                );
            }
            validated.add(repository.clone(), vec![branches[0].clone()]);
        }
        validated
    }
}

impl fmt::Display for Outcomes {
    /// 对应 Go：`(*Outcomes).String`。
    ///
    /// 把集合格式化为不包含仓库或分支名称的紧凑摘要。空集合输出
    /// `Outcomes{empty}`，非空集合输出 `Outcomes{repositories: N}`。
    ///
    /// # 参数
    /// - `formatter`：标准格式化目标；本方法不会分配额外仓库列表。
    ///
    /// # 返回
    /// 成功写入时返回 `Ok(())`，底层 formatter 拒绝写入时透传 `fmt::Error`。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            formatter.write_str("Outcomes{empty}")
        } else {
            write!(formatter, "Outcomes{{repositories: {}}}", self.len())
        }
    }
}
