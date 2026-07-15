//! 从 Go `shared/embedded.go` 机械迁移的内置字节资源。
//!
//! 这些常量会原样写入用户目录，因此包括尾随换行在内的所有字节
//! 都是兼容性的一部分。

/// BYOC 环境缺少 `~/.claude/settings.json` 时写入的默认配置。
pub const DEFAULT_SETTINGS_JSON: &[u8] = br#"{
    "$schema": "https://json.schemastore.org/claude-code-settings.json",
    "hooks": {
        "Stop": [
            {
                "matcher": "",
                "hooks": [
                    {
                        "type": "command",
                        "command": "~/.claude/stop-hook-git-check.sh"
                    }
                ]
            }
        ]
    },
    "permissions": {
        "allow": ["Skill"]
    }
}
"#;

/// BYOC 默认 stop hook，用于防止未提交或未推送的 Git 工作丢失。
pub const STOP_HOOK_SCRIPT: &[u8] = br#"#!/bin/bash

# Read the JSON input from stdin
input=$(cat)

# Check if stop hook is already active (recursion prevention)
stop_hook_active=$(echo "$input" | jq -r '.stop_hook_active')
if [[ "$stop_hook_active" = "true" ]]; then
  exit 0
fi

# Check if we're in a git repository - bail if not
if ! git rev-parse --git-dir >/dev/null 2>&1; then
  exit 0
fi

# Check for uncommitted changes (both staged and unstaged)
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "There are uncommitted changes in the repository. Please commit and push these changes to the remote branch." >&2
  exit 2
fi

# Check for untracked files that might be important
untracked_files=$(git ls-files --others --exclude-standard)
if [[ -n "$untracked_files" ]]; then
  echo "There are untracked files in the repository. Please commit and push these changes to the remote branch." >&2
  exit 2
fi

current_branch=$(git branch --show-current)
if [[ -n "$current_branch" ]]; then
  if git rev-parse "origin/$current_branch" >/dev/null 2>&1; then
    # Branch exists on remote - compare against it
    unpushed=$(git rev-list "origin/$current_branch..HEAD" --count 2>/dev/null) || unpushed=0
    if [[ "$unpushed" -gt 0 ]]; then
      echo "There are $unpushed unpushed commit(s) on branch '$current_branch'. Please push these changes to the remote repository." >&2
      exit 2
    fi
  else
    # Branch doesn't exist on remote - compare against default branch
    unpushed=$(git rev-list "origin/HEAD..HEAD" --count 2>/dev/null) || unpushed=0
    if [[ "$unpushed" -gt 0 ]]; then
      echo "Branch '$current_branch' has $unpushed unpushed commit(s) and no remote branch. Please push these changes to the remote repository." >&2
      exit 2
    fi
  fi
fi

exit 0
"#;

/// Baku 子类型使用的 Claude Code settings。
pub const BAKU_SETTINGS_JSON: &[u8] = br#"{
    "$schema": "https://json.schemastore.org/claude-code-settings.json",
    "hooks": {
        "Stop": [
            {
                "matcher": "",
                "hooks": [
                    {
                        "type": "command",
                        "command": "~/.claude/stop-hook-baku.sh"
                    }
                ]
            }
        ]
    },
    "permissions": {
        "allow": ["Skill"]
    }
}
"#;

/// Baku stop hook，检查 Vite、Git 和 TypeScript，但不要求向远端推送。
pub const BAKU_STOP_HOOK_SCRIPT: &[u8] = r#"#!/bin/bash

# Baku stop hook: checks for Vite dev server errors and uncommitted git changes.
# Unlike the CCR stop hook, this does NOT check for unpushed commits (no remote in Baku).
#
# This hook is ADVISORY. It prompts Claude to review potential issues before stopping,
# but Claude can proceed on the second attempt (stop_hook_active recursion guard).

input=$(cat)

# Recursion prevention: if the stop hook is already active, allow stop.
# This means Claude can always stop on the second attempt after reviewing the issues.
stop_hook_active=$(echo "$input" | jq -r '.stop_hook_active')
if [[ "$stop_hook_active" = "true" ]]; then
  exit 0
fi

has_issues=false

# === Check 1: Vite dev server logs for potential errors ===
LOG_FILE="/tmp/vite-dev.log"

if [[ -f "$LOG_FILE" ]]; then
  # Strip ANSI escape sequences (colors, cursor movement, clear-line)
  recent_logs=$(tail -100 "$LOG_FILE" | sed 's/\x1b\[[0-9;]*[A-Za-z]//g')

  # Broad error pattern to catch potential issues. These may include stale errors
  # that have already been fixed — Claude should check the logs to determine if
  # they are current.
  #
  # Vite / dev server
  ERROR_PATTERN='Internal server error'
  ERROR_PATTERN="$ERROR_PATTERN|Pre-transform error"
  ERROR_PATTERN="$ERROR_PATTERN|server restart failed"
  ERROR_PATTERN="$ERROR_PATTERN|Cannot optimize dependency"
  # Build tools (ESBuild, Rollup)
  ERROR_PATTERN="$ERROR_PATTERN|Transform failed"
  ERROR_PATTERN="$ERROR_PATTERN|Build failed"
  ERROR_PATTERN="$ERROR_PATTERN|Rollup failed to resolve"
  ERROR_PATTERN="$ERROR_PATTERN|PLUGIN_ERROR"
  ERROR_PATTERN="$ERROR_PATTERN|UNRESOLVED_IMPORT"
  # TypeScript
  ERROR_PATTERN="$ERROR_PATTERN|error TS[0-9]+:"
  ERROR_PATTERN="$ERROR_PATTERN|Cannot find module"
  ERROR_PATTERN="$ERROR_PATTERN|Cannot find name"
  ERROR_PATTERN="$ERROR_PATTERN|is not assignable to"
  ERROR_PATTERN="$ERROR_PATTERN|does not exist on type"
  ERROR_PATTERN="$ERROR_PATTERN|has no exported member"
  ERROR_PATTERN="$ERROR_PATTERN|Module not found"
  ERROR_PATTERN="$ERROR_PATTERN|Could not resolve"
  # React / JSX
  ERROR_PATTERN="$ERROR_PATTERN|Unterminated JSX"
  ERROR_PATTERN="$ERROR_PATTERN|Adjacent JSX elements"
  ERROR_PATTERN="$ERROR_PATTERN|Invalid hook call"
  # CSS
  ERROR_PATTERN="$ERROR_PATTERN|CssSyntaxError"
  ERROR_PATTERN="$ERROR_PATTERN|Unknown at rule @"
  # Node.js runtime
  ERROR_PATTERN="$ERROR_PATTERN|SyntaxError:"
  ERROR_PATTERN="$ERROR_PATTERN|TypeError:"
  ERROR_PATTERN="$ERROR_PATTERN|ReferenceError:"
  ERROR_PATTERN="$ERROR_PATTERN|EADDRINUSE"

  if echo "$recent_logs" | grep -qE "$ERROR_PATTERN"; then
    echo "HINT: Potential errors were found in the Vite dev server logs (/tmp/vite-dev.log)." >&2
    echo "These may be stale errors that have already been fixed. Please review the log" >&2
    echo "and use your own judgment to determine if these are current issues that need" >&2
    echo "fixing, or if they can be safely ignored." >&2
    echo "" >&2
    echo "Matching lines from recent logs:" >&2
    echo "$recent_logs" | grep -E "$ERROR_PATTERN" | tail -10 >&2
    has_issues=true
  fi
fi

# === Check 2: Uncommitted git changes (commit only, no push check) ===
# Commits preserve work across session restarts — there is no remote to push to.
if git -C /home/claude/project rev-parse --git-dir >/dev/null 2>&1; then
  if ! git -C /home/claude/project diff --quiet || ! git -C /home/claude/project diff --cached --quiet; then
    echo "" >&2
    echo "There are uncommitted changes. Please commit before stopping so your work is preserved across sessions." >&2
    has_issues=true
  fi

  untracked_files=$(git -C /home/claude/project ls-files --others --exclude-standard --)
  if [[ -n "$untracked_files" ]]; then
    echo "" >&2
    echo "There are untracked files. Please add and commit them before stopping so your work is preserved across sessions." >&2
    has_issues=true
  fi
fi

# === Check 3: TypeScript type errors (Vite does NOT check types) ===
# Vite uses esbuild for transpilation which strips types without checking them.
if [[ -f /home/claude/project/tsconfig.json ]]; then
  tsc_output=$(cd /home/claude/project && npx tsc --noEmit 2>&1)
  tsc_exit=$?
  if [[ $tsc_exit -ne 0 ]]; then
    echo "" >&2
    echo "TypeScript type errors found. Please fix before stopping:" >&2
    echo "$tsc_output" | grep -E "error TS" | tail -15 >&2
    has_issues=true
  fi
fi

if [[ "$has_issues" = true ]]; then
  exit 2
fi

exit 0
"#
.as_bytes();

#[cfg(test)]
mod tests {
    use super::*;

    /// 四份资源都保留 shebang/JSON 起始字节和尾随换行。
    #[test]
    fn embedded_resources_keep_wire_bytes() {
        assert!(DEFAULT_SETTINGS_JSON.starts_with(b"{"));
        assert!(BAKU_SETTINGS_JSON.starts_with(b"{"));
        assert!(STOP_HOOK_SCRIPT.starts_with(b"#!/bin/bash\n"));
        assert!(BAKU_STOP_HOOK_SCRIPT.starts_with(b"#!/bin/bash\n"));
        for resource in [
            DEFAULT_SETTINGS_JSON,
            BAKU_SETTINGS_JSON,
            STOP_HOOK_SCRIPT,
            BAKU_STOP_HOOK_SCRIPT,
        ] {
            assert!(resource.ends_with(b"\n"));
        }
    }
}
