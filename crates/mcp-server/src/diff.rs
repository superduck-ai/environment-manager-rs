//! 保留尾随空行、LCS 平局顺序和三行上下文的统一差异输出。
use std::fmt::Write;
struct Op<'a> {
    kind: char,
    text: &'a str,
    old: usize,
    new: usize,
}
/// 生成参考工具的差异文本，避免通用 diff 库对换行和 hunk 的重新解释。
pub fn unified(old: &str, new: &str, path: &str) -> String {
    if old == new {
        return "(no changes)".into();
    }
    let a: Vec<_> = if old.is_empty() {
        vec![]
    } else {
        old.split('\n').collect()
    };
    let b: Vec<_> = if new.is_empty() {
        vec![]
    } else {
        new.split('\n').collect()
    };
    let prefix = a.iter().zip(&b).take_while(|(a, b)| a == b).count();
    let suffix = a[prefix..]
        .iter()
        .rev()
        .zip(b[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let mut raw: Vec<(char, &str)> = a[..prefix].iter().map(|s| (' ', *s)).collect();
    let am = &a[prefix..a.len() - suffix];
    let bm = &b[prefix..b.len() - suffix];
    if am.len().saturating_mul(bm.len()) > 4_000_000 {
        raw.extend(am.iter().map(|s| ('-', *s)));
        raw.extend(bm.iter().map(|s| ('+', *s)));
    } else {
        let width = bm.len() + 1;
        let mut dp = vec![0u32; (am.len() + 1) * width];
        for i in (0..am.len()).rev() {
            for j in (0..bm.len()).rev() {
                dp[i * width + j] = if am[i] == bm[j] {
                    dp[(i + 1) * width + j + 1] + 1
                } else {
                    dp[(i + 1) * width + j].max(dp[i * width + j + 1])
                };
            }
        }
        let (mut i, mut j) = (0, 0);
        while i < am.len() && j < bm.len() {
            if am[i] == bm[j] {
                raw.push((' ', am[i]));
                i += 1;
                j += 1;
            } else if dp[(i + 1) * width + j] >= dp[i * width + j + 1] {
                raw.push(('-', am[i]));
                i += 1;
            } else {
                raw.push(('+', bm[j]));
                j += 1;
            }
        }
        raw.extend(am[i..].iter().map(|s| ('-', *s)));
        raw.extend(bm[j..].iter().map(|s| ('+', *s)));
    }
    raw.extend(a[a.len() - suffix..].iter().map(|s| (' ', *s)));
    let (mut old_no, mut new_no) = (1, 1);
    let ops: Vec<_> = raw
        .into_iter()
        .map(|(kind, text)| {
            let old = if kind == '+' { 0 } else { old_no };
            let new = if kind == '-' { 0 } else { new_no };
            if kind != '+' {
                old_no += 1;
            }
            if kind != '-' {
                new_no += 1;
            }
            Op {
                kind,
                text,
                old,
                new,
            }
        })
        .collect();
    let mut keep = vec![false; ops.len()];
    for (i, op) in ops.iter().enumerate() {
        if op.kind != ' ' {
            keep[i.saturating_sub(3)..=(i + 3).min(ops.len() - 1)].fill(true);
        }
    }
    if !keep.iter().any(|b| *b) {
        return "(no changes)".into();
    }
    let mut out = format!("--- {path}\n+++ {path}\n");
    let mut i = 0;
    while i < ops.len() {
        if !keep[i] {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        while j < ops.len() && keep[j] {
            j += 1;
        }
        let h = &ops[i..j];
        let old_start = h.iter().find(|op| op.old != 0).map_or(0, |op| op.old);
        let new_start = h.iter().find(|op| op.new != 0).map_or(0, |op| op.new);
        let old_count = h.iter().filter(|op| op.kind != '+').count();
        let new_count = h.iter().filter(|op| op.kind != '-').count();
        writeln!(
            out,
            "@@ -{old_start},{old_count} +{new_start},{new_count} @@"
        )
        .unwrap();
        for op in h {
            writeln!(out, "{}{}", op.kind, op.text).unwrap();
        }
        i = j;
    }
    out.pop();
    out
}
