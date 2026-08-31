//! Single, shared scalar-expression evaluator for the SQL engine.
//!
//! Previously `sql/dml.rs` and `sql/select.rs` each had their own, mutually
//! inconsistent copies of "turn a `sqlparser::ast::Expr` into a `Value`"
//! (dml's version couldn't see other columns or do arithmetic at all; one of
//! select's two versions handled `json_extract` in the projection list but
//! not in `WHERE`). This module is the one place that logic lives now, so
//! every part of the engine agrees on what `price * 2`, `json_extract(...)`,
//! `a || b`, `CASE WHEN ...`, etc. mean.

use crate::error::SkvsError;
use crate::schema::{Row, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, UnaryOperator, Value as SqlValue,
};

/// Everything an expression might need to resolve itself: the current row
/// (`None` when evaluating an expression with no row context, e.g. an INSERT
/// VALUES list) and the flat `?` placeholder parameter list.
pub struct EvalCtx<'a> {
    pub row: Option<&'a Row>,
}

impl<'a> EvalCtx<'a> {
    pub fn with_row(row: &'a Row) -> Self {
        EvalCtx { row: Some(row) }
    }
    pub fn no_row() -> Self {
        EvalCtx { row: None }
    }
}

/// SQLite-style truthiness: NULL and zero-ish values are false.
pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Integer(i) => *i != 0,
        Value::Real(f) => *f != 0.0,
        Value::Text(s) => !s.is_empty(),
        Value::Blob(b) => !b.is_empty(),
    }
}

fn bool_val(b: bool) -> Value {
    Value::Integer(if b { 1 } else { 0 })
}

/// Count how many `?` placeholders `expr` consumes, without needing actual
/// param values. Must recurse into exactly the same set of sub-expressions
/// as `eval` does, or the two disagree about how many placeholders a given
/// expression consumed (see `sql/dml.rs`'s UPDATE SET/WHERE param-offset
/// logic, which relies on this).
pub fn count_placeholders(expr: &Expr) -> usize {
    match expr {
        Expr::Value(SqlValue::Placeholder(_)) => 1,
        Expr::Value(_) => 0,
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => 0,
        Expr::UnaryOp { expr, .. } => count_placeholders(expr),
        Expr::Nested(inner) => count_placeholders(inner),
        Expr::BinaryOp { left, right, .. } => count_placeholders(left) + count_placeholders(right),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => count_placeholders(inner),
        Expr::Between { expr, low, high, .. } => {
            count_placeholders(expr) + count_placeholders(low) + count_placeholders(high)
        }
        Expr::InList { expr, list, .. } => {
            count_placeholders(expr) + list.iter().map(count_placeholders).sum::<usize>()
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            count_placeholders(expr) + count_placeholders(pattern)
        }
        Expr::Case { operand, conditions, results, else_result } => {
            operand.as_deref().map(count_placeholders).unwrap_or(0)
                + conditions.iter().map(count_placeholders).sum::<usize>()
                + results.iter().map(count_placeholders).sum::<usize>()
                + else_result.as_deref().map(count_placeholders).unwrap_or(0)
        }
        Expr::Function(func) => func
            .args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => count_placeholders(e),
                FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } => count_placeholders(e),
                _ => 0,
            })
            .sum(),
        _ => 0,
    }
}

/// Evaluate a scalar expression down to a runtime `Value`, resolving column
/// references against `ctx.row` (if any) and substituting `?` placeholders
/// from `params` in left-to-right order via `param_idx`.
pub fn eval(expr: &Expr, ctx: &EvalCtx, params: &[Value], param_idx: &mut usize) -> Value {
    match expr {
        Expr::Identifier(id) => lookup_column(ctx, &id.value),
        Expr::CompoundIdentifier(parts) => {
            // Joins qualify columns from a non-first table as "alias.col" in
            // the combined row (see sql/select.rs::execute_joins). Try the
            // fully-qualified form first, then fall back to the bare column
            // name for unqualified lookups against a single-table row.
            let full = parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>().join(".");
            let val = ctx.row.and_then(|r| r.get(&full)).cloned();
            val.unwrap_or_else(|| {
                parts.last().map(|p| lookup_column(ctx, &p.value)).unwrap_or(Value::Null)
            })
        }
        Expr::Value(v) => eval_literal(v, params, param_idx),
        Expr::Nested(inner) => eval(inner, ctx, params, param_idx),
        Expr::UnaryOp { op, expr } => {
            let inner = eval(expr, ctx, params, param_idx);
            match op {
                UnaryOperator::Minus => match inner {
                    Value::Integer(i) => Value::Integer(-i),
                    Value::Real(f) => Value::Real(-f),
                    other => other,
                },
                UnaryOperator::Plus => inner,
                UnaryOperator::Not => bool_val(!truthy(&inner)),
                _ => inner,
            }
        }
        Expr::BinaryOp { left, op, right } => eval_binary_op(left, op, right, ctx, params, param_idx),
        Expr::IsNull(inner) => bool_val(matches!(eval(inner, ctx, params, param_idx), Value::Null)),
        Expr::IsNotNull(inner) => bool_val(!matches!(eval(inner, ctx, params, param_idx), Value::Null)),
        Expr::Between { expr, negated, low, high } => {
            let v = eval(expr, ctx, params, param_idx);
            let lo = eval(low, ctx, params, param_idx);
            let hi = eval(high, ctx, params, param_idx);
            let in_range = v.compare(&lo) != std::cmp::Ordering::Less && v.compare(&hi) != std::cmp::Ordering::Greater;
            bool_val(in_range != *negated)
        }
        Expr::InList { expr, list, negated } => {
            let v = eval(expr, ctx, params, param_idx);
            let found = list.iter().any(|item| eval(item, ctx, params, param_idx) == v);
            bool_val(found != *negated)
        }
        Expr::Like { negated, expr, pattern, .. } => {
            let v = eval(expr, ctx, params, param_idx);
            let p = eval(pattern, ctx, params, param_idx);
            let matched = match (&v, &p) {
                (Value::Text(s), Value::Text(pat)) => sql_like_match(s, pat, false),
                _ => false,
            };
            bool_val(matched != *negated)
        }
        Expr::ILike { negated, expr, pattern, .. } => {
            let v = eval(expr, ctx, params, param_idx);
            let p = eval(pattern, ctx, params, param_idx);
            let matched = match (&v, &p) {
                (Value::Text(s), Value::Text(pat)) => sql_like_match(s, pat, true),
                _ => false,
            };
            bool_val(matched != *negated)
        }
        Expr::Case { operand, conditions, results, else_result } => {
            for (cond, res) in conditions.iter().zip(results.iter()) {
                let is_match = if let Some(op) = operand {
                    let opv = eval(op, ctx, params, param_idx);
                    let cv = eval(cond, ctx, params, param_idx);
                    opv == cv
                } else {
                    truthy(&eval(cond, ctx, params, param_idx))
                };
                if is_match {
                    return eval(res, ctx, params, param_idx);
                }
            }
            else_result
                .as_deref()
                .map(|e| eval(e, ctx, params, param_idx))
                .unwrap_or(Value::Null)
        }
        Expr::Function(func) => eval_function(func, ctx, params, param_idx),
        Expr::Subquery(_) | Expr::Exists { .. } => Value::Null,
        _ => Value::Null,
    }
}

fn lookup_column(ctx: &EvalCtx, name: &str) -> Value {
    ctx.row.and_then(|r| r.get(name).cloned()).unwrap_or(Value::Null)
}

fn eval_literal(v: &SqlValue, params: &[Value], param_idx: &mut usize) -> Value {
    match v {
        SqlValue::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Value::Integer(i)
            } else if let Ok(f) = n.parse::<f64>() {
                Value::Real(f)
            } else {
                Value::Null
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => Value::Text(s.clone()),
        SqlValue::Null => Value::Null,
        SqlValue::Boolean(b) => Value::Integer(if *b { 1 } else { 0 }),
        SqlValue::Placeholder(_) => {
            let val = params.get(*param_idx).cloned().unwrap_or(Value::Null);
            *param_idx += 1;
            val
        }
        _ => Value::Null,
    }
}

/// SQL `LIKE` matching: `%` = any run of characters, `_` = exactly one
/// character. Case-sensitive (matches SQLite's default BINARY collation for
/// LIKE on ASCII; `ILIKE` callers should lowercase both sides first).
fn sql_like_match(text: &str, pattern: &str, case_insensitive_hint: bool) -> bool {
    let (text, pattern) = if case_insensitive_hint {
        (text.to_lowercase(), pattern.to_lowercase())
    } else {
        (text.to_string(), pattern.to_string())
    };
    like_match_chars(&text.chars().collect::<Vec<_>>(), &pattern.chars().collect::<Vec<_>>())
}

fn like_match_chars(text: &[char], pattern: &[char]) -> bool {
    // Standard DP-free recursive glob matcher for SQL LIKE (%, _).
    match pattern.first() {
        None => text.is_empty(),
        Some('%') => {
            like_match_chars(text, &pattern[1..])
                || (!text.is_empty() && like_match_chars(&text[1..], pattern))
        }
        Some('_') => !text.is_empty() && like_match_chars(&text[1..], &pattern[1..]),
        Some(c) => {
            !text.is_empty() && text[0] == *c && like_match_chars(&text[1..], &pattern[1..])
        }
    }
}

fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Real(f) => Some(*f),
        _ => None,
    }
}

fn to_display_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => format!("x'{}'", hex::encode(b)),
    }
}

fn eval_binary_op(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    ctx: &EvalCtx,
    params: &[Value],
    param_idx: &mut usize,
) -> Value {
    // AND/OR use SQL truthiness, not value equality, and short-circuit like
    // any other language's boolean operators.
    match op {
        BinaryOperator::And => {
            let l = eval(left, ctx, params, param_idx);
            if !truthy(&l) {
                return bool_val(false);
            }
            let r = eval(right, ctx, params, param_idx);
            return bool_val(truthy(&r));
        }
        BinaryOperator::Or => {
            let l = eval(left, ctx, params, param_idx);
            if truthy(&l) {
                let _ = eval(right, ctx, params, param_idx); // keep param_idx consistent isn't needed further, but stay consistent with And
                return bool_val(true);
            }
            let r = eval(right, ctx, params, param_idx);
            return bool_val(truthy(&r));
        }
        _ => {}
    }

    let l = eval(left, ctx, params, param_idx);
    let r = eval(right, ctx, params, param_idx);

    match op {
        BinaryOperator::Eq => bool_val(l == r),
        BinaryOperator::NotEq => bool_val(l != r),
        BinaryOperator::Gt => bool_val(l.compare(&r) == std::cmp::Ordering::Greater),
        BinaryOperator::Lt => bool_val(l.compare(&r) == std::cmp::Ordering::Less),
        BinaryOperator::GtEq => bool_val(l.compare(&r) != std::cmp::Ordering::Less),
        BinaryOperator::LtEq => bool_val(l.compare(&r) != std::cmp::Ordering::Greater),
        BinaryOperator::StringConcat => Value::Text(format!("{}{}", to_display_string(&l), to_display_string(&r))),
        BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Modulo => {
            arithmetic(&l, op, &r)
        }
        _ => Value::Null,
    }
}

fn arithmetic(l: &Value, op: &BinaryOperator, r: &Value) -> Value {
    let (Some(lf), Some(rf)) = (to_f64(l), to_f64(r)) else { return Value::Null };
    let both_int = matches!(l, Value::Integer(_)) && matches!(r, Value::Integer(_));

    match op {
        BinaryOperator::Plus => numeric_result(lf + rf, both_int),
        BinaryOperator::Minus => numeric_result(lf - rf, both_int),
        BinaryOperator::Multiply => numeric_result(lf * rf, both_int),
        BinaryOperator::Divide => {
            if rf == 0.0 {
                Value::Null
            } else {
                numeric_result(lf / rf, false) // SQL division always promotes to real-ish unless exact
            }
        }
        BinaryOperator::Modulo => {
            if rf == 0.0 {
                Value::Null
            } else if both_int {
                if let (Value::Integer(li), Value::Integer(ri)) = (l, r) {
                    Value::Integer(li % ri)
                } else {
                    Value::Real(lf % rf)
                }
            } else {
                Value::Real(lf % rf)
            }
        }
        _ => Value::Null,
    }
}

fn numeric_result(f: f64, prefer_int: bool) -> Value {
    if prefer_int && f.fract() == 0.0 && f.abs() < i64::MAX as f64 {
        Value::Integer(f as i64)
    } else {
        Value::Real(f)
    }
}

fn eval_function(
    func: &sqlparser::ast::Function,
    ctx: &EvalCtx,
    params: &[Value],
    param_idx: &mut usize,
) -> Value {
    let name = func.name.to_string().to_lowercase();
    let args: Vec<Value> = func
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => eval(e, ctx, params, param_idx),
            FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } => eval(e, ctx, params, param_idx),
            _ => Value::Null,
        })
        .collect();

    match name.as_str() {
        "json_extract" => {
            let json_str = match args.first() {
                Some(Value::Text(s)) => s.clone(),
                _ => return Value::Null,
            };
            let path = match args.get(1) {
                Some(Value::Text(s)) => s.clone(),
                _ => "$".to_string(),
            };
            crate::json::json_extract(&json_str, &path).unwrap_or(Value::Null)
        }
        "json_array" => crate::json::json_array(&args).map(Value::Text).unwrap_or(Value::Null),
        "json_object" => {
            let mut keys = Vec::new();
            let mut vals = Vec::new();
            let mut it = args.iter();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                keys.push(match k {
                    Value::Text(s) => s.clone(),
                    other => to_display_string(other),
                });
                vals.push(v.clone());
            }
            crate::json::json_object(&keys, &vals).map(Value::Text).unwrap_or(Value::Null)
        }
        "json_set" => {
            let json_str = match args.first() {
                Some(Value::Text(s)) => s.clone(),
                _ => return Value::Null,
            };
            let path = match args.get(1) {
                Some(Value::Text(s)) => s.clone(),
                _ => return Value::Null,
            };
            let new_val = args.get(2).cloned().unwrap_or(Value::Null);
            crate::json::json_set(&json_str, &path, &new_val).map(Value::Text).unwrap_or(Value::Null)
        }
        "upper" => match args.first() {
            Some(Value::Text(s)) => Value::Text(s.to_uppercase()),
            other => other.cloned().unwrap_or(Value::Null),
        },
        "lower" => match args.first() {
            Some(Value::Text(s)) => Value::Text(s.to_lowercase()),
            other => other.cloned().unwrap_or(Value::Null),
        },
        "length" | "len" => match args.first() {
            Some(Value::Text(s)) => Value::Integer(s.chars().count() as i64),
            Some(Value::Blob(b)) => Value::Integer(b.len() as i64),
            _ => Value::Null,
        },
        "abs" => match args.first() {
            Some(Value::Integer(i)) => Value::Integer(i.abs()),
            Some(Value::Real(f)) => Value::Real(f.abs()),
            _ => Value::Null,
        },
        "round" => {
            let places = match args.get(1) {
                Some(Value::Integer(i)) => *i as i32,
                _ => 0,
            };
            match to_f64(args.first().unwrap_or(&Value::Null)) {
                Some(f) => {
                    let mult = 10f64.powi(places);
                    Value::Real((f * mult).round() / mult)
                }
                None => Value::Null,
            }
        }
        "coalesce" => args.into_iter().find(|v| !matches!(v, Value::Null)).unwrap_or(Value::Null),
        "ifnull" => {
            let a = args.first().cloned().unwrap_or(Value::Null);
            if matches!(a, Value::Null) {
                args.get(1).cloned().unwrap_or(Value::Null)
            } else {
                a
            }
        }
        "nullif" => {
            let a = args.first().cloned().unwrap_or(Value::Null);
            let b = args.get(1).cloned().unwrap_or(Value::Null);
            if a == b { Value::Null } else { a }
        }
        "trim" => match args.first() {
            Some(Value::Text(s)) => Value::Text(s.trim().to_string()),
            other => other.cloned().unwrap_or(Value::Null),
        },
        "ltrim" => match args.first() {
            Some(Value::Text(s)) => Value::Text(s.trim_start().to_string()),
            other => other.cloned().unwrap_or(Value::Null),
        },
        "rtrim" => match args.first() {
            Some(Value::Text(s)) => Value::Text(s.trim_end().to_string()),
            other => other.cloned().unwrap_or(Value::Null),
        },
        "substr" | "substring" => match args.first() {
            Some(Value::Text(s)) => {
                let chars: Vec<char> = s.chars().collect();
                let start = match args.get(1) {
                    Some(Value::Integer(i)) => *i,
                    _ => 1,
                };
                // SQLite: 1-based; negative counts from the end.
                let start_idx: i64 = if start > 0 { start - 1 } else { (chars.len() as i64 + start).max(0) };
                let start_idx = start_idx.max(0) as usize;
                let len = match args.get(2) {
                    Some(Value::Integer(i)) => (*i).max(0) as usize,
                    _ => chars.len().saturating_sub(start_idx),
                };
                let end = (start_idx + len).min(chars.len());
                if start_idx >= chars.len() {
                    Value::Text(String::new())
                } else {
                    Value::Text(chars[start_idx..end].iter().collect())
                }
            }
            _ => Value::Null,
        },
        "replace" => match (args.first(), args.get(1), args.get(2)) {
            (Some(Value::Text(s)), Some(Value::Text(from)), Some(Value::Text(to))) => {
                Value::Text(s.replace(from.as_str(), to))
            }
            _ => Value::Null,
        },
        "typeof" => Value::Text(match args.first() {
            Some(Value::Null) | None => "null",
            Some(Value::Integer(_)) => "integer",
            Some(Value::Real(_)) => "real",
            Some(Value::Text(_)) => "text",
            Some(Value::Blob(_)) => "blob",
        }.to_string()),
        "hex" => match args.first() {
            Some(Value::Blob(b)) => Value::Text(hex::encode(b)),
            Some(Value::Text(s)) => Value::Text(hex::encode(s.as_bytes())),
            _ => Value::Null,
        },
        _ => Value::Null,
    }
}

#[allow(dead_code)]
pub type ExprResult = Result<Value, SkvsError>;

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_expr(s: &str) -> Expr {
        let sql = format!("SELECT {}", s);
        let stmts = Parser::parse_sql(&GenericDialect {}, &sql).unwrap();
        if let sqlparser::ast::Statement::Query(q) = &stmts[0] {
            if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let sqlparser::ast::SelectItem::UnnamedExpr(e) = &sel.projection[0] {
                    return e.clone();
                }
            }
        }
        panic!("could not parse expr");
    }

    #[test]
    fn arithmetic_promotes_to_real_only_when_needed() {
        let mut idx = 0usize;
        let ctx = EvalCtx::no_row();
        assert_eq!(eval(&parse_expr("2 + 3"), &ctx, &[], &mut idx), Value::Integer(5));
        assert_eq!(eval(&parse_expr("1 / 2"), &ctx, &[], &mut idx), Value::Real(0.5));
    }

    #[test]
    fn like_wildcards() {
        assert!(sql_like_match("hello world", "hello%", false));
        assert!(sql_like_match("hello", "h_llo", false));
        assert!(!sql_like_match("hello", "world", false));
    }

    #[test]
    fn case_expr() {
        let mut idx = 0usize;
        let ctx = EvalCtx::no_row();
        let v = eval(&parse_expr("CASE WHEN 1 = 2 THEN 'a' ELSE 'b' END"), &ctx, &[], &mut idx);
        assert_eq!(v, Value::Text("b".into()));
    }
}
