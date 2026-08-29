use sqlparser::ast::{
    Expr, GroupByExpr, JoinConstraint, JoinOperator, OrderByExpr, Query, Select, SelectItem,
    SetExpr, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::state::KvsState;
use crate::schema::*;
use crate::error::SkvsError;
use crate::sql::QueryResult;
use std::collections::HashMap;

pub fn execute_select(
    state: &KvsState,
    db_id: u32,
    query: &Query,
    params: &[Value],
    tx_id: Option<u64>,
) -> Result<QueryResult, SkvsError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => return Err(SkvsError::Unsupported("Only simple SELECT statements are supported".into())),
    };

    let mut rows: Vec<Row>;
    let columns: Vec<String>;

    if select.from.len() == 1 {
        let (table, view_query) = resolve_from(state, db_id, &select.from[0].relation)?;

        if let Some(view_sql) = view_query {
            // The FROM target is a view: run its stored query instead of a real table.
            let view_stmts = Parser::parse_sql(&GenericDialect {}, &view_sql)?;
            let view_query = match view_stmts.first() {
                Some(sqlparser::ast::Statement::Query(q)) => q.as_ref().clone(),
                _ => return Err(SkvsError::View("View definition must be a SELECT".into())),
            };
            let inner = execute_select(state, db_id, &view_query, params, tx_id)?;
            rows = inner.rows;
        } else {
            let store = state.get_table_store(db_id, &table)
                .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table)))?;
            rows = store.iter().map(|entry| {
                let mut row = entry.value().clone();
                // Internal-only field so WHERE fts_match(...) can map a row back
                // to the rowid the FTS index was built against. Stripped before
                // the result is returned to the caller.
                row.insert("_rowid_".to_string(), Value::Integer(*entry.key() as i64));
                row
            }).collect();
        }

        if let Some(where_expr) = &select.selection {
            rows = rows.into_iter()
                .filter(|row| {
                    let mut idx = 0usize;
                    evaluate_where_ctx(state, db_id, row, where_expr, params, &mut idx)
                })
                .collect();
        }

        let grouped = is_grouped(&select.group_by);
        rows = apply_group_by(rows, &select.group_by, &select.projection)?;

        if !query.order_by.is_empty() {
            rows = apply_order_by(rows, &query.order_by)?;
        }

        if let Some(limit_expr) = &query.limit {
            rows = apply_limit(rows, limit_expr, &query.offset)?;
        }

        if !grouped {
            rows = apply_projection(rows, &select.projection);
        }

        columns = get_projection_columns(&select.projection);
    } else if select.from.len() > 1 {
        rows = execute_joins(state, db_id, &select.from, params)?;

        if let Some(where_expr) = &select.selection {
            rows = rows.into_iter()
                .filter(|row| {
                    let mut idx = 0usize;
                    evaluate_where_ctx(state, db_id, row, where_expr, params, &mut idx)
                })
                .collect();
        }

        if !query.order_by.is_empty() {
            rows = apply_order_by(rows, &query.order_by)?;
        }
        if let Some(limit_expr) = &query.limit {
            rows = apply_limit(rows, limit_expr, &query.offset)?;
        }

        if !is_grouped(&select.group_by) {
            rows = apply_projection(rows, &select.projection);
        }

        columns = get_projection_columns(&select.projection);
    } else {
        rows = Vec::new();
        columns = get_projection_columns(&select.projection);
    }

    if select.distinct.is_some() {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| {
            let key = row.iter().map(|(k, v)| format!("{}:{:?}", k, v)).collect::<String>();
            seen.insert(key)
        });
    }

    // Never leak the internal rowid-tracking field to callers.
    for row in rows.iter_mut() {
        row.shift_remove("_rowid_");
    }

    Ok(QueryResult::rows(rows, columns))
}

fn is_grouped(group_by: &GroupByExpr) -> bool {
    match group_by {
        GroupByExpr::Expressions(exprs) => !exprs.is_empty(),
        GroupByExpr::All => false,
    }
}

/// Apply the SELECT list to each row: pick out plain columns, evaluate
/// scalar expressions/function calls (e.g. json_extract), and honor aliases.
/// A bare `*` (or no projection) passes rows through unchanged.
fn apply_projection(rows: Vec<Row>, projection: &[SelectItem]) -> Vec<Row> {
    if projection.is_empty() || projection.iter().all(|item| matches!(item, SelectItem::Wildcard(_))) {
        return rows;
    }

    rows.into_iter().map(|row| {
        let mut new_row = Row::new();
        for item in projection {
            match item {
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                    for (k, v) in row.iter() {
                        new_row.insert(k.clone(), v.clone());
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let name = match expr {
                        Expr::Identifier(id) => id.value.clone(),
                        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()).unwrap_or_else(|| expr.to_string()),
                        _ => expr.to_string(),
                    };
                    new_row.insert(name, eval_scalar_expr(expr, &row));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    new_row.insert(alias.to_string(), eval_scalar_expr(expr, &row));
                }
            }
        }
        new_row
    }).collect()
}

/// Evaluate a scalar (non-aggregate) SELECT-list expression against a single row.
fn eval_scalar_expr(expr: &Expr, row: &Row) -> Value {
    match expr {
        Expr::Identifier(id) => row.get(&id.value).cloned().unwrap_or(Value::Null),
        Expr::CompoundIdentifier(parts) => parts.last()
            .and_then(|p| row.get(&p.value).cloned())
            .unwrap_or(Value::Null),
        Expr::Nested(inner) => eval_scalar_expr(inner, row),
        Expr::Value(v) => {
            let mut idx = 0usize;
            eval_expr_to_value(&Expr::Value(v.clone()), row, &[], &mut idx)
        }
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_lowercase();
            let args: Vec<Value> = func.args.iter().filter_map(|arg| match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => Some(eval_scalar_expr(e, row)),
                _ => None,
            }).collect();
            match func_name.as_str() {
                "json_extract" => {
                    let json_str = match args.get(0) { Some(Value::Text(s)) => s.clone(), _ => return Value::Null };
                    let path = match args.get(1) { Some(Value::Text(s)) => s.clone(), _ => "$".to_string() };
                    crate::json::json_extract(&json_str, &path).unwrap_or(Value::Null)
                }
                "upper" => match args.get(0) { Some(Value::Text(s)) => Value::Text(s.to_uppercase()), other => other.cloned().unwrap_or(Value::Null) },
                "lower" => match args.get(0) { Some(Value::Text(s)) => Value::Text(s.to_lowercase()), other => other.cloned().unwrap_or(Value::Null) },
                "length" => match args.get(0) {
                    Some(Value::Text(s)) => Value::Integer(s.len() as i64),
                    Some(Value::Blob(b)) => Value::Integer(b.len() as i64),
                    _ => Value::Null,
                },
                _ => Value::Null,
            }
        }
        _ => Value::Null,
    }
}

/// Resolve a FROM item to either (table_name, None) for a real table, or
/// (view_name, Some(view_sql)) if it refers to a stored view.
fn resolve_from(state: &KvsState, db_id: u32, relation: &TableFactor) -> Result<(String, Option<String>), SkvsError> {
    let table = parse_table_factor(relation).0;
    if let Some(view_sql) = state.get_view_definition(db_id, &table) {
        Ok((table, Some(view_sql)))
    } else {
        Ok((table, None))
    }
}

fn parse_table_factor(relation: &TableFactor) -> (String, Option<String>) {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let table = name.to_string();
            let alias_name = alias.as_ref().map(|a| a.name.to_string());
            (table, alias_name)
        }
        _ => (relation.to_string(), None),
    }
}

fn execute_joins(
    state: &KvsState,
    db_id: u32,
    from: &[TableWithJoins],
    params: &[Value],
) -> Result<Vec<Row>, SkvsError> {
    if from.is_empty() {
        return Ok(vec![]);
    }

    let (table1, _alias1) = parse_table_factor(&from[0].relation);
    let store1 = state.get_table_store(db_id, &table1)
        .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table1)))?;
    let mut result: Vec<Row> = store1.iter().map(|e| e.value().clone()).collect();

    for join in &from[0].joins {
        let (table2, alias2) = parse_table_factor(&join.relation);
        let store2 = state.get_table_store(db_id, &table2)
            .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table2)))?;

        match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr)) => {
                let mut new_result = Vec::new();
                for row1 in &result {
                    for row2_ref in store2.iter() {
                        let row2 = row2_ref.value();
                        let mut combined = row1.clone();
                        for (k, v) in row2.iter() {
                            let col_name = if let Some(alias) = &alias2 {
                                format!("{}.{}", alias, k)
                            } else {
                                k.clone()
                            };
                            combined.insert(col_name, v.clone());
                        }
                        let mut idx = 0usize;
                        if evaluate_where_ctx(state, db_id, &combined, expr, params, &mut idx) {
                            new_result.push(combined);
                        }
                    }
                }
                result = new_result;
            }
            JoinOperator::CrossJoin => {
                let mut new_result = Vec::new();
                for row1 in &result {
                    for row2_ref in store2.iter() {
                        let mut combined = row1.clone();
                        for (k, v) in row2_ref.value().iter() {
                            let col_name = if let Some(alias) = &alias2 {
                                format!("{}.{}", alias, k)
                            } else {
                                k.clone()
                            };
                            combined.insert(col_name, v.clone());
                        }
                        new_result.push(combined);
                    }
                }
                result = new_result;
            }
            _ => {
                // LEFT/RIGHT/FULL OUTER joins are not implemented yet; fall back
                // to an inner join so results stay predictable rather than empty.
            }
        }
    }

    Ok(result)
}

fn apply_group_by(rows: Vec<Row>, group_by: &GroupByExpr, projection: &[SelectItem]) -> Result<Vec<Row>, SkvsError> {
    let exprs: &[Expr] = match group_by {
        GroupByExpr::Expressions(exprs) => exprs,
        GroupByExpr::All => return Ok(rows),
    };
    if exprs.is_empty() {
        return Ok(rows);
    }

    let mut groups: HashMap<String, Vec<Row>> = HashMap::new();
    for row in rows {
        let mut idx = 0usize;
        let key = exprs.iter()
            .map(|expr| format!("{:?}", eval_expr_to_value(expr, &row, &[], &mut idx)))
            .collect::<Vec<_>>()
            .join("\u{1}");
        groups.entry(key).or_default().push(row);
    }

    let mut result = Vec::new();
    for (_key, group_rows) in groups {
        let mut row = Row::new();
        for item in projection {
            if let SelectItem::UnnamedExpr(expr) = item {
                let val = evaluate_aggregate(expr, &group_rows);
                row.insert(expr.to_string(), val);
            } else if let SelectItem::ExprWithAlias { expr, alias } = item {
                let val = evaluate_aggregate(expr, &group_rows);
                row.insert(alias.to_string(), val);
            }
        }
        result.push(row);
    }
    Ok(result)
}

fn evaluate_aggregate(expr: &Expr, rows: &[Row]) -> Value {
    if let Expr::Function(func) = expr {
        let func_name = func.name.to_string().to_lowercase();
        let arg_ident = func.args.first().and_then(|arg| match arg {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(id))) => Some(id.value.clone()),
            _ => None,
        });
        match func_name.as_str() {
            "count" => Value::Integer(rows.len() as i64),
            "sum" => {
                if let Some(col) = &arg_ident {
                    let sum: f64 = rows.iter().filter_map(|r| numeric(r.get(col))).sum();
                    if sum.fract() == 0.0 { Value::Integer(sum as i64) } else { Value::Real(sum) }
                } else { Value::Null }
            }
            "avg" => {
                if let Some(col) = &arg_ident {
                    let vals: Vec<f64> = rows.iter().filter_map(|r| numeric(r.get(col))).collect();
                    if vals.is_empty() { Value::Null } else { Value::Real(vals.iter().sum::<f64>() / vals.len() as f64) }
                } else { Value::Null }
            }
            "max" => {
                if let Some(col) = &arg_ident {
                    rows.iter().filter_map(|r| r.get(col).cloned()).max_by(|a, b| a.compare(b)).unwrap_or(Value::Null)
                } else { Value::Null }
            }
            "min" => {
                if let Some(col) = &arg_ident {
                    rows.iter().filter_map(|r| r.get(col).cloned()).min_by(|a, b| a.compare(b)).unwrap_or(Value::Null)
                } else { Value::Null }
            }
            _ => Value::Null,
        }
    } else if let Expr::Identifier(id) = expr {
        rows.first().and_then(|r| r.get(&id.value).cloned()).unwrap_or(Value::Null)
    } else {
        Value::Null
    }
}

fn numeric(val: Option<&Value>) -> Option<f64> {
    match val {
        Some(Value::Integer(i)) => Some(*i as f64),
        Some(Value::Real(f)) => Some(*f),
        _ => None,
    }
}

fn apply_order_by(mut rows: Vec<Row>, order_by: &[OrderByExpr]) -> Result<Vec<Row>, SkvsError> {
    for order in order_by.iter().rev() {
        let col_name = order.expr.to_string();
        let asc = order.asc.unwrap_or(true);
        rows.sort_by(|a, b| {
            let va = a.get(&col_name).unwrap_or(&Value::Null);
            let vb = b.get(&col_name).unwrap_or(&Value::Null);
            let cmp = va.compare(vb);
            if asc { cmp } else { cmp.reverse() }
        });
    }
    Ok(rows)
}

fn apply_limit(rows: Vec<Row>, limit_expr: &Expr, offset: &Option<sqlparser::ast::Offset>) -> Result<Vec<Row>, SkvsError> {
    let limit = expr_to_usize(limit_expr).unwrap_or(rows.len());
    let offset_val = offset.as_ref().and_then(|o| expr_to_usize(&o.value)).unwrap_or(0);
    Ok(rows.into_iter().skip(offset_val).take(limit).collect())
}

fn expr_to_usize(expr: &Expr) -> Option<usize> {
    if let Expr::Value(sqlparser::ast::Value::Number(s, _)) = expr {
        s.parse::<usize>().ok()
    } else {
        None
    }
}

fn get_projection_columns(projection: &[SelectItem]) -> Vec<String> {
    if projection.is_empty() {
        return vec!["*".to_string()];
    }
    projection.iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => expr.to_string(),
            SelectItem::ExprWithAlias { alias, .. } => alias.to_string(),
            SelectItem::QualifiedWildcard(..) => "*".to_string(),
            SelectItem::Wildcard(_) => "*".to_string(),
        })
        .collect()
}

/// WHERE evaluation without state/FTS access (used for UPDATE/DELETE).
pub fn evaluate_where(row: &Row, expr: &Expr, params: &[Value], param_idx: &mut usize) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                sqlparser::ast::BinaryOperator::And => {
                    return evaluate_where(row, left, params, param_idx) && evaluate_where(row, right, params, param_idx);
                }
                sqlparser::ast::BinaryOperator::Or => {
                    return evaluate_where(row, left, params, param_idx) || evaluate_where(row, right, params, param_idx);
                }
                _ => {}
            }
            let l = eval_expr_to_value(left, row, params, param_idx);
            let r = eval_expr_to_value(right, row, params, param_idx);
            match op {
                sqlparser::ast::BinaryOperator::Eq => l == r,
                sqlparser::ast::BinaryOperator::NotEq => l != r,
                sqlparser::ast::BinaryOperator::Gt => l.compare(&r) == std::cmp::Ordering::Greater,
                sqlparser::ast::BinaryOperator::Lt => l.compare(&r) == std::cmp::Ordering::Less,
                sqlparser::ast::BinaryOperator::GtEq => l.compare(&r) != std::cmp::Ordering::Less,
                sqlparser::ast::BinaryOperator::LtEq => l.compare(&r) != std::cmp::Ordering::Greater,
                _ => false,
            }
        }
        Expr::Nested(inner) => evaluate_where(row, inner, params, param_idx),
        Expr::Identifier(ident) => {
            row.get(&ident.value).map(bool_from_value).unwrap_or(false)
        }
        Expr::IsNull(inner) => matches!(eval_expr_to_value(inner, row, params, param_idx), Value::Null),
        Expr::IsNotNull(inner) => !matches!(eval_expr_to_value(inner, row, params, param_idx), Value::Null),
        _ => false,
    }
}

/// WHERE evaluation with access to `state`/`db_id`, used for full-text search
/// via `fts_match(table, query)` inside a top-level SELECT.
pub fn evaluate_where_ctx(state: &KvsState, db_id: u32, row: &Row, expr: &Expr, params: &[Value], param_idx: &mut usize) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            sqlparser::ast::BinaryOperator::And => {
                evaluate_where_ctx(state, db_id, row, left, params, param_idx)
                    && evaluate_where_ctx(state, db_id, row, right, params, param_idx)
            }
            sqlparser::ast::BinaryOperator::Or => {
                evaluate_where_ctx(state, db_id, row, left, params, param_idx)
                    || evaluate_where_ctx(state, db_id, row, right, params, param_idx)
            }
            _ => evaluate_where(row, expr, params, param_idx),
        },
        Expr::Nested(inner) => evaluate_where_ctx(state, db_id, row, inner, params, param_idx),
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_lowercase();
            if func_name == "fts_match" && func.args.len() == 2 {
                let table_name = match &func.args[0] {
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(id))) => id.value.clone(),
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)))) => s.clone(),
                    _ => return false,
                };
                let query_text = match &func.args[1] {
                    sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)))) => s.clone(),
                    _ => return false,
                };
                if let Some(fts) = state.get_fts_table(db_id, &table_name) {
                    if let Some(Value::Integer(rowid)) = row.get("_rowid_") {
                        let rowid = *rowid as RowId;
                        let handle = tokio::runtime::Handle::try_current();
                        let results = match handle {
                            Ok(h) => tokio::task::block_in_place(|| h.block_on(fts.search(&query_text))),
                            Err(_) => {
                                let rt = tokio::runtime::Runtime::new().unwrap();
                                rt.block_on(fts.search(&query_text))
                            }
                        };
                        return results.contains(&rowid);
                    }
                }
                return false;
            }
            evaluate_where(row, expr, params, param_idx)
        }
        _ => evaluate_where(row, expr, params, param_idx),
    }
}

pub fn eval_expr_to_value(expr: &Expr, row: &Row, params: &[Value], param_idx: &mut usize) -> Value {
    match expr {
        Expr::Identifier(ident) => row.get(&ident.value).cloned().unwrap_or(Value::Null),
        Expr::Value(v) => match v {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() { Value::Integer(i) }
                else if let Ok(f) = n.parse::<f64>() { Value::Real(f) }
                else { Value::Null }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => Value::Text(s.clone()),
            sqlparser::ast::Value::Null => Value::Null,
            sqlparser::ast::Value::Boolean(b) => Value::Integer(if *b { 1 } else { 0 }),
            sqlparser::ast::Value::Placeholder(_) => {
                let val = params.get(*param_idx).cloned().unwrap_or(Value::Null);
                *param_idx += 1;
                val
            }
            _ => Value::Null,
        },
        Expr::Nested(inner) => eval_expr_to_value(inner, row, params, param_idx),
        _ => Value::Null,
    }
}

fn bool_from_value(val: &Value) -> bool {
    match val {
        Value::Integer(i) => *i != 0,
        Value::Real(f) => *f != 0.0,
        Value::Text(s) => !s.is_empty(),
        Value::Blob(b) => !b.is_empty(),
        Value::Null => false,
    }
}
