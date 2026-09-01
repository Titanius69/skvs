use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, JoinConstraint, JoinOperator, OrderByExpr, Query,
    SelectItem, SetExpr, TableFactor, TableWithJoins, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::state::KvsState;
use crate::schema::*;
use crate::error::SkvsError;
use crate::sql::QueryResult;
use crate::expr::{self, truthy, EvalCtx};
use std::collections::HashMap;
use std::ops::Bound;

pub fn execute_select(
    state: &KvsState,
    db_id: u32,
    query: &Query,
    params: &[Value],
    tx_id: Option<u32>,
) -> Result<QueryResult, SkvsError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => return Err(SkvsError::Unsupported("Only simple SELECT statements are supported".into())),
    };

    let mut rows: Vec<Row>;
    let mut schema_for_wildcard: Option<std::sync::Arc<TableSchema>> = None;

    if select.from.len() == 1 && select.from[0].joins.is_empty() {
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
            schema_for_wildcard = state.get_schema(db_id, &table);
            rows = rows_for_simple_where(state, db_id, &table, &select.selection, params)?;
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
            rows = apply_projection(rows, &select.projection, params);
        }
    } else if select.from.len() > 1 || (!select.from.is_empty() && !select.from[0].joins.is_empty()) {
        rows = execute_joins(state, db_id, &select.from, params)?;

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
            rows = apply_projection(rows, &select.projection, params);
        }
    } else {
        rows = Vec::new();
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

    // Column names: for an explicit projection list (no wildcard) these are
    // known statically. For `SELECT *` (or a mix like `SELECT id, *`) the
    // actual output columns depend on the row's own keys - the projection
    // step above already expanded `*` per-row, so read them back off the
    // first row. Only when there are no matching rows at all do we fall back
    // to the table's schema (single-table case) or, failing that, `["*"]`.
    let columns = if is_pure_or_mixed_wildcard(&select.projection) {
        if let Some(first) = rows.first() {
            first.keys().cloned().collect()
        } else if let Some(schema) = &schema_for_wildcard {
            schema.columns.keys().cloned().collect()
        } else {
            vec!["*".to_string()]
        }
    } else {
        get_projection_columns(&select.projection)
    };

    Ok(QueryResult::rows(rows, columns))
}

fn is_pure_or_mixed_wildcard(projection: &[SelectItem]) -> bool {
    projection.is_empty() || projection.iter().any(|item| matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)))
}

/// Fetch the candidate rows for a single-table (no JOIN) query. When the
/// WHERE clause is a single top-level `column = <constant>` predicate and
/// that column has a secondary index, the index is used to fetch only the
/// matching rowids directly instead of scanning every row in the table;
/// otherwise this just returns every row, same as before (the WHERE clause
/// is still re-applied in full afterwards either way, so this is purely an
/// optimization and can never change the result set).
fn rows_for_simple_where(
    state: &KvsState,
    db_id: u32,
    table: &str,
    selection: &Option<Expr>,
    params: &[Value],
) -> Result<Vec<Row>, SkvsError> {
    if let Some(where_expr) = selection {
        if let Some((col, val)) = simple_equality(where_expr, params) {
            if let Some(schema) = state.get_schema(db_id, table) {
                if let Some(idx) = schema.indices.iter().find(|i| i.columns.len() == 1 && i.columns[0] == col) {
                    let ids = crate::index::lookup_index(state, db_id, table, &idx.name, &val);
                    return rows_from_ids(state, db_id, table, ids);
                }
            }
        }
        if let Some((col, lower, upper)) = simple_range(where_expr, params) {
            if let Some(schema) = state.get_schema(db_id, table) {
                if let Some(idx) = schema.indices.iter().find(|i| i.columns.len() == 1 && i.columns[0] == col) {
                    let ids = crate::index::range_lookup(state, db_id, table, &idx.name, lower, upper);
                    return rows_from_ids(state, db_id, table, ids);
                }
            }
        }
    }

    let store = state.get_table_store(db_id, table)
        .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table)))?;
    Ok(store.iter().map(|entry| {
        let mut row = entry.value().clone();
        // Internal-only field so WHERE fts_match(...) can map a row back
        // to the rowid the FTS index was built against. Stripped before
        // the result is returned to the caller.
        row.insert("_rowid_".to_string(), Value::Integer(*entry.key() as i64));
        row
    }).collect())
}

/// Turns a list of rowids straight into their rows (tagging each with the
/// internal `_rowid_` field), used by both the equality and range index
/// fast paths in `rows_for_simple_where` so an indexed WHERE never has to
/// touch rows it can already rule out.
fn rows_from_ids(state: &KvsState, db_id: u32, table: &str, ids: Vec<RowId>) -> Result<Vec<Row>, SkvsError> {
    let store = state.get_table_store(db_id, table)
        .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table)))?;
    Ok(ids.into_iter().filter_map(|rowid| {
        store.get(&rowid).map(|r| {
            let mut row = r.clone();
            row.insert("_rowid_".to_string(), Value::Integer(rowid as i64));
            row
        })
    }).collect())
}

/// Recognizes a WHERE clause that is *exactly* one range comparison on a
/// single column against a literal/param (`col > x`, `x <= col`, or
/// `col BETWEEN a AND b`), with nothing else (no surrounding AND/OR).
/// Mirrors `simple_equality` below, just for `<`/`<=`/`>`/`>=`/`BETWEEN`
/// instead of `=`. Anything more complex falls back to a full scan; the
/// WHERE clause is still re-applied in full afterwards either way, so this
/// is purely an optimization and can never change the result set.
fn simple_range(expr: &Expr, params: &[Value]) -> Option<(String, Bound<Value>, Bound<Value>)> {
    let ctx = EvalCtx::no_row();
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let (col, val, flipped) = match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(id), other) if !matches!(other, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
                    let mut idx = 0usize;
                    (id.value.clone(), expr::eval(other, &ctx, params, &mut idx), false)
                }
                (other, Expr::Identifier(id)) if !matches!(other, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
                    let mut idx = 0usize;
                    (id.value.clone(), expr::eval(other, &ctx, params, &mut idx), true)
                }
                _ => return None,
            };
            // Normalize so `op` always reads left-to-right as `col OP val`.
            let op = if flipped {
                match op {
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    other => other.clone(),
                }
            } else {
                op.clone()
            };
            match op {
                BinaryOperator::Gt => Some((col, Bound::Excluded(val), Bound::Unbounded)),
                BinaryOperator::GtEq => Some((col, Bound::Included(val), Bound::Unbounded)),
                BinaryOperator::Lt => Some((col, Bound::Unbounded, Bound::Excluded(val))),
                BinaryOperator::LtEq => Some((col, Bound::Unbounded, Bound::Included(val))),
                _ => None,
            }
        }
        Expr::Between { expr: inner, negated: false, low, high } => {
            if let Expr::Identifier(id) = inner.as_ref() {
                let mut idx_l = 0usize;
                let mut idx_h = 0usize;
                let lo = expr::eval(low, &ctx, params, &mut idx_l);
                let hi = expr::eval(high, &ctx, params, &mut idx_h);
                Some((id.value.clone(), Bound::Included(lo), Bound::Included(hi)))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Recognizes a WHERE clause that is *exactly* `col = <literal-or-param>`
/// (or the reversed `<literal-or-param> = col`), with nothing else
/// (no surrounding AND/OR). Anything more complex than that falls back to a
/// full table scan - this only exists to speed up the very common
/// point-lookup case, not to be a general query planner.
fn simple_equality(expr: &Expr, params: &[Value]) -> Option<(String, Value)> {
    if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = expr {
        let ctx = EvalCtx::no_row();
        match (left.as_ref(), right.as_ref()) {
            (Expr::Identifier(id), other) if !matches!(other, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
                let mut idx = 0usize;
                Some((id.value.clone(), expr::eval(other, &ctx, params, &mut idx)))
            }
            (other, Expr::Identifier(id)) if !matches!(other, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) => {
                let mut idx = 0usize;
                Some((id.value.clone(), expr::eval(other, &ctx, params, &mut idx)))
            }
            _ => None,
        }
    } else {
        None
    }
}

fn is_grouped(group_by: &GroupByExpr) -> bool {
    match group_by {
        GroupByExpr::Expressions(exprs) => !exprs.is_empty(),
        GroupByExpr::All => false,
    }
}

/// Apply the SELECT list to each row: pick out plain columns, evaluate
/// scalar expressions/function calls (e.g. json_extract, arithmetic, CASE),
/// and honor aliases. A bare `*` (or no projection) passes rows through
/// unchanged; `*` mixed with other items expands in place, in projection order.
fn apply_projection(rows: Vec<Row>, projection: &[SelectItem], params: &[Value]) -> Vec<Row> {
    if projection.is_empty() || projection.iter().all(|item| matches!(item, SelectItem::Wildcard(_))) {
        return rows;
    }

    rows.into_iter().map(|row| {
        let mut new_row = Row::new();
        let ctx = EvalCtx::with_row(&row);
        for item in projection {
            match item {
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                    for (k, v) in row.iter() {
                        new_row.insert(k.clone(), v.clone());
                    }
                }
                SelectItem::UnnamedExpr(e) => {
                    let name = match e {
                        Expr::Identifier(id) => id.value.clone(),
                        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()).unwrap_or_else(|| e.to_string()),
                        _ => e.to_string(),
                    };
                    let mut idx = 0usize;
                    new_row.insert(name, expr::eval(e, &ctx, params, &mut idx));
                }
                SelectItem::ExprWithAlias { expr: e, alias } => {
                    let mut idx = 0usize;
                    new_row.insert(alias.to_string(), expr::eval(e, &ctx, params, &mut idx));
                }
            }
        }
        new_row
    }).collect()
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

fn qualified_name(table_or_alias: &str, col: &str) -> String {
    format!("{}.{}", table_or_alias, col)
}

/// Merges `row`'s columns into `combined`, tagged under `table_or_alias`
/// (e.g. `authors.name`) so `alias.column` / `table.column` references
/// always resolve unambiguously - and also under the bare column name, but
/// only if no earlier table in this join already claimed that name.
///
/// That second part matters: two joined tables very commonly share a column
/// name (`id` above all), and the previous, simpler scheme of only
/// qualifying the *second* table's alias (and only when an alias was even
/// given) let the second table's `id` silently overwrite the first table's
/// `id` under the same bare key whenever neither side had a real alias -
/// which then broke the *join condition itself* (`a.id = b.id` would
/// resolve `a.id` as a fallback bare lookup that had already been clobbered
/// by `b.id`), not just the projected output.
fn insert_row_qualified(combined: &mut Row, table_or_alias: &str, row: &Row) {
    for (k, v) in row.iter() {
        combined.insert(qualified_name(table_or_alias, k), v.clone());
        combined.entry(k.clone()).or_insert_with(|| v.clone());
    }
}

fn table_ref_name(table: &str, alias: &Option<String>) -> String {
    alias.clone().unwrap_or_else(|| table.to_string())
}

/// A version of `row2`'s columns (as inserted by `insert_row_qualified`
/// under `table_or_alias`) with every value NULLed out, used to pad an
/// unmatched left-side row in a LEFT/FULL OUTER JOIN. Prefers the table's
/// schema (so the shape is right even if `store2` happens to be empty);
/// falls back to a sample row otherwise.
fn null_padded(table_or_alias: &str, schema2: &Option<std::sync::Arc<TableSchema>>, sample: Option<&Row>) -> Row {
    let mut r = Row::new();
    if let Some(s) = schema2 {
        for col in s.columns.keys() {
            r.insert(qualified_name(table_or_alias, col), Value::Null);
            r.entry(col.clone()).or_insert(Value::Null);
        }
    } else if let Some(sample) = sample {
        for k in sample.keys() {
            r.insert(qualified_name(table_or_alias, k), Value::Null);
            r.entry(k.clone()).or_insert(Value::Null);
        }
    }
    r
}

fn join_condition_matches(state: &KvsState, db_id: u32, combined: &Row, constraint: &JoinConstraint, params: &[Value]) -> bool {
    match constraint {
        JoinConstraint::On(expr) => {
            let mut idx = 0usize;
            evaluate_where_ctx(state, db_id, combined, expr, params, &mut idx)
        }
        JoinConstraint::Using(cols) => {
            cols.iter().all(|c| {
                // USING(col) matches when both sides' (unqualified and any
                // qualified copy) values for that column are equal.
                let name = &c.value;
                row_col_any(combined, name).is_some()
            })
        }
        JoinConstraint::Natural | JoinConstraint::None => true,
    }
}

fn row_col_any(row: &Row, name: &str) -> Option<Value> {
    row.get(name).cloned()
}

/// Recognizes a JOIN's `ON` condition as a plain equi-join between the two
/// tables involved in *this* join (`t1.col1 = t2.col2`, qualified with
/// either the table name or its alias, in either order) so the executor can
/// build a hash index on one side instead of a nested-loop comparing every
/// row of one side against every row of the other. Anything else (compound
/// conditions, unqualified columns, non-equality, expressions) returns
/// `None` and the caller falls back to the always-correct nested loop.
///
/// Only applied to the *first* join in a chain (see call site): from the
/// second join onward, `name1`'s accumulated rows already carry columns
/// from earlier tables too, and a qualified reference could belong to any
/// of them, not just the immediate left-hand table - the nested-loop path
/// (which resolves columns generically through `evaluate_where_ctx`)
/// already handles that correctly, so it's left alone rather than special-
/// cased further.
fn extract_equi_join(constraint: &JoinConstraint, name1: &str, name2: &str) -> Option<(String, String)> {
    let expr = match constraint {
        JoinConstraint::On(expr) => expr,
        _ => return None,
    };
    if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = expr {
        let side = |e: &Expr| -> Option<(u8, String)> {
            match e {
                Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    if parts[0].value == name1 {
                        Some((1, parts[1].value.clone()))
                    } else if parts[0].value == name2 {
                        Some((2, parts[1].value.clone()))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        };
        match (side(left), side(right)) {
            (Some((1, c1)), Some((2, c2))) => Some((c1, c2)),
            (Some((2, c2)), Some((1, c1))) => Some((c1, c2)),
            _ => None,
        }
    } else {
        None
    }
}

/// Groups `rows2` by the (raw, unqualified) value of `col2`, keyed by a
/// canonical string form of the `Value` so it can live in a plain
/// `HashMap` (`Value` itself only implements `PartialEq`, not `Hash`,
/// because of the `f64` variant).
fn hash_by_column<'a>(rows2: &'a [Row], col2: &str) -> HashMap<String, Vec<&'a Row>> {
    let mut map: HashMap<String, Vec<&Row>> = HashMap::new();
    for r in rows2 {
        if let Some(v) = r.get(col2) {
            map.entry(crate::index::value_to_key_string(v)).or_default().push(r);
        }
    }
    map
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

    let (table1, alias1) = parse_table_factor(&from[0].relation);
    let name1 = table_ref_name(&table1, &alias1);
    let store1 = state.get_table_store(db_id, &table1)
        .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table1)))?;
    let mut result: Vec<Row> = store1.iter().map(|e| {
        let mut row = Row::new();
        insert_row_qualified(&mut row, &name1, e.value());
        row
    }).collect();

    for (join_idx, join) in from[0].joins.iter().enumerate() {
        let (table2, alias2) = parse_table_factor(&join.relation);
        let name2 = table_ref_name(&table2, &alias2);
        let store2 = state.get_table_store(db_id, &table2)
            .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table2)))?;
        let schema2 = state.get_schema(db_id, &table2);
        let rows2: Vec<Row> = store2.iter().map(|e| e.value().clone()).collect();
        // Only safe on the first hop of the chain - see `extract_equi_join` doc comment.
        let equi = if join_idx == 0 {
            match &join.join_operator {
                JoinOperator::Inner(c) | JoinOperator::LeftOuter(c) => extract_equi_join(c, &name1, &name2),
                _ => None,
            }
        } else {
            None
        };

        let combine = |row1: &Row, row2: &Row| -> Row {
            let mut combined = row1.clone();
            insert_row_qualified(&mut combined, &name2, row2);
            combined
        };

        match &join.join_operator {
            JoinOperator::Inner(constraint) => {
                if let Some((col1, col2)) = &equi {
                    // Hash join: O(n + m) instead of O(n * m). Build the
                    // smaller/probe side once, then a single hashmap lookup
                    // per left-side row replaces scanning all of rows2.
                    let by_val = hash_by_column(&rows2, col2);
                    let q1 = qualified_name(&name1, col1);
                    let mut new_result = Vec::with_capacity(result.len());
                    for row1 in &result {
                        if let Some(v1) = row1.get(&q1) {
                            if let Some(matches) = by_val.get(&crate::index::value_to_key_string(v1)) {
                                for row2 in matches {
                                    new_result.push(combine(row1, row2));
                                }
                            }
                        }
                    }
                    result = new_result;
                    continue;
                }
                let mut new_result = Vec::new();
                for row1 in &result {
                    for row2 in &rows2 {
                        let combined = combine(row1, row2);
                        if join_condition_matches(state, db_id, &combined, constraint, params) {
                            new_result.push(combined);
                        }
                    }
                }
                result = new_result;
            }
            JoinOperator::LeftOuter(constraint) => {
                if let Some((col1, col2)) = &equi {
                    let by_val = hash_by_column(&rows2, col2);
                    let q1 = qualified_name(&name1, col1);
                    let mut new_result = Vec::with_capacity(result.len());
                    for row1 in &result {
                        let matches = row1.get(&q1).and_then(|v1| by_val.get(&crate::index::value_to_key_string(v1)));
                        match matches {
                            Some(matches) if !matches.is_empty() => {
                                for row2 in matches {
                                    new_result.push(combine(row1, row2));
                                }
                            }
                            _ => {
                                let mut combined = row1.clone();
                                for (k, v) in null_padded(&name2, &schema2, rows2.first()) {
                                    combined.insert(k, v);
                                }
                                new_result.push(combined);
                            }
                        }
                    }
                    result = new_result;
                    continue;
                }
                let mut new_result = Vec::new();
                for row1 in &result {
                    let mut matched = false;
                    for row2 in &rows2 {
                        let combined = combine(row1, row2);
                        if join_condition_matches(state, db_id, &combined, constraint, params) {
                            new_result.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut combined = row1.clone();
                        for (k, v) in null_padded(&name2, &schema2, rows2.first()) {
                            combined.insert(k, v);
                        }
                        new_result.push(combined);
                    }
                }
                result = new_result;
            }
            JoinOperator::RightOuter(constraint) => {
                let left_cols: Vec<String> = result.first().map(|r| r.keys().cloned().collect()).unwrap_or_default();
                let mut new_result = Vec::new();
                for row2 in &rows2 {
                    let mut matched = false;
                    for row1 in &result {
                        let combined = combine(row1, row2);
                        if join_condition_matches(state, db_id, &combined, constraint, params) {
                            new_result.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut combined = Row::new();
                        for k in &left_cols {
                            combined.insert(k.clone(), Value::Null);
                        }
                        insert_row_qualified(&mut combined, &name2, row2);
                        new_result.push(combined);
                    }
                }
                result = new_result;
            }
            JoinOperator::FullOuter(constraint) => {
                let left_cols: Vec<String> = result.first().map(|r| r.keys().cloned().collect()).unwrap_or_default();
                let mut matched2 = vec![false; rows2.len()];
                let mut new_result = Vec::new();
                for row1 in &result {
                    let mut matched1 = false;
                    for (j, row2) in rows2.iter().enumerate() {
                        let combined = combine(row1, row2);
                        if join_condition_matches(state, db_id, &combined, constraint, params) {
                            new_result.push(combined);
                            matched1 = true;
                            matched2[j] = true;
                        }
                    }
                    if !matched1 {
                        let mut combined = row1.clone();
                        for (k, v) in null_padded(&name2, &schema2, rows2.first()) {
                            combined.insert(k, v);
                        }
                        new_result.push(combined);
                    }
                }
                for (j, row2) in rows2.iter().enumerate() {
                    if !matched2[j] {
                        let mut combined = Row::new();
                        for k in &left_cols {
                            combined.insert(k.clone(), Value::Null);
                        }
                        insert_row_qualified(&mut combined, &name2, row2);
                        new_result.push(combined);
                    }
                }
                result = new_result;
            }
            JoinOperator::CrossJoin => {
                let mut new_result = Vec::new();
                for row1 in &result {
                    for row2 in &rows2 {
                        new_result.push(combine(row1, row2));
                    }
                }
                result = new_result;
            }
            other => {
                return Err(SkvsError::Unsupported(format!("JOIN type not supported: {:?}", other)));
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

    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<Row>> = HashMap::new();
    for row in rows {
        let ctx = EvalCtx::with_row(&row);
        let key = exprs.iter()
            .map(|e| {
                let mut idx = 0usize;
                format!("{:?}", expr::eval(e, &ctx, &[], &mut idx))
            })
            .collect::<Vec<_>>()
            .join("\u{1}");
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups.entry(key).or_default().push(row);
    }

    let mut result = Vec::new();
    for key in order {
        let group_rows = groups.remove(&key).unwrap_or_default();
        let mut row = Row::new();
        for item in projection {
            if let SelectItem::UnnamedExpr(e) = item {
                let val = evaluate_aggregate(e, &group_rows);
                let name = match e {
                    Expr::Identifier(id) => id.value.clone(),
                    _ => e.to_string(),
                };
                row.insert(name, val);
            } else if let SelectItem::ExprWithAlias { expr: e, alias } = item {
                let val = evaluate_aggregate(e, &group_rows);
                row.insert(alias.to_string(), val);
            }
        }
        result.push(row);
    }
    Ok(result)
}

fn evaluate_aggregate(e: &Expr, rows: &[Row]) -> Value {
    if let Expr::Function(func) = e {
        let func_name = func.name.to_string().to_lowercase();
        let is_distinct = func.distinct;
        let arg_ident = func.args.first().and_then(|arg| match arg {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(id))) => Some(id.value.clone()),
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                parts.last().map(|p| p.value.clone())
            }
            _ => None,
        });
        let is_star = matches!(
            func.args.first(),
            Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard))
        );
        match func_name.as_str() {
            "count" => {
                if is_star || func.args.is_empty() {
                    Value::Integer(rows.len() as i64)
                } else if let Some(col) = &arg_ident {
                    let mut vals: Vec<&Value> = rows.iter().filter_map(|r| r.get(col)).filter(|v| !matches!(v, Value::Null)).collect();
                    if is_distinct {
                        let mut seen = std::collections::HashSet::new();
                        vals.retain(|v| seen.insert(format!("{:?}", v)));
                    }
                    Value::Integer(vals.len() as i64)
                } else {
                    Value::Integer(rows.len() as i64)
                }
            }
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
    } else if let Expr::Identifier(id) = e {
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
        let asc = order.asc.unwrap_or(true);
        rows.sort_by(|a, b| {
            let ctx_a = EvalCtx::with_row(a);
            let ctx_b = EvalCtx::with_row(b);
            let mut idx_a = 0usize;
            let mut idx_b = 0usize;
            let va = expr::eval(&order.expr, &ctx_a, &[], &mut idx_a);
            let vb = expr::eval(&order.expr, &ctx_b, &[], &mut idx_b);
            let cmp = va.compare(&vb);
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
    if let Expr::Value(SqlValue::Number(s, _)) = expr {
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
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(id) => id.value.clone(),
                Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()).unwrap_or_else(|| expr.to_string()),
                _ => expr.to_string(),
            },
            SelectItem::ExprWithAlias { alias, .. } => alias.to_string(),
            SelectItem::QualifiedWildcard(..) => "*".to_string(),
            SelectItem::Wildcard(_) => "*".to_string(),
        })
        .collect()
}

/// WHERE evaluation. Recurses through AND/OR/NOT/parens itself (rather than
/// delegating those to the generic scalar evaluator) purely so short-circuit
/// evaluation and the `fts_match(...)` special case both work; every other
/// expression shape is handed to the shared `expr::eval` + `expr::truthy`.
pub fn evaluate_where_ctx(state: &KvsState, db_id: u32, row: &Row, expr_node: &Expr, params: &[Value], param_idx: &mut usize) -> bool {
    match expr_node {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            evaluate_where_ctx(state, db_id, row, left, params, param_idx)
                && evaluate_where_ctx(state, db_id, row, right, params, param_idx)
        }
        Expr::BinaryOp { left, op: BinaryOperator::Or, right } => {
            evaluate_where_ctx(state, db_id, row, left, params, param_idx)
                || evaluate_where_ctx(state, db_id, row, right, params, param_idx)
        }
        Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Not, expr: inner } => {
            !evaluate_where_ctx(state, db_id, row, inner, params, param_idx)
        }
        Expr::Nested(inner) => evaluate_where_ctx(state, db_id, row, inner, params, param_idx),
        Expr::Function(func) if func.name.to_string().to_lowercase() == "fts_match" && func.args.len() == 2 => {
            let table_name = match &func.args[0] {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(id))) => id.value.clone(),
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(SqlValue::SingleQuotedString(s)))) => s.clone(),
                _ => return false,
            };
            let query_text = match &func.args[1] {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(SqlValue::SingleQuotedString(s)))) => s.clone(),
                _ => return false,
            };
            let fts = match state.get_fts_table(db_id, &table_name) {
                Some(fts) => fts,
                None => return false,
            };
            let rowid = match row.get("_rowid_") {
                Some(Value::Integer(id)) => *id as RowId,
                _ => return false,
            };
            let handle = tokio::runtime::Handle::try_current();
            let results = match handle {
                Ok(h) => tokio::task::block_in_place(|| h.block_on(fts.search(&query_text))),
                Err(_) => {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    rt.block_on(fts.search(&query_text))
                }
            };
            results.contains(&rowid)
        }
        _ => {
            let ctx = EvalCtx::with_row(row);
            truthy(&expr::eval(expr_node, &ctx, params, param_idx))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::DatabaseConfig;
    use crate::sql::SqlEngine;
    use crate::state::KvsState;
    use crate::schema::Value;

    fn new_state() -> KvsState {
        KvsState::new(&[DatabaseConfig { id: 0, name: "default".into() }])
    }

    #[test]
    fn select_star_reports_real_columns_not_a_literal_asterisk() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (id INTEGER, name TEXT)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (id, name) VALUES (1, 'a')", &[], None).unwrap();
        let result = SqlEngine::execute(&state, 0, "SELECT * FROM t", &[], None).unwrap();
        assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn left_outer_join_pads_unmatched_rows_with_null() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE a (id INTEGER, val TEXT)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "CREATE TABLE b (a_id INTEGER, note TEXT)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO a (id, val) VALUES (1, 'x')", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO a (id, val) VALUES (2, 'y')", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO b (a_id, note) VALUES (1, 'has-note')", &[], None).unwrap();

        let result = SqlEngine::execute(
            &state, 0,
            "SELECT a.id, b.note FROM a LEFT JOIN b ON a.id = b.a_id",
            &[], None,
        ).unwrap();
        assert_eq!(result.rows.len(), 2, "every left-side row must appear exactly once");
        let unmatched = result.rows.iter().find(|r| r.get("id") == Some(&Value::Integer(2))).unwrap();
        assert_eq!(unmatched.get("note"), Some(&Value::Null));
    }

    #[test]
    fn inner_join_still_only_returns_matches() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE a (id INTEGER)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "CREATE TABLE b (a_id INTEGER)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO a (id) VALUES (1)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO a (id) VALUES (2)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO b (a_id) VALUES (1)", &[], None).unwrap();

        let result = SqlEngine::execute(&state, 0, "SELECT a.id FROM a JOIN b ON a.id = b.a_id", &[], None).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn range_query_uses_index_and_returns_correct_rows() {
        // Regression/coverage test for the new BTreeMap range index: an
        // indexed `>` predicate must return exactly the same rows a full
        // scan would, not just "whatever the index fast path happens to
        // fetch".
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (id INTEGER, score INTEGER)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "CREATE INDEX idx_score ON t (score)", &[], None).unwrap();
        for (id, score) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
            SqlEngine::execute(&state, 0, &format!("INSERT INTO t (id, score) VALUES ({}, {})", id, score), &[], None).unwrap();
        }
        let result = SqlEngine::execute(&state, 0, "SELECT id FROM t WHERE score > 20", &[], None).unwrap();
        let mut ids: Vec<i64> = result.rows.iter().filter_map(|r| match r.get("id") { Some(Value::Integer(i)) => Some(*i), _ => None }).collect();
        ids.sort();
        assert_eq!(ids, vec![3, 4]);

        let between = SqlEngine::execute(&state, 0, "SELECT id FROM t WHERE score BETWEEN 20 AND 30", &[], None).unwrap();
        let mut ids2: Vec<i64> = between.rows.iter().filter_map(|r| match r.get("id") { Some(Value::Integer(i)) => Some(*i), _ => None }).collect();
        ids2.sort();
        assert_eq!(ids2, vec![2, 3]);
    }

    #[test]
    fn create_index_on_table_with_existing_rows_covers_them_immediately() {
        // Regression test: CREATE INDEX used to only register the index in
        // the schema without populating it from rows already in the table,
        // so a lookup against it would incorrectly report zero matches
        // until the next INSERT/UPDATE touched each row.
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (id INTEGER, name TEXT)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (id, name) VALUES (1, 'pre-existing')", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "CREATE INDEX idx_name ON t (name)", &[], None).unwrap();

        let result = SqlEngine::execute(&state, 0, "SELECT id FROM t WHERE name = 'pre-existing'", &[], None).unwrap();
        assert_eq!(result.rows.len(), 1, "index must cover rows that existed before CREATE INDEX ran");
    }

    #[test]
    fn where_compares_real_column_against_integer_literal_correctly() {
        // Regression test for the Value::compare cross-type bug: comparing
        // an Integer literal against a Real column used to always report
        // "Equal", which made `WHERE price > 100` match nothing at all.
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (price REAL)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (price) VALUES (150.5)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (price) VALUES (50.5)", &[], None).unwrap();
        let result = SqlEngine::execute(&state, 0, "SELECT price FROM t WHERE price > 100", &[], None).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("price"), Some(&Value::Real(150.5)));
    }
}

#[cfg(test)]
mod join_collision_tests {
    use crate::config::DatabaseConfig;
    use crate::sql::SqlEngine;
    use crate::state::KvsState;
    use crate::schema::Value;

    fn new_state() -> KvsState {
        KvsState::new(&[DatabaseConfig { id: 0, name: "default".into() }])
    }

    #[test]
    fn join_condition_is_correct_even_when_both_tables_share_a_column_name() {
        // Regression test: previously, joining two unaliased tables that
        // both have an `id` column (extremely common) let the second
        // table's `id` silently overwrite the first table's `id` in the
        // merged row, corrupting the join condition itself - not just the
        // projected output - into effectively comparing the second table's
        // own columns to each other, independent of the first table.
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE authors (id INTEGER, name TEXT)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "CREATE TABLE books (id INTEGER, title TEXT, author_id INTEGER)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO authors (id, name) VALUES (1, 'Tolkien'), (2, 'Orwell')", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO books (id, title, author_id) VALUES (1, 'LOTR', 1), (2, '1984', 2), (3, 'Ghost', 99)", &[], None).unwrap();

        let result = SqlEngine::execute(
            &state, 0,
            "SELECT authors.name, books.title FROM authors JOIN books ON authors.id = books.author_id",
            &[], None,
        ).unwrap();
        assert_eq!(result.rows.len(), 2, "each author should match exactly its own book, not a cross product");
        let names: Vec<_> = result.rows.iter().filter_map(|r| r.get("name").cloned()).collect();
        assert!(names.contains(&Value::Text("Tolkien".into())));
        assert!(names.contains(&Value::Text("Orwell".into())));
    }
}
