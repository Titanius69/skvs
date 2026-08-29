use sqlparser::ast::{Assignment, Expr, SetExpr, Statement, Value as SqlValue};
use crate::state::KvsState;
use crate::schema::*;
use crate::constraint::validate_constraints;
use crate::index::{update_indexes, IndexOp};
use crate::trigger::fire_triggers;
use crate::error::SkvsError;
use crate::sql::QueryResult;
use crate::sql::select::evaluate_where;

pub fn execute_insert(
    state: &KvsState,
    db_id: u32,
    stmt: &Statement,
    params: &[Value],
    tx_id: Option<u64>,
) -> Result<QueryResult, SkvsError> {
    if let Statement::Insert { table_name, columns, source, .. } = stmt {
        let table = table_name.to_string();
        let schema = state.get_schema(db_id, &table)
            .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table)))?;
        let store = state.get_table_store(db_id, &table)
            .ok_or_else(|| SkvsError::Schema(format!("Store for {} not found", table)))?;
        let rowid = state.get_next_rowid(db_id, &table);

        let mut row = Row::new();
        let col_names: Vec<String> = if columns.is_empty() {
            schema.columns.keys().cloned().collect::<Vec<_>>()
        } else {
            columns.iter().map(|c| c.value.clone()).collect()
        };

        let mut param_idx = 0usize;
        if let Some(source) = source {
            if let SetExpr::Values(values) = source.body.as_ref() {
                if let Some(value_row) = values.rows.first() {
                    for (i, col_name) in col_names.iter().enumerate() {
                        if i < value_row.len() {
                            let expr = &value_row[i];
                            let val = evaluate_expr(expr, params, &mut param_idx)?;
                            row.insert(col_name.clone(), val);
                        }
                    }
                }
            }
        }

        // Apply column defaults for anything not supplied. An omitted
        // INTEGER PRIMARY KEY auto-generates from the row counter (SQLite's
        // "rowid alias" behavior), matching the assumption trigger bodies and
        // callers make when they don't specify it explicitly.
        for (col_name, col_def) in &schema.columns {
            if !row.contains_key(col_name) {
                if col_def.primary_key && col_def.data_type == DataType::Integer {
                    row.insert(col_name.clone(), Value::Integer(rowid as i64));
                } else if let Some(default) = &col_def.default {
                    row.insert(col_name.clone(), default.clone());
                } else {
                    row.insert(col_name.clone(), Value::Null);
                }
            }
        }

        // Validate constraints
        validate_constraints(state, db_id, &schema, &row, None)?;

        // BEFORE INSERT triggers
        fire_triggers(state, db_id, &table, TriggerEvent::Insert, TriggerTiming::Before, None, Some(&row), tx_id)?;

        // Insert into store
        store.insert(rowid, row.clone());

        // Update indexes
        update_indexes(state, db_id, &table, &row, rowid, IndexOp::Insert)?;

        // Keep any fts5 virtual table's full-text index in sync
        sync_fts_index(state, db_id, &table, rowid, Some(&row), false);

        // AFTER INSERT triggers
        fire_triggers(state, db_id, &table, TriggerEvent::Insert, TriggerTiming::After, None, Some(&row), tx_id)?;

        Ok(QueryResult::affected(1))
    } else {
        Err(SkvsError::Unsupported("Not an INSERT statement".into()))
    }
}

pub fn execute_update(
    state: &KvsState,
    db_id: u32,
    stmt: &Statement,
    params: &[Value],
    tx_id: Option<u64>,
) -> Result<QueryResult, SkvsError> {
    if let Statement::Update { table, assignments, selection, .. } = stmt {
        let table_name = match &table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
            other => other.to_string(),
        };
        let store = state.get_table_store(db_id, &table_name)
            .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table_name)))?;
        let schema = state.get_schema(db_id, &table_name)
            .ok_or_else(|| SkvsError::Schema(format!("Schema for {} not found", table_name)))?;

        let mut affected = 0u64;
        let mut rows_to_update = Vec::new();

        // `params` is one flat list of `?` values in left-to-right textual order:
        // first every placeholder in the SET assignments, then every placeholder in
        // the WHERE clause. The WHERE clause must therefore start reading `params`
        // *after* skipping however many placeholders the assignments consume -
        // starting it at 0 (as if WHERE had its own private params array) makes it
        // silently read values meant for SET, so a WHERE like `uuid = ?` ends up
        // comparing the uuid column against whatever the first SET value was. That
        // reliably never matches, and every caller doing a compare-and-swap style
        // `UPDATE ... SET x = ? WHERE id = ? AND version = ?` would see 0 affected
        // rows on every attempt, indistinguishable from constant write contention.
        let set_param_count: usize = assignments.iter()
            .map(|assignment| count_placeholders(&assignment.value))
            .sum();

        // Collect rows
        for entry in store.iter() {
            let rowid = *entry.key();
            let row = entry.value();
            let mut where_param_idx = set_param_count;
            let matches = match selection {
                Some(where_expr) => evaluate_where(row, where_expr, params, &mut where_param_idx),
                None => true,
            };
            if matches {
                rows_to_update.push((rowid, row.clone()));
            }
        }

        // Apply updates
        for (rowid, old_row) in rows_to_update {
            let mut new_row = old_row.clone();
            let mut param_idx = 0usize;
            for assignment in assignments {
                let Assignment { id, value } = assignment;
                let col_name = id
                    .last()
                    .map(|ident| ident.value.clone())
                    .unwrap_or_default();
                let new_val = evaluate_expr(value, params, &mut param_idx)?;
                new_row.insert(col_name, new_val);
            }

            // Validate constraints
            validate_constraints(state, db_id, &schema, &new_row, Some(&old_row))?;

            // BEFORE UPDATE triggers
            fire_triggers(state, db_id, &table_name, TriggerEvent::Update, TriggerTiming::Before, Some(&old_row), Some(&new_row), tx_id)?;

            // Update store
            store.insert(rowid, new_row.clone());
            affected += 1;

            // Update indexes
            update_indexes(state, db_id, &table_name, &new_row, rowid, IndexOp::Update { old_row: old_row.clone() })?;

            // Keep any fts5 virtual table's full-text index in sync
            sync_fts_index(state, db_id, &table_name, rowid, Some(&new_row), false);

            // AFTER UPDATE triggers
            fire_triggers(state, db_id, &table_name, TriggerEvent::Update, TriggerTiming::After, Some(&old_row), Some(&new_row), tx_id)?;
        }

        Ok(QueryResult::affected(affected))
    } else {
        Err(SkvsError::Unsupported("Not an UPDATE statement".into()))
    }
}

pub fn execute_delete(
    state: &KvsState,
    db_id: u32,
    stmt: &Statement,
    params: &[Value],
    tx_id: Option<u64>,
) -> Result<QueryResult, SkvsError> {
    if let Statement::Delete { tables, from, selection, .. } = stmt {
        let table_name = if let Some(t) = tables.first() {
            t.to_string()
        } else if let Some(f) = from.first() {
            match &f.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
                other => other.to_string(),
            }
        } else {
            return Err(SkvsError::Unsupported("DELETE without a target table".into()));
        };

        let store = state.get_table_store(db_id, &table_name)
            .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table_name)))?;

        let mut affected = 0u64;
        let mut rows_to_delete = Vec::new();

        for entry in store.iter() {
            let rowid = *entry.key();
            let row = entry.value();
            let mut where_param_idx = 0usize;
            let matches = match selection {
                Some(where_expr) => evaluate_where(row, where_expr, params, &mut where_param_idx),
                None => true,
            };
            if matches {
                rows_to_delete.push((rowid, row.clone()));
            }
        }

        for (rowid, row) in rows_to_delete {
            // BEFORE DELETE triggers
            fire_triggers(state, db_id, &table_name, TriggerEvent::Delete, TriggerTiming::Before, Some(&row), None, tx_id)?;

            store.remove(&rowid);
            affected += 1;
            update_indexes(state, db_id, &table_name, &row, rowid, IndexOp::Delete)?;

            // Keep any fts5 virtual table's full-text index in sync
            sync_fts_index(state, db_id, &table_name, rowid, Some(&row), true);

            // AFTER DELETE triggers
            fire_triggers(state, db_id, &table_name, TriggerEvent::Delete, TriggerTiming::After, Some(&row), None, tx_id)?;
        }

        Ok(QueryResult::affected(affected))
    } else {
        Err(SkvsError::Unsupported("Not a DELETE statement".into()))
    }
}

/// Counts how many `?` placeholders an expression consumes, without needing
/// any actual param values. Used to figure out where in the flat `params`
/// list a later, independent expression (e.g. a WHERE clause following a
/// SET list) should start reading from. Must walk exactly the same node
/// types `evaluate_expr` recurses into, or the two will disagree about how
/// many placeholders were consumed.
fn count_placeholders(expr: &Expr) -> usize {
    match expr {
        Expr::Value(SqlValue::Placeholder(_)) => 1,
        Expr::Value(_) => 0,
        Expr::UnaryOp { expr, .. } => count_placeholders(expr),
        Expr::Nested(inner) => count_placeholders(inner),
        Expr::BinaryOp { left, right, .. } => count_placeholders(left) + count_placeholders(right),
        _ => 0,
    }
}

/// Evaluate a scalar expression down to a stored `Value`, substituting `?`
/// placeholders from `params` in left-to-right order.
pub fn evaluate_expr(expr: &Expr, params: &[Value], param_idx: &mut usize) -> Result<Value, SkvsError> {
    match expr {
        Expr::Value(value) => match value {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() { Ok(Value::Integer(i)) }
                else if let Ok(f) = n.parse::<f64>() { Ok(Value::Real(f)) }
                else { Ok(Value::Null) }
            }
            SqlValue::SingleQuotedString(s) => Ok(Value::Text(s.clone())),
            SqlValue::Null => Ok(Value::Null),
            SqlValue::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            SqlValue::Placeholder(_) => {
                let val = params.get(*param_idx).cloned().unwrap_or(Value::Null);
                *param_idx += 1;
                Ok(val)
            }
            _ => Ok(Value::Null),
        },
        Expr::Identifier(_) => Ok(Value::Null),
        Expr::UnaryOp { op, expr } => {
            let inner = evaluate_expr(expr, params, param_idx)?;
            match (op, inner) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Integer(i)) => Ok(Value::Integer(-i)),
                (sqlparser::ast::UnaryOperator::Minus, Value::Real(f)) => Ok(Value::Real(-f)),
                (_, other) => Ok(other),
            }
        }
        _ => Ok(Value::Null),
    }
}

/// If `table` has a registered fts5 virtual table, keep its inverted index in
/// sync. `row` holds the row's contents (new contents for insert/update, the
/// just-deleted contents for a delete); `is_delete` selects which.
fn sync_fts_index(state: &KvsState, db_id: u32, table: &str, rowid: RowId, row: Option<&Row>, is_delete: bool) {
    let fts = match state.get_fts_table(db_id, table) {
        Some(fts) => fts,
        None => return,
    };
    let content = match row.and_then(|r| r.get(&fts.content_column)) {
        Some(Value::Text(s)) => s.clone(),
        _ => return,
    };

    let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> = if is_delete {
        Box::pin(async move { let _ = fts.delete(rowid, &content).await; })
    } else {
        Box::pin(async move { let _ = fts.insert(rowid, &content).await; })
    };

    match tokio::runtime::Handle::try_current() {
        Ok(h) => tokio::task::block_in_place(|| h.block_on(fut)),
        Err(_) => {
            let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
            rt.block_on(fut);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DatabaseConfig;
    use crate::state::KvsState;
    use crate::sql::SqlEngine;

    fn new_state() -> KvsState {
        KvsState::new(&[DatabaseConfig { id: 0, name: "default".into() }])
    }

    /// Reproduces the SkvsInventory CAS pattern: an UPDATE whose SET list has
    /// placeholders that come *before* the WHERE clause's own placeholders in
    /// the flat params list. Before the fix, the WHERE clause read `params[0]`
    /// (meant for the first SET value) instead of its own uuid/timestamp
    /// params, so this update would always affect 0 rows no matter what was
    /// actually stored.
    #[test]
    fn update_where_reads_its_own_params_not_the_sets() {
        let state = new_state();

        SqlEngine::execute(
            &state, 0,
            "CREATE TABLE t (uuid TEXT PRIMARY KEY, inventory TEXT, updated_at INTEGER)",
            &[], None,
        ).unwrap();

        SqlEngine::execute(
            &state, 0,
            "INSERT INTO t (uuid, inventory, updated_at) VALUES (?, ?, ?)",
            &[Value::Text("abc-123".into()), Value::Text("old".into()), Value::Integer(100)],
            None,
        ).unwrap();

        // SET updated_at=?, inventory=?  WHERE uuid=? AND updated_at<=?
        let result = SqlEngine::execute(
            &state, 0,
            "UPDATE t SET updated_at = ?, inventory = ? WHERE uuid = ? AND updated_at <= ?",
            &[
                Value::Integer(200),                 // SET updated_at
                Value::Text("new".into()),            // SET inventory
                Value::Text("abc-123".into()),        // WHERE uuid
                Value::Integer(100),                  // WHERE updated_at <=
            ],
            None,
        ).unwrap();

        assert_eq!(result.affected_rows, Some(1), "CAS update should match the existing row");

        let rows = SqlEngine::execute(
            &state, 0,
            "SELECT inventory, updated_at FROM t WHERE uuid = ?",
            &[Value::Text("abc-123".into())],
            None,
        ).unwrap();
        assert_eq!(rows.rows[0].get("inventory"), Some(&Value::Text("new".into())));
        assert_eq!(rows.rows[0].get("updated_at"), Some(&Value::Integer(200)));
    }

    #[test]
    fn count_placeholders_matches_evaluate_expr_consumption() {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "UPDATE t SET a = ?, b = ? WHERE id = ?").unwrap();
        if let Statement::Update { assignments, .. } = &stmts[0] {
            let total: usize = assignments.iter().map(|a| count_placeholders(&a.value)).sum();
            assert_eq!(total, 2);
        } else {
            panic!("expected UPDATE statement");
        }
    }
}
