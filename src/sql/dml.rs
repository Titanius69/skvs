use sqlparser::ast::{Assignment, SetExpr, Statement};
use crate::state::KvsState;
use crate::schema::*;
use crate::constraint::validate_constraints;
use crate::index::{update_indexes, IndexOp};
use crate::trigger::fire_triggers;
use crate::error::SkvsError;
use crate::sql::QueryResult;
use crate::sql::select::evaluate_where_ctx;
use crate::expr::{count_placeholders, eval, EvalCtx};
use crate::transaction::UndoOp;

fn journal(state: &KvsState, tx_id: Option<u32>, op: UndoOp) {
    if let Some(id) = tx_id {
        state.txns.record(id, op);
    }
}

pub fn execute_insert(
    state: &KvsState,
    db_id: u32,
    stmt: &Statement,
    params: &[Value],
    tx_id: Option<u32>,
) -> Result<QueryResult, SkvsError> {
    if let Statement::Insert { table_name, columns, source, .. } = stmt {
        let table = table_name.to_string();
        let schema = state.get_schema(db_id, &table)
            .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table)))?;
        let store = state.get_table_store(db_id, &table)
            .ok_or_else(|| SkvsError::Schema(format!("Store for {} not found", table)))?;

        let col_names: Vec<String> = if columns.is_empty() {
            schema.columns.keys().cloned().collect::<Vec<_>>()
        } else {
            columns.iter().map(|c| c.value.clone()).collect()
        };

        // Build the list of rows to insert, either from a `VALUES (...), (...), ...`
        // list (every tuple, not just the first - a single-row-only INSERT here used
        // to silently drop every row after the first) or from `INSERT INTO t SELECT ...`.
        let mut candidate_rows: Vec<Row> = Vec::new();
        if let Some(source) = source {
            match source.body.as_ref() {
                SetExpr::Values(values) => {
                    let ctx = EvalCtx::no_row();
                    for value_row in &values.rows {
                        let mut param_idx = 0usize;
                        let mut row = Row::new();
                        for (i, col_name) in col_names.iter().enumerate() {
                            if i < value_row.len() {
                                let val = eval(&value_row[i], &ctx, params, &mut param_idx);
                                row.insert(col_name.clone(), val);
                            }
                        }
                        candidate_rows.push(row);
                    }
                }
                SetExpr::Select(_) => {
                    let inner = crate::sql::select::execute_select(state, db_id, source, params, tx_id)?;
                    for src_row in inner.rows {
                        let mut row = Row::new();
                        for (col_name, val) in col_names.iter().zip(src_row.values()) {
                            row.insert(col_name.clone(), val.clone());
                        }
                        candidate_rows.push(row);
                    }
                }
                _ => return Err(SkvsError::Unsupported("Unsupported INSERT source".into())),
            }
        }
        if candidate_rows.is_empty() {
            candidate_rows.push(Row::new());
        }

        let mut affected = 0u64;
        for mut row in candidate_rows {
            let rowid = state.get_next_rowid(db_id, &table);

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

            validate_constraints(state, db_id, &schema, &row, rowid, None)?;

            fire_triggers(state, db_id, &table, TriggerEvent::Insert, TriggerTiming::Before, None, Some(&row), tx_id)?;

            store.insert(rowid, row.clone());
            journal(state, tx_id, UndoOp::Insert { table: table.clone(), rowid });

            update_indexes(state, db_id, &table, &row, rowid, IndexOp::Insert)?;
            sync_fts_index(state, db_id, &table, rowid, Some(&row), false);

            fire_triggers(state, db_id, &table, TriggerEvent::Insert, TriggerTiming::After, None, Some(&row), tx_id)?;

            affected += 1;
        }

        Ok(QueryResult::affected(affected))
    } else {
        Err(SkvsError::Unsupported("Not an INSERT statement".into()))
    }
}

pub fn execute_update(
    state: &KvsState,
    db_id: u32,
    stmt: &Statement,
    params: &[Value],
    tx_id: Option<u32>,
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

        for entry in store.iter() {
            let rowid = *entry.key();
            let row = entry.value();
            let mut where_param_idx = set_param_count;
            let matches = match selection {
                Some(where_expr) => evaluate_where_ctx(state, db_id, row, where_expr, params, &mut where_param_idx),
                None => true,
            };
            if matches {
                rows_to_update.push((rowid, row.clone()));
            }
        }

        for (rowid, old_row) in rows_to_update {
            let mut new_row = old_row.clone();
            let mut param_idx = 0usize;
            // SET expressions see the row as it was *before* this statement's
            // changes (standard SQL semantics), so `SET a = a + 1, b = a` uses
            // the original `a`, not one another's freshly-written values -
            // hence evaluating every assignment against `old_row`, not the
            // in-progress `new_row`.
            let ctx = EvalCtx::with_row(&old_row);
            for assignment in assignments {
                let Assignment { id, value } = assignment;
                let col_name = id
                    .last()
                    .map(|ident| ident.value.clone())
                    .unwrap_or_default();
                let new_val = eval(value, &ctx, params, &mut param_idx);
                new_row.insert(col_name, new_val);
            }

            validate_constraints(state, db_id, &schema, &new_row, rowid, Some(&old_row))?;

            fire_triggers(state, db_id, &table_name, TriggerEvent::Update, TriggerTiming::Before, Some(&old_row), Some(&new_row), tx_id)?;

            store.insert(rowid, new_row.clone());
            journal(state, tx_id, UndoOp::Update { table: table_name.clone(), rowid, old_row: old_row.clone() });
            affected += 1;

            update_indexes(state, db_id, &table_name, &new_row, rowid, IndexOp::Update { old_row: old_row.clone() })?;
            sync_fts_index_change(state, db_id, &table_name, rowid, Some(&old_row), Some(&new_row));

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
    tx_id: Option<u32>,
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
                Some(where_expr) => evaluate_where_ctx(state, db_id, row, where_expr, params, &mut where_param_idx),
                None => true,
            };
            if matches {
                rows_to_delete.push((rowid, row.clone()));
            }
        }

        for (rowid, row) in rows_to_delete {
            fire_triggers(state, db_id, &table_name, TriggerEvent::Delete, TriggerTiming::Before, Some(&row), None, tx_id)?;

            store.remove(&rowid);
            journal(state, tx_id, UndoOp::Delete { table: table_name.clone(), rowid, row: row.clone() });
            affected += 1;
            update_indexes(state, db_id, &table_name, &row, rowid, IndexOp::Delete)?;
            sync_fts_index(state, db_id, &table_name, rowid, Some(&row), true);

            fire_triggers(state, db_id, &table_name, TriggerEvent::Delete, TriggerTiming::After, Some(&row), None, tx_id)?;
        }

        Ok(QueryResult::affected(affected))
    } else {
        Err(SkvsError::Unsupported("Not a DELETE statement".into()))
    }
}

/// If `table` has a registered fts5 virtual table, keep its inverted index in
/// sync. `row` holds the row's contents (new contents for insert/update, the
/// just-deleted contents for a delete); `is_delete` selects which.
pub fn sync_fts_index(state: &KvsState, db_id: u32, table: &str, rowid: RowId, row: Option<&Row>, is_delete: bool) {
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

/// Moves a row's fts5 index entry from `old_row`'s content to `new_row`'s.
/// Used by UPDATE, and by rolling back an UPDATE (with the arguments
/// swapped).
///
/// Previously UPDATE only ever called the insert half of this (re-indexing
/// the new content) and never removed the old content's tokens first. That
/// left every earlier version of an updated row's text permanently
/// searchable - a query would keep matching content that had since been
/// edited away - and inflated the FTS table's internal document counter by
/// one on every single update to the same row, corrupting its (unused so
/// far, but present) ranking statistics.
pub fn sync_fts_index_change(state: &KvsState, db_id: u32, table: &str, rowid: RowId, old_row: Option<&Row>, new_row: Option<&Row>) {
    if old_row.is_some() {
        sync_fts_index(state, db_id, table, rowid, old_row, true);
    }
    if new_row.is_some() {
        sync_fts_index(state, db_id, table, rowid, new_row, false);
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

        let result = SqlEngine::execute(
            &state, 0,
            "UPDATE t SET updated_at = ?, inventory = ? WHERE uuid = ? AND updated_at <= ?",
            &[
                Value::Integer(200),
                Value::Text("new".into()),
                Value::Text("abc-123".into()),
                Value::Integer(100),
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
    fn multi_row_insert_inserts_every_row() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (a INTEGER, b INTEGER)", &[], None).unwrap();
        let result = SqlEngine::execute(
            &state, 0,
            "INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)",
            &[], None,
        ).unwrap();
        assert_eq!(result.affected_rows, Some(3));
        let rows = SqlEngine::execute(&state, 0, "SELECT a, b FROM t ORDER BY a", &[], None).unwrap();
        assert_eq!(rows.rows.len(), 3);
        assert_eq!(rows.rows[2].get("a"), Some(&Value::Integer(5)));
    }

    #[test]
    fn update_set_can_reference_other_columns() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (a INTEGER, b INTEGER)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (a, b) VALUES (10, 0)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "UPDATE t SET b = a + 1", &[], None).unwrap();
        let rows = SqlEngine::execute(&state, 0, "SELECT b FROM t", &[], None).unwrap();
        assert_eq!(rows.rows[0].get("b"), Some(&Value::Integer(11)));
    }

    #[test]
    fn transaction_rollback_undoes_insert_and_update() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (a INTEGER)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (a) VALUES (1)", &[], None).unwrap();

        let tx = state.txns.begin(0);
        SqlEngine::execute_single_statement(
            &state, 0,
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, "INSERT INTO t (a) VALUES (2)").unwrap().remove(0),
            &[], Some(tx),
        ).unwrap();
        SqlEngine::execute_single_statement(
            &state, 0,
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, "UPDATE t SET a = 99 WHERE a = 1").unwrap().remove(0),
            &[], Some(tx),
        ).unwrap();

        let mid = SqlEngine::execute(&state, 0, "SELECT a FROM t ORDER BY a", &[], None).unwrap();
        assert_eq!(mid.rows.len(), 2);

        state.txns.rollback(&state, tx).unwrap();

        let after = SqlEngine::execute(&state, 0, "SELECT a FROM t ORDER BY a", &[], None).unwrap();
        assert_eq!(after.rows.len(), 1);
        assert_eq!(after.rows[0].get("a"), Some(&Value::Integer(1)));
    }

    #[test]
    fn autocommit_insert_is_atomic_on_constraint_failure() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (a INTEGER UNIQUE)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (a) VALUES (1)", &[], None).unwrap();

        // Second value (1) collides with the row already there - the whole
        // statement (including the first, otherwise-valid, value 2) should
        // be rolled back rather than partially applied.
        let err = SqlEngine::execute(&state, 0, "INSERT INTO t (a) VALUES (2), (1)", &[], None);
        assert!(err.is_err());

        let rows = SqlEngine::execute(&state, 0, "SELECT a FROM t", &[], None).unwrap();
        assert_eq!(rows.rows.len(), 1, "partial insert must have been rolled back");
    }
}
