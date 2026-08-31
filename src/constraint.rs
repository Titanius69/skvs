use crate::state::KvsState;
use crate::schema::*;
use crate::error::SkvsError;
use crate::expr::{eval, truthy, EvalCtx};

/// Validates a candidate row (about to be inserted, or the post-UPDATE
/// version of an existing row) against every constraint declared on
/// `schema`: NOT NULL, column- and table-level UNIQUE/PRIMARY KEY, FOREIGN
/// KEY, and CHECK. `rowid` is the row's own id (already-generated for an
/// INSERT, the existing id for an UPDATE) so uniqueness scans can exclude
/// the row from colliding with itself; `old_row` is `Some` only for UPDATE
/// (kept for callers/future use - the exclusion itself is done via `rowid`).
pub fn validate_constraints(
    state: &KvsState,
    db_id: u32,
    schema: &TableSchema,
    row: &Row,
    rowid: RowId,
    _old_row: Option<&Row>,
) -> Result<(), SkvsError> {
    // NOT NULL
    for (col_name, col_def) in &schema.columns {
        if col_def.not_null {
            match row.get(col_name) {
                Some(Value::Null) | None => {
                    return Err(SkvsError::ConstraintViolation(format!("{} cannot be NULL", col_name)));
                }
                _ => {}
            }
        }
    }

    // Column-level UNIQUE / PRIMARY KEY
    for (col_name, col_def) in &schema.columns {
        if col_def.unique || col_def.primary_key {
            if let Some(val) = row.get(col_name) {
                if !matches!(val, Value::Null)
                    && value_exists_elsewhere(state, db_id, schema, std::slice::from_ref(col_name), std::slice::from_ref(val), rowid)
                {
                    let kind = if col_def.primary_key { "PRIMARY KEY" } else { "UNIQUE" };
                    return Err(SkvsError::ConstraintViolation(format!(
                        "{} constraint failed: {}.{}", kind, schema.name, col_name
                    )));
                }
            }
        }
    }

    // Table-level UNIQUE(...) / PRIMARY KEY(...) groups (composite keys).
    for group in &schema.unique_groups {
        let vals: Vec<Value> = group.columns.iter().map(|c| row.get(c).cloned().unwrap_or(Value::Null)).collect();
        if group.is_primary && vals.iter().any(|v| matches!(v, Value::Null)) {
            return Err(SkvsError::ConstraintViolation(format!(
                "PRIMARY KEY constraint failed: {}({})", schema.name, group.columns.join(", ")
            )));
        }
        if vals.iter().any(|v| matches!(v, Value::Null)) {
            // SQL UNIQUE treats NULLs as distinct from each other; skip the check.
            continue;
        }
        if value_exists_elsewhere(state, db_id, schema, &group.columns, &vals, rowid) {
            let kind = if group.is_primary { "PRIMARY KEY" } else { "UNIQUE" };
            return Err(SkvsError::ConstraintViolation(format!(
                "{} constraint failed: {}({})", kind, schema.name, group.columns.join(", ")
            )));
        }
    }

    // FOREIGN KEY: the referenced row must exist (when the local column isn't NULL).
    for fk in &schema.foreign_keys {
        if fk.column.is_empty() {
            continue;
        }
        if let Some(val) = row.get(&fk.column) {
            if matches!(val, Value::Null) {
                continue;
            }
            let ref_store = match state.get_table_store(db_id, &fk.ref_table) {
                Some(s) => s,
                None => continue, // referenced table doesn't exist (yet) - nothing to enforce against
            };
            let found = ref_store.iter().any(|entry| entry.value().get(&fk.ref_column) == Some(val));
            if !found {
                return Err(SkvsError::ConstraintViolation(format!(
                    "FOREIGN KEY constraint failed: {}.{} -> {}.{}",
                    schema.name, fk.column, fk.ref_table, fk.ref_column
                )));
            }
        }
    }

    // CHECK: column-level (`check_expr` on a ColumnDef) and table-level (`table_checks`).
    for (col_name, col_def) in &schema.columns {
        if let Some(check_sql) = &col_def.check_expr {
            if !eval_check(row, check_sql) {
                return Err(SkvsError::ConstraintViolation(format!(
                    "CHECK constraint failed: {}.{} ({})", schema.name, col_name, check_sql
                )));
            }
        }
    }
    for check_sql in &schema.table_checks {
        if !eval_check(row, check_sql) {
            return Err(SkvsError::ConstraintViolation(format!(
                "CHECK constraint failed: {} ({})", schema.name, check_sql
            )));
        }
    }

    Ok(())
}

/// True if some OTHER row (not `exclude_rowid`) in the table already has
/// this exact combination of values in `columns`. Used for both simple
/// single-column UNIQUE and composite (table-level) UNIQUE/PRIMARY KEY.
///
/// This does a full scan of the table's rows by default. For small-to-medium
/// tables (the expected scale here) that's fine; if `columns` is a single
/// column with a matching secondary index, that index is used to narrow the
/// scan instead of touching every row.
fn value_exists_elsewhere(
    state: &KvsState,
    db_id: u32,
    schema: &TableSchema,
    columns: &[String],
    values: &[Value],
    exclude_rowid: RowId,
) -> bool {
    let store = match state.get_table_store(db_id, &schema.name) {
        Some(s) => s,
        None => return false,
    };

    if columns.len() == 1 {
        if let Some(idx) = schema.indices.iter().find(|i| i.columns.len() == 1 && i.columns[0] == columns[0]) {
            let ids = crate::index::lookup_index(state, db_id, &schema.name, &idx.name, &values[0]);
            return ids.iter().any(|id| *id != exclude_rowid);
        }
    }

    let found = store.iter().any(|entry| {
        *entry.key() != exclude_rowid
            && columns.iter().zip(values.iter()).all(|(c, v)| entry.value().get(c) == Some(v))
    });
    found
}

fn eval_check(row: &Row, check_sql: &str) -> bool {
    // `check_expr`/`table_checks` are stored as the expression's rendered
    // SQL text (from sqlparser's `Display`), so re-parse it here as a
    // stand-alone expression. Re-parsing on every row is a bit wasteful for
    // very hot tables, but keeps `TableSchema` (and its bincode snapshot
    // format) simple - it only ever holds plain data, no parsed ASTs.
    let sql = format!("SELECT {}", check_sql);
    let stmts = match sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &sql) {
        Ok(s) => s,
        Err(_) => return true, // can't parse it back: don't block writes on an internal bug
    };
    let expr = match stmts.first() {
        Some(sqlparser::ast::Statement::Query(q)) => match q.body.as_ref() {
            sqlparser::ast::SetExpr::Select(sel) => match sel.projection.first() {
                Some(sqlparser::ast::SelectItem::UnnamedExpr(e)) => e.clone(),
                _ => return true,
            },
            _ => return true,
        },
        _ => return true,
    };
    let ctx = EvalCtx::with_row(row);
    let mut idx = 0usize;
    truthy(&eval(&expr, &ctx, &[], &mut idx))
}

#[cfg(test)]
mod tests {
    use crate::config::DatabaseConfig;
    use crate::sql::SqlEngine;
    use crate::state::KvsState;

    fn new_state() -> KvsState {
        KvsState::new(&[DatabaseConfig { id: 0, name: "default".into() }])
    }

    #[test]
    fn table_level_primary_key_is_enforced() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (id INTEGER, name TEXT, PRIMARY KEY(id))", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (id, name) VALUES (1, 'a')", &[], None).unwrap();
        let err = SqlEngine::execute(&state, 0, "INSERT INTO t (id, name) VALUES (1, 'b')", &[], None);
        assert!(err.is_err(), "duplicate primary key should be rejected");
    }

    #[test]
    fn column_default_is_actually_stored() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (id INTEGER, status TEXT DEFAULT 'pending')", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (id) VALUES (1)", &[], None).unwrap();
        let rows = SqlEngine::execute(&state, 0, "SELECT status FROM t", &[], None).unwrap();
        assert_eq!(rows.rows[0].get("status"), Some(&crate::schema::Value::Text("pending".into())));
    }

    #[test]
    fn foreign_key_requires_existing_row() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE parent (id INTEGER PRIMARY KEY)", &[], None).unwrap();
        SqlEngine::execute(
            &state, 0,
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))",
            &[], None,
        ).unwrap();
        let err = SqlEngine::execute(&state, 0, "INSERT INTO child (id, parent_id) VALUES (1, 99)", &[], None);
        assert!(err.is_err(), "FK to a non-existent parent row should be rejected");

        SqlEngine::execute(&state, 0, "INSERT INTO parent (id) VALUES (99)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO child (id, parent_id) VALUES (1, 99)", &[], None).unwrap();
    }

    #[test]
    fn check_constraint_rejects_bad_values() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (age INTEGER CHECK (age >= 0))", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (age) VALUES (5)", &[], None).unwrap();
        let err = SqlEngine::execute(&state, 0, "INSERT INTO t (age) VALUES (-1)", &[], None);
        assert!(err.is_err(), "negative age should fail the CHECK constraint");
    }

    #[test]
    fn update_can_exclude_itself_from_unique_check() {
        let state = new_state();
        SqlEngine::execute(&state, 0, "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (id, email) VALUES (1, 'a@x.com')", &[], None).unwrap();
        // Updating a row to the *same* value it already has must not trip
        // the UNIQUE check against itself.
        let result = SqlEngine::execute(&state, 0, "UPDATE t SET email = 'a@x.com' WHERE id = 1", &[], None).unwrap();
        assert_eq!(result.affected_rows, Some(1));
    }
}
