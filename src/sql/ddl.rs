use sqlparser::ast::{
    AlterTableOperation, ColumnDef as SqlColDef, ColumnOption, ObjectType, ReferentialAction,
    Statement, TableConstraint,
};
use crate::state::KvsState;
use crate::schema::*;
use crate::error::SkvsError;
use crate::sql::QueryResult;
use crate::view;
use std::sync::Arc;
use dashmap::DashMap;
use indexmap::IndexMap;

pub fn create_table(
    state: &KvsState,
    db_id: u32,
    stmt: &Statement,
) -> Result<QueryResult, SkvsError> {
    let (name, sql_columns, constraints, if_not_exists) = match stmt {
        Statement::CreateTable { name, columns, constraints, if_not_exists, .. } => {
            (name, columns, constraints, *if_not_exists)
        }
        _ => return Err(SkvsError::Unsupported("Not a CREATE TABLE statement".into())),
    };

    let table_name = name.to_string();
    let schemas = state.schemas.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
    if schemas.contains_key(&table_name) {
        if if_not_exists {
            return Ok(QueryResult::empty());
        }
        return Err(SkvsError::Schema(format!("Table {} already exists", table_name)));
    }

    let mut columns = IndexMap::new();
    let mut primary_keys = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut unique_groups = Vec::new();
    let mut table_checks = Vec::new();

    for col in sql_columns {
        let col_def = parse_column_def(col);
        if col_def.primary_key {
            primary_keys.push(col_def.name.clone());
        }
        columns.insert(col_def.name.clone(), col_def);
    }

    for constraint in constraints {
        match constraint {
            // Table-level `[CONSTRAINT name] { PRIMARY KEY | UNIQUE } (col, ...)`.
            // Previously silently ignored - the very common
            // `CREATE TABLE t (id INTEGER, ..., PRIMARY KEY(id))` form (as
            // opposed to `id INTEGER PRIMARY KEY`) left the table with no
            // rowid_column and no uniqueness enforcement at all.
            TableConstraint::Unique { columns: cols, is_primary, .. } => {
                let col_names: Vec<String> = cols.iter().map(|c| c.value.clone()).collect();
                if *is_primary {
                    for c in &col_names {
                        if let Some(cd) = columns.get_mut(c) {
                            cd.primary_key = true;
                            cd.not_null = true;
                        }
                    }
                    if let Some(first) = col_names.first() {
                        primary_keys.push(first.clone());
                    }
                } else {
                    for c in &col_names {
                        if let Some(cd) = columns.get_mut(c) {
                            cd.unique = true;
                        }
                    }
                }
                unique_groups.push(UniqueGroup { columns: col_names, is_primary: *is_primary });
            }
            TableConstraint::ForeignKey {
                columns: fk_columns, foreign_table, referred_columns, on_delete, on_update, ..
            } => {
                let col_name = fk_columns.first().map(|c| c.to_string()).unwrap_or_default();
                let ref_table = foreign_table.to_string();
                let ref_col = referred_columns.first().map(|c| c.to_string()).unwrap_or_default();
                let fk = ForeignKeyDef {
                    column: col_name,
                    ref_table,
                    ref_column: ref_col,
                    on_delete: parse_fk_action(on_delete),
                    on_update: parse_fk_action(on_update),
                };
                foreign_keys.push(fk);
            }
            TableConstraint::Check { expr, .. } => {
                table_checks.push(expr.to_string());
            }
            _ => {}
        }
    }

    let schema = TableSchema {
        name: table_name.clone(),
        columns,
        rowid_column: primary_keys.first().cloned(),
        foreign_keys,
        indices: vec![],
        triggers: vec![],
        unique_groups,
        table_checks,
        fts5_content_column: None,
    };

    schemas.insert(table_name.clone(), Arc::new(schema));

    let dbs = state.dbs.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
    dbs.insert(table_name.clone(), Arc::new(DashMap::new()));

    let gens = state.rowid_generators.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
    gens.insert(table_name, 1);

    Ok(QueryResult::empty())
}

fn parse_column_def(col: &SqlColDef) -> ColumnDef {
    let name = col.name.to_string();
    let data_type = match &col.data_type {
        sqlparser::ast::DataType::Int(_) => DataType::Integer,
        sqlparser::ast::DataType::Integer(_) => DataType::Integer,
        sqlparser::ast::DataType::Float(_) => DataType::Real,
        sqlparser::ast::DataType::Double => DataType::Real,
        sqlparser::ast::DataType::Text => DataType::Text,
        sqlparser::ast::DataType::String(_) => DataType::Text,
        sqlparser::ast::DataType::Varchar(_) => DataType::Text,
        sqlparser::ast::DataType::Blob(_) => DataType::Blob,
        _ => DataType::Blob,
    };
    let mut primary_key = false;
    let mut not_null = false;
    let mut unique = false;
    let mut default = None;
    let mut auto_increment = false;
    let mut check_expr = None;

    for opt in &col.options {
        match &opt.option {
            ColumnOption::NotNull => not_null = true,
            ColumnOption::Unique { is_primary } => {
                unique = true;
                if *is_primary {
                    primary_key = true;
                    not_null = true;
                }
            }
            // `DEFAULT <expr>` previously always stored Value::Null instead
            // of the literal that was actually written, silently discarding
            // every `DEFAULT 0` / `DEFAULT 'x'` / `DEFAULT CURRENT_TIMESTAMP`
            // in the schema. Evaluate it as a constant expression (no row/
            // params available at DDL time, so only literal defaults
            // resolve to something other than NULL - that matches what a
            // real default actually is: a constant).
            ColumnOption::Default(expr) => {
                let mut idx = 0usize;
                let ctx = crate::expr::EvalCtx::no_row();
                default = Some(crate::expr::eval(expr, &ctx, &[], &mut idx));
            }
            ColumnOption::Check(expr) => check_expr = Some(expr.to_string()),
            // MySQL `AUTO_INCREMENT` / SQLite `AUTOINCREMENT` arrive as an
            // opaque token list rather than a dedicated AST variant.
            ColumnOption::DialectSpecific(tokens) => {
                let joined = tokens.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(" ").to_uppercase();
                if joined.contains("AUTOINCREMENT") || joined.contains("AUTO_INCREMENT") {
                    auto_increment = true;
                }
            }
            _ => {}
        }
    }

    ColumnDef {
        name,
        data_type,
        primary_key,
        auto_increment,
        not_null,
        default,
        unique,
        check_expr,
    }
}

fn parse_fk_action(action: &Option<ReferentialAction>) -> FkAction {
    match action {
        Some(ReferentialAction::Cascade) => FkAction::Cascade,
        Some(ReferentialAction::SetNull) => FkAction::SetNull,
        Some(ReferentialAction::Restrict) => FkAction::Restrict,
        Some(ReferentialAction::SetDefault) => FkAction::SetDefault,
        _ => FkAction::NoAction,
    }
}

pub fn drop_table(state: &KvsState, db_id: u32, stmt: &Statement) -> Result<QueryResult, SkvsError> {
    let (names, if_exists) = match stmt {
        Statement::Drop { object_type: ObjectType::Table, names, if_exists, .. } => (names, *if_exists),
        _ => return Err(SkvsError::Unsupported("Not a DROP TABLE statement".into())),
    };
    for name in names {
        let table = name.to_string();
        let schemas = state.schemas.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
        if schemas.remove(&table).is_none() && !if_exists {
            return Err(SkvsError::Schema(format!("Table {} not found", table)));
        }
        if let Some(dbs) = state.dbs.get(&db_id) {
            dbs.remove(&table);
        }
        if let Some(gens) = state.rowid_generators.get(&db_id) {
            gens.remove(&table);
        }
        // A dropped table shouldn't leave stale triggers or an fts5 index
        // registered against its name if it (or a same-named replacement) is
        // used again later.
        if let Some(triggers) = state.triggers.get(&db_id) {
            triggers.remove(&table);
        }
        if let Some(fts) = state.fts_tables.get(&db_id) {
            fts.remove(&table);
        }
        if let Some(views) = state.views.get(&db_id) {
            views.remove(&table);
        }
    }
    Ok(QueryResult::empty())
}

/// `ALTER TABLE`: supports `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN ... TO ...`,
/// and `RENAME TO <new_name>`. Multiple operations in one statement (as
/// sqlparser allows) are applied in order.
pub fn alter_table(state: &KvsState, db_id: u32, stmt: &Statement) -> Result<QueryResult, SkvsError> {
    let (name, if_exists, operations) = match stmt {
        Statement::AlterTable { name, if_exists, operations, .. } => (name, *if_exists, operations),
        _ => return Err(SkvsError::Unsupported("Not an ALTER TABLE statement".into())),
    };
    let mut table_name = name.to_string();

    let schemas = state.schemas.get(&db_id)
        .ok_or_else(|| SkvsError::Schema(format!("Database {} not found", db_id)))?;

    if !schemas.contains_key(&table_name) {
        if if_exists {
            return Ok(QueryResult::empty());
        }
        return Err(SkvsError::Schema(format!("Table {} not found", table_name)));
    }

    for op in operations {
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                let col_def = parse_column_def(column_def);
                let mut schema = (**schemas.get(&table_name).unwrap()).clone();
                if schema.columns.contains_key(&col_def.name) {
                    return Err(SkvsError::Schema(format!("Column {} already exists", col_def.name)));
                }
                // Backfill the new column onto every existing row so
                // `SELECT *` and constraint checks see a consistent shape
                // immediately, matching what SQLite does for ADD COLUMN.
                if let Some(store) = state.get_table_store(db_id, &table_name) {
                    let default = col_def.default.clone().unwrap_or(Value::Null);
                    for mut entry in store.iter_mut() {
                        entry.value_mut().insert(col_def.name.clone(), default.clone());
                    }
                }
                schema.columns.insert(col_def.name.clone(), col_def);
                schemas.insert(table_name.clone(), Arc::new(schema));
            }
            AlterTableOperation::DropColumn { column_name, if_exists: col_if_exists, .. } => {
                let mut schema = (**schemas.get(&table_name).unwrap()).clone();
                let col = column_name.value.clone();
                if !schema.columns.shift_remove(&col).is_some() && !*col_if_exists {
                    return Err(SkvsError::Schema(format!("Column {} not found", col)));
                }
                if schema.rowid_column.as_deref() == Some(col.as_str()) {
                    schema.rowid_column = None;
                }
                if let Some(store) = state.get_table_store(db_id, &table_name) {
                    for mut entry in store.iter_mut() {
                        entry.value_mut().shift_remove(&col);
                    }
                }
                schemas.insert(table_name.clone(), Arc::new(schema));
            }
            AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                let mut schema = (**schemas.get(&table_name).unwrap()).clone();
                let old = old_column_name.value.clone();
                let new = new_column_name.value.clone();
                let (idx, _, mut col_def) = schema.columns.shift_remove_full(&old)
                    .ok_or_else(|| SkvsError::Schema(format!("Column {} not found", old)))?
                    .into();
                col_def.name = new.clone();
                schema.columns.shift_insert(idx.min(schema.columns.len()), new.clone(), col_def);
                if schema.rowid_column.as_deref() == Some(old.as_str()) {
                    schema.rowid_column = Some(new.clone());
                }
                if let Some(store) = state.get_table_store(db_id, &table_name) {
                    for mut entry in store.iter_mut() {
                        if let Some(v) = entry.value_mut().shift_remove(&old) {
                            entry.value_mut().insert(new.clone(), v);
                        }
                    }
                }
                schemas.insert(table_name.clone(), Arc::new(schema));
            }
            AlterTableOperation::RenameTable { table_name: new_name } => {
                let new_table = new_name.to_string();
                if let Some((_, schema)) = schemas.remove(&table_name) {
                    schemas.insert(new_table.clone(), schema);
                }
                if let Some(dbs) = state.dbs.get(&db_id) {
                    if let Some((_, store)) = dbs.remove(&table_name) {
                        dbs.insert(new_table.clone(), store);
                    }
                }
                if let Some(gens) = state.rowid_generators.get(&db_id) {
                    if let Some((_, g)) = gens.remove(&table_name) {
                        gens.insert(new_table.clone(), g);
                    }
                }
                table_name = new_table;
            }
            _ => {
                return Err(SkvsError::Unsupported(format!("ALTER TABLE operation not supported: {}", op)));
            }
        }
    }

    Ok(QueryResult::empty())
}

pub fn create_index(state: &KvsState, db_id: u32, stmt: &Statement) -> Result<QueryResult, SkvsError> {
    let (name, table_name, columns, unique, if_not_exists) = match stmt {
        Statement::CreateIndex { name, table_name, columns, unique, if_not_exists, .. } => {
            (name, table_name, columns, *unique, *if_not_exists)
        }
        _ => return Err(SkvsError::Unsupported("Not a CREATE INDEX statement".into())),
    };

    let table = table_name.to_string();
    let schemas = state.schemas.get(&db_id)
        .ok_or_else(|| SkvsError::Schema(format!("Database {} not found", db_id)))?;
    let schema_arc = schemas.get(&table)
        .ok_or_else(|| SkvsError::Schema(format!("Table {} not found", table)))?
        .clone();

    let index_name = name.as_ref().map(|n| n.to_string())
        .unwrap_or_else(|| format!("idx_{}_{}", table, columns.len()));

    if schema_arc.indices.iter().any(|i| i.name == index_name) {
        if if_not_exists {
            return Ok(QueryResult::empty());
        }
        return Err(SkvsError::Schema(format!("Index {} already exists", index_name)));
    }

    let col_names: Vec<String> = columns.iter().map(|c| c.expr.to_string()).collect();
    let mut new_schema = (*schema_arc).clone();
    new_schema.indices.push(IndexDef {
        name: index_name,
        columns: col_names,
        unique,
        partial_where: None,
        is_expression: false,
    });
    schemas.insert(table, Arc::new(new_schema));

    Ok(QueryResult::empty())
}

pub fn drop_index(state: &KvsState, db_id: u32, stmt: &Statement) -> Result<QueryResult, SkvsError> {
    let (names, if_exists) = match stmt {
        Statement::Drop { object_type: ObjectType::Index, names, if_exists, .. } => (names, *if_exists),
        _ => return Err(SkvsError::Unsupported("Not a DROP INDEX statement".into())),
    };

    let schemas = state.schemas.get(&db_id)
        .ok_or_else(|| SkvsError::Schema(format!("Database {} not found", db_id)))?;

    for name in names {
        let index_name = name.to_string();
        let mut found = false;
        for mut entry in schemas.iter_mut() {
            if entry.indices.iter().any(|i| i.name == index_name) {
                let mut new_schema = (**entry.value()).clone();
                new_schema.indices.retain(|i| i.name != index_name);
                *entry.value_mut() = Arc::new(new_schema);
                found = true;
                break;
            }
        }
        if !found && !if_exists {
            return Err(SkvsError::Schema(format!("Index {} not found", index_name)));
        }
    }

    Ok(QueryResult::empty())
}

pub fn create_view(state: &KvsState, db_id: u32, stmt: &Statement) -> Result<QueryResult, SkvsError> {
    let (name, query) = match stmt {
        Statement::CreateView { name, query, .. } => (name.to_string(), query.to_string()),
        _ => return Err(SkvsError::Unsupported("Not a CREATE VIEW statement".into())),
    };
    view::create_view(state, db_id, &name, &query)?;
    Ok(QueryResult::empty())
}

/// Handle `CREATE VIRTUAL TABLE name USING fts5(col1, col2, ...)`.
///
/// We only support the fts5 module. The virtual table is backed by a plain
/// table (so ordinary INSERT/SELECT/UPDATE/DELETE work against it, just like
/// SQLite lets you read/write an fts5 table directly), plus a registered
/// `FtsVirtualTable` full-text index over its first column that `fts_match()`
/// can query from a WHERE clause.
pub fn create_virtual_table(state: &KvsState, db_id: u32, stmt: &Statement) -> Result<QueryResult, SkvsError> {
    let (name, if_not_exists, module_name, module_args) = match stmt {
        Statement::CreateVirtualTable { name, if_not_exists, module_name, module_args, .. } => {
            (name.to_string(), *if_not_exists, module_name.to_string().to_lowercase(), module_args)
        }
        _ => return Err(SkvsError::Unsupported("Not a CREATE VIRTUAL TABLE statement".into())),
    };

    if module_name != "fts5" {
        return Err(SkvsError::Unsupported(format!("Virtual table module '{}' not supported (only fts5)", module_name)));
    }

    let schemas = state.schemas.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
    if schemas.contains_key(&name) {
        if if_not_exists {
            return Ok(QueryResult::empty());
        }
        return Err(SkvsError::Schema(format!("Table {} already exists", name)));
    }

    let col_names: Vec<String> = module_args.iter().map(|a| a.value.clone()).collect();
    let content_column = col_names.first().cloned().unwrap_or_else(|| "content".to_string());

    let mut columns = IndexMap::new();
    for col_name in &col_names {
        columns.insert(col_name.clone(), ColumnDef {
            name: col_name.clone(),
            data_type: DataType::Text,
            primary_key: false,
            auto_increment: false,
            not_null: false,
            default: None,
            unique: false,
            check_expr: None,
        });
    }

    let schema = TableSchema {
        name: name.clone(),
        columns,
        rowid_column: None,
        foreign_keys: vec![],
        indices: vec![],
        triggers: vec![],
        unique_groups: vec![],
        table_checks: vec![],
        fts5_content_column: Some(content_column.clone()),
    };
    schemas.insert(name.clone(), Arc::new(schema));

    let dbs = state.dbs.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
    dbs.insert(name.clone(), Arc::new(DashMap::new()));

    let gens = state.rowid_generators.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
    gens.insert(name.clone(), 1);

    state.register_fts_table(db_id, &name, Arc::new(crate::fts::FtsVirtualTable::new(&name, &content_column)))?;

    Ok(QueryResult::empty())
}

/// Handle `CREATE TRIGGER name {BEFORE|AFTER|INSTEAD OF} {INSERT|UPDATE|DELETE}
/// ON table [FOR EACH ROW] [WHEN condition] BEGIN ... END`.
///
/// sqlparser 0.40 has no AST node for CREATE TRIGGER, so this is parsed by
/// hand directly off the raw SQL text rather than through `Parser::parse_sql`.
pub fn create_trigger(state: &KvsState, db_id: u32, sql: &str) -> Result<QueryResult, SkvsError> {
    let err = || SkvsError::SqlParse("Malformed CREATE TRIGGER statement".into());

    let sql = sql.trim();
    let upper = sql.to_uppercase();
    let rest = upper.strip_prefix("CREATE TRIGGER").ok_or_else(err)?;
    let rest_start = sql.len() - rest.len();
    let rest_raw = &sql[rest_start..];

    let mut words = rest_raw.split_whitespace();
    let name = words.next().ok_or_else(err)?.to_string();

    let timing_word = words.next().ok_or_else(err)?.to_uppercase();
    let timing = match timing_word.as_str() {
        "BEFORE" => TriggerTiming::Before,
        "AFTER" => TriggerTiming::After,
        "INSTEAD" => {
            words.next(); // consume "OF"
            TriggerTiming::InsteadOf
        }
        _ => return Err(err()),
    };

    let event_word = words.next().ok_or_else(err)?.to_uppercase();
    let event = match event_word.as_str() {
        "INSERT" => TriggerEvent::Insert,
        "UPDATE" => TriggerEvent::Update,
        "DELETE" => TriggerEvent::Delete,
        _ => return Err(err()),
    };

    let on_word = words.next().ok_or_else(err)?.to_uppercase();
    if on_word != "ON" {
        return Err(err());
    }
    let table = words.next().ok_or_else(err)?.trim_matches(|c: char| c == ',' || c == ';').to_string();

    let remaining_upper = upper[upper.find(" ON ").map(|i| i + 4).unwrap_or(0)..].to_string();
    let for_each_row = remaining_upper.contains("FOR EACH ROW");

    // WHEN <condition> (optional), up to BEGIN
    let condition = if let Some(when_pos) = upper.find(" WHEN ") {
        let begin_pos = upper[when_pos..].find(" BEGIN ").map(|p| when_pos + p);
        begin_pos.map(|bp| sql[when_pos + 6..bp].trim().to_string())
    } else {
        None
    };

    let begin_pos = sql.to_uppercase().find("BEGIN").ok_or_else(err)?;
    let end_pos = sql.to_uppercase().rfind("END").ok_or_else(err)?;
    if end_pos <= begin_pos {
        return Err(err());
    }
    let body = sql[begin_pos + 5..end_pos].trim().to_string();

    let trigger_def = TriggerDef {
        name,
        timing,
        event,
        table: table.clone(),
        for_each_row,
        condition,
        body,
    };

    state.add_trigger(db_id, &table, Arc::new(trigger_def))?;
    Ok(QueryResult::empty())
}

pub fn drop_view(state: &KvsState, db_id: u32, stmt: &Statement) -> Result<QueryResult, SkvsError> {
    let (names, if_exists) = match stmt {
        Statement::Drop { object_type: ObjectType::View, names, if_exists, .. } => (names, *if_exists),
        _ => return Err(SkvsError::Unsupported("Not a DROP VIEW statement".into())),
    };
    for name in names {
        match view::drop_view(state, db_id, &name.to_string()) {
            Ok(()) => {}
            Err(_) if if_exists => {}
            Err(e) => return Err(e),
        }
    }
    Ok(QueryResult::empty())
}
