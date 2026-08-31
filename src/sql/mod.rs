pub mod ddl;
pub mod dml;
pub mod select;

use crate::state::KvsState;
use crate::schema::*;
use crate::error::SkvsError;
use sqlparser::ast::{ObjectType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use serde::{Serialize, Deserialize};

pub struct SqlEngine;

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub affected_rows: Option<u64>,
    /// Set only by `BEGIN` (or the dedicated `begin_transaction` entry
    /// point): the new transaction's id, which the caller must pass back on
    /// every subsequent statement (including the eventual COMMIT/ROLLBACK)
    /// that should belong to this transaction.
    #[serde(default)]
    pub tx_id: Option<u32>,
}

impl QueryResult {
    pub fn empty() -> Self {
        QueryResult { columns: vec![], rows: vec![], affected_rows: None, tx_id: None }
    }
    pub fn affected(n: u64) -> Self {
        QueryResult { columns: vec![], rows: vec![], affected_rows: Some(n), tx_id: None }
    }
    pub fn rows(rows: Vec<Row>, columns: Vec<String>) -> Self {
        QueryResult { columns, rows, affected_rows: None, tx_id: None }
    }
    pub fn began(tx_id: u32) -> Self {
        QueryResult { columns: vec![], rows: vec![], affected_rows: None, tx_id: Some(tx_id) }
    }
}

impl SqlEngine {
    pub fn execute(
        state: &KvsState,
        db_id: u32,
        sql: &str,
        params: &[Value],
        tx_id: Option<u32>,
    ) -> Result<QueryResult, SkvsError> {
        // sqlparser 0.40 has no AST node for CREATE TRIGGER, so it's parsed
        // by hand straight off the SQL text before we ever reach the real parser.
        if sql.trim_start().to_uppercase().starts_with("CREATE TRIGGER") {
            return ddl::create_trigger(state, db_id, sql);
        }

        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;
        if statements.len() != 1 {
            return Err(SkvsError::Unsupported("Multiple statements not supported".into()));
        }
        let stmt = statements.into_iter().next().unwrap();

        // BEGIN / COMMIT / ROLLBACK are handled here rather than in
        // `execute_single_statement`, since they act on the transaction
        // manager itself rather than on any table.
        match &stmt {
            Statement::StartTransaction { .. } => {
                let new_tx = state.txns.begin(db_id);
                return Ok(QueryResult::began(new_tx));
            }
            Statement::Commit { .. } => {
                let id = tx_id.ok_or_else(|| {
                    SkvsError::Transaction("COMMIT with no active transaction id".into())
                })?;
                state.txns.commit(id)?;
                return Ok(QueryResult::empty());
            }
            Statement::Rollback { .. } => {
                let id = tx_id.ok_or_else(|| {
                    SkvsError::Transaction("ROLLBACK with no active transaction id".into())
                })?;
                state.txns.rollback(state, id)?;
                return Ok(QueryResult::empty());
            }
            _ => {}
        }

        // Give every write statement atomicity even when the caller didn't
        // wrap it in an explicit BEGIN/COMMIT: run it (and any trigger
        // cascade it fires, since triggers execute through this same
        // `tx_id`-threaded path) inside a short-lived internal transaction,
        // and roll that back if any part of it fails. Without this, e.g. a
        // multi-row INSERT that fails validation on its third row would
        // leave the first two permanently applied with no way to undo them.
        let needs_atomicity_net = tx_id.is_none()
            && matches!(stmt, Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. });

        if needs_atomicity_net {
            let shadow = state.txns.begin(db_id);
            match Self::execute_single_statement(state, db_id, stmt, params, Some(shadow)) {
                Ok(result) => {
                    let _ = state.txns.commit(shadow);
                    Ok(result)
                }
                Err(e) => {
                    let _ = state.txns.rollback(state, shadow);
                    Err(e)
                }
            }
        } else {
            Self::execute_single_statement(state, db_id, stmt, params, tx_id)
        }
    }

    pub fn execute_single_statement(
        state: &KvsState,
        db_id: u32,
        stmt: Statement,
        params: &[Value],
        tx_id: Option<u32>,
    ) -> Result<QueryResult, SkvsError> {
        match &stmt {
            Statement::Query(query) => select::execute_select(state, db_id, query, params, tx_id),
            Statement::Insert { .. } => dml::execute_insert(state, db_id, &stmt, params, tx_id),
            Statement::Update { .. } => dml::execute_update(state, db_id, &stmt, params, tx_id),
            Statement::Delete { .. } => dml::execute_delete(state, db_id, &stmt, params, tx_id),
            Statement::CreateTable { .. } => ddl::create_table(state, db_id, &stmt),
            Statement::AlterTable { .. } => ddl::alter_table(state, db_id, &stmt),
            Statement::CreateIndex { .. } => ddl::create_index(state, db_id, &stmt),
            Statement::CreateView { .. } => ddl::create_view(state, db_id, &stmt),
            Statement::CreateVirtualTable { .. } => ddl::create_virtual_table(state, db_id, &stmt),
            Statement::Drop { object_type, .. } => match object_type {
                ObjectType::Table => ddl::drop_table(state, db_id, &stmt),
                ObjectType::Index => ddl::drop_index(state, db_id, &stmt),
                ObjectType::View => ddl::drop_view(state, db_id, &stmt),
                _ => Err(SkvsError::Unsupported(format!("DROP {:?} not implemented", object_type))),
            },
            Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. } => {
                Err(SkvsError::Unsupported(
                    "BEGIN/COMMIT/ROLLBACK must go through SqlEngine::execute, not execute_single_statement".into(),
                ))
            }
            _ => Err(SkvsError::Unsupported(format!("Statement not implemented: {:?}", stmt))),
        }
    }
}

