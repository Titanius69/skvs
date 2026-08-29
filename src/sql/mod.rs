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
}

impl QueryResult {
    pub fn empty() -> Self {
        QueryResult { columns: vec![], rows: vec![], affected_rows: None }
    }
    pub fn affected(n: u64) -> Self {
        QueryResult { columns: vec![], rows: vec![], affected_rows: Some(n) }
    }
    pub fn rows(rows: Vec<Row>, columns: Vec<String>) -> Self {
        QueryResult { columns, rows, affected_rows: None }
    }
}

impl SqlEngine {
    pub fn execute(
        state: &KvsState,
        db_id: u32,
        sql: &str,
        params: &[Value],
        tx_id: Option<u64>,
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
        Self::execute_single_statement(state, db_id, stmt, params, tx_id)
    }

    pub fn execute_single_statement(
        state: &KvsState,
        db_id: u32,
        stmt: Statement,
        params: &[Value],
        tx_id: Option<u64>,
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
            _ => Err(SkvsError::Unsupported(format!("Statement not implemented: {:?}", stmt))),
        }
    }
}
