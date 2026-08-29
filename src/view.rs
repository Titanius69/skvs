use crate::state::KvsState;
use crate::error::SkvsError;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Create a view
pub fn create_view(
    state: &KvsState,
    db_id: u32,
    view_name: &str,
    query: &str,
) -> Result<(), SkvsError> {
    // Parse the query to validate it's a SELECT
    let statements = Parser::parse_sql(&GenericDialect {}, query)?;
    if statements.len() != 1 {
        return Err(SkvsError::Unsupported("View definition must be a single SELECT".into()));
    }
    match &statements[0] {
        sqlparser::ast::Statement::Query(_) => {
            // Valid SELECT
        }
        _ => return Err(SkvsError::Unsupported("View must be a SELECT query".into())),
    }

    // Store the view definition
    state.add_view(db_id, view_name, query)?;
    Ok(())
}

/// Drop a view
pub fn drop_view(state: &KvsState, db_id: u32, view_name: &str) -> Result<(), SkvsError> {
    state.remove_view(db_id, view_name)
}

/// Resolve a view: if the table name is a view, return its definition
pub fn resolve_view(state: &KvsState, db_id: u32, name: &str) -> Option<String> {
    state.get_view_definition(db_id, name)
}