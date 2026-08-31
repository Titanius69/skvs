use crate::state::KvsState;
use crate::schema::*;
use crate::sql::SqlEngine;
use crate::error::SkvsError;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Execute triggers for a given event on a table
pub fn fire_triggers(
    state: &KvsState,
    db_id: u32,
    table: &str,
    event: TriggerEvent,
    timing: TriggerTiming,
    old_row: Option<&Row>,
    new_row: Option<&Row>,
    tx_id: Option<u32>,
) -> Result<(), SkvsError> {
    // Get triggers for this table
    let triggers = state.get_triggers(db_id, table);
    
    for trigger_def in triggers {
        if trigger_def.event == event && trigger_def.timing == timing {
            // Check condition if any
            if let Some(cond) = &trigger_def.condition {
                // Build a row containing both OLD and NEW if available
                let mut ctx_row = Row::new();
                if let Some(old) = old_row {
                    for (k, v) in old.iter() {
                        ctx_row.insert(format!("OLD.{}", k), v.clone());
                    }
                }
                if let Some(new) = new_row {
                    for (k, v) in new.iter() {
                        ctx_row.insert(format!("NEW.{}", k), v.clone());
                    }
                }
                // Evaluate condition using SQL engine's WHERE evaluator
                let mut parser = Parser::new(&GenericDialect {})
                    .try_with_sql(cond)
                    .map_err(|e| SkvsError::SqlParse(e.to_string()))?;
                let cond_expr = parser.parse_expr()
                    .map_err(|e| SkvsError::SqlParse(e.to_string()))?;
                let mut param_idx = 0usize;
                let result = crate::sql::select::evaluate_where_ctx(state, db_id, &ctx_row, &cond_expr, &[], &mut param_idx);
                if !result {
                    continue; // Condition failed, skip trigger
                }
            }

            // Execute trigger body (SQL statements)
            let body_sql = substitute_trigger_variables(&trigger_def.body, old_row, new_row);
            
            let statements = Parser::parse_sql(&GenericDialect {}, &body_sql)
                .map_err(|e| SkvsError::SqlParse(e.to_string()))?;
            
            for stmt in statements {
                // Execute each statement using the SQL engine
                // We need to pass the transaction ID if any
                // Use a new transaction context if none provided
                let _ = SqlEngine::execute_single_statement(state, db_id, stmt, &[], tx_id)?;
            }
        }
    }
    Ok(())
}

/// Substitute OLD.column and NEW.column references in trigger body
fn substitute_trigger_variables(body: &str, old_row: Option<&Row>, new_row: Option<&Row>) -> String {
    let mut result = body.to_string();
    if let Some(old) = old_row {
        for (k, v) in old.iter() {
            let pattern = format!("OLD.{}", k);
            let replacement = value_to_sql_literal(v);
            result = result.replace(&pattern, &replacement);
        }
    }
    if let Some(new) = new_row {
        for (k, v) in new.iter() {
            let pattern = format!("NEW.{}", k);
            let replacement = value_to_sql_literal(v);
            result = result.replace(&pattern, &replacement);
        }
    }
    result
}

/// Convert a Value to SQL literal string for substitution
fn value_to_sql_literal(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => format!("'{}'", s.replace("'", "''")),
        Value::Blob(b) => format!("X'{}'", hex::encode(b)),
    }
}