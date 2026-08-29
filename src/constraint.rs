use crate::state::KvsState;
use crate::schema::*;
use crate::error::KvsError;

pub fn validate_constraints(
    state: &KvsState,
    db_id: u32,
    schema: &TableSchema,
    row: &Row,
    old_row: Option<&Row>,
) -> Result<(), KvsError> {
    for (col_name, col_def) in &schema.columns {
        if col_def.not_null {
            if let Some(val) = row.get(col_name) {
                if matches!(val, Value::Null) {
                    return Err(KvsError::ConstraintViolation(format!("{} cannot be NULL", col_name)));
                }
            } else {
                return Err(KvsError::ConstraintViolation(format!("{} missing", col_name)));
            }
        }
    }
    Ok(())
}