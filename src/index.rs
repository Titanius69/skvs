use crate::error::SkvsError;
use crate::schema::*;
use crate::state::KvsState;

/// The kind of change that happened to a row, used to keep secondary
/// indexes in sync.
#[derive(Debug, Clone)]
pub enum IndexOp {
    Insert,
    Delete,
    Update { old_row: Row },
}

const INDEX_TABLE: &str = "__indexes__";

fn value_to_key_string(v: &Value) -> String {
    match v {
        Value::Null => "n:".to_string(),
        Value::Integer(i) => format!("i:{}", i),
        Value::Real(f) => format!("f:{}", f),
        Value::Text(s) => format!("t:{}", s),
        Value::Blob(b) => format!("b:{}", hex::encode(b)),
    }
}

fn index_key(table: &str, index_name: &str, value: &Value) -> Vec<u8> {
    format!("{}:{}:{}", table, index_name, value_to_key_string(value)).into_bytes()
}

fn index_value(idx: &IndexDef, row: &Row) -> Option<Value> {
    // Only single-column indexes are supported for now.
    let col = idx.columns.first()?;
    row.get(col).cloned()
}

fn read_ids(state: &KvsState, db_id: u32, key: &[u8]) -> Vec<RowId> {
    state
        .get_raw(db_id, INDEX_TABLE, key)
        .and_then(|bytes| bincode::deserialize::<Vec<RowId>>(&bytes).ok())
        .unwrap_or_default()
}

fn write_ids(state: &KvsState, db_id: u32, key: Vec<u8>, ids: &[RowId]) {
    if ids.is_empty() {
        state.remove_raw(db_id, INDEX_TABLE, &key);
        return;
    }
    if let Ok(bytes) = bincode::serialize(ids) {
        state.put_raw(db_id, INDEX_TABLE, key, bytes);
    }
}

fn add_to_index(state: &KvsState, db_id: u32, table: &str, idx: &IndexDef, row: &Row, rowid: RowId) {
    if let Some(val) = index_value(idx, row) {
        let key = index_key(table, &idx.name, &val);
        let mut ids = read_ids(state, db_id, &key);
        if !ids.contains(&rowid) {
            ids.push(rowid);
        }
        write_ids(state, db_id, key, &ids);
    }
}

fn remove_from_index(state: &KvsState, db_id: u32, table: &str, idx: &IndexDef, row: &Row, rowid: RowId) {
    if let Some(val) = index_value(idx, row) {
        let key = index_key(table, &idx.name, &val);
        let mut ids = read_ids(state, db_id, &key);
        ids.retain(|r| *r != rowid);
        write_ids(state, db_id, key, &ids);
    }
}

/// Keep all secondary indexes defined on `table` in sync with a row change.
pub fn update_indexes(
    state: &KvsState,
    db_id: u32,
    table: &str,
    row: &Row,
    rowid: RowId,
    op: IndexOp,
) -> Result<(), SkvsError> {
    let schema = match state.get_schema(db_id, table) {
        Some(s) => s,
        None => return Ok(()),
    };
    if schema.indices.is_empty() {
        return Ok(());
    }

    match op {
        IndexOp::Insert => {
            for idx in &schema.indices {
                add_to_index(state, db_id, table, idx, row, rowid);
            }
        }
        IndexOp::Delete => {
            for idx in &schema.indices {
                remove_from_index(state, db_id, table, idx, row, rowid);
            }
        }
        IndexOp::Update { old_row } => {
            for idx in &schema.indices {
                remove_from_index(state, db_id, table, idx, &old_row, rowid);
                add_to_index(state, db_id, table, idx, row, rowid);
            }
        }
    }
    Ok(())
}

/// Look up row ids for a given value using a named index. Useful for future
/// query-planning work; not yet wired into the SQL executor.
pub fn lookup_index(state: &KvsState, db_id: u32, table: &str, index_name: &str, value: &Value) -> Vec<RowId> {
    let key = index_key(table, index_name, value);
    read_ids(state, db_id, &key)
}
