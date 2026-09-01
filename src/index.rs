use crate::error::SkvsError;
use crate::schema::*;
use crate::state::{IndexStore, KvsState};
use std::cmp::Ordering;
use std::ops::Bound;

/// The kind of change that happened to a row, used to keep secondary
/// indexes in sync.
#[derive(Debug, Clone)]
pub enum IndexOp {
    Insert,
    Delete,
    Update { old_row: Row },
}

/// Total-order wrapper around `Value` so a secondary index can live in a
/// `BTreeMap` (point *and* range lookups, both O(log n) instead of an O(n)
/// scan) rather than the old scheme of bincode-blobbing a `Vec<RowId>` per
/// distinct value into the raw key/value store. That old scheme meant every
/// single index update - even flipping one row - deserialized and
/// re-serialized the *entire* id list for that value, and a range predicate
/// (`WHERE x > 10`, `BETWEEN`) couldn't use the index at all since the blob
/// keys weren't ordered in any lookup-friendly way.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexKey(pub Value);
impl Eq for IndexKey {}
impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.compare(&other.0)
    }
}

/// Canonical, type-distinguishing string form of a `Value`, used as a hash
/// key by the SELECT-side hash-join fast path (see `sql/select.rs`).
pub(crate) fn value_to_key_string(v: &Value) -> String {
    match v {
        Value::Null => "n:".to_string(),
        Value::Integer(i) => format!("i:{}", i),
        Value::Real(f) => format!("f:{}", f),
        Value::Text(s) => format!("t:{}", s),
        Value::Blob(b) => format!("b:{}", hex::encode(b)),
    }
}

fn index_value(idx: &IndexDef, row: &Row) -> Option<Value> {
    // Only single-column indexes are supported for now.
    let col = idx.columns.first()?;
    row.get(col).cloned()
}

fn index_store(state: &KvsState, db_id: u32, table: &str, index_name: &str) -> IndexStore {
    state.get_or_create_index_store(db_id, table, index_name)
}

fn add_to_index(state: &KvsState, db_id: u32, table: &str, idx: &IndexDef, row: &Row, rowid: RowId) {
    if let Some(val) = index_value(idx, row) {
        let store = index_store(state, db_id, table, &idx.name);
        let mut map = store.write().unwrap();
        let ids = map.entry(IndexKey(val)).or_insert_with(Vec::new);
        if !ids.contains(&rowid) {
            ids.push(rowid);
        }
    }
}

fn remove_from_index(state: &KvsState, db_id: u32, table: &str, idx: &IndexDef, row: &Row, rowid: RowId) {
    if let Some(val) = index_value(idx, row) {
        let store = index_store(state, db_id, table, &idx.name);
        let mut map = store.write().unwrap();
        let key = IndexKey(val);
        let mut now_empty = false;
        if let Some(ids) = map.get_mut(&key) {
            ids.retain(|r| *r != rowid);
            now_empty = ids.is_empty();
        }
        if now_empty {
            map.remove(&key);
        }
    }
}

/// Removes an index's whole in-memory BTreeMap (used by `DROP INDEX`).
pub fn drop_index_store(state: &KvsState, db_id: u32, table: &str, index_name: &str) {
    state.drop_index_store(db_id, table, index_name);
}

/// Rebuilds an index from the table's current live rows. Needed in two
/// cases: (1) `CREATE INDEX` on a table that already has data - without
/// this, the index silently covered zero of the existing rows and every
/// lookup against it would incorrectly report "no match"; (2) restoring a
/// snapshot after a restart, since only row content is persisted, not the
/// in-memory index structure itself.
pub fn rebuild_index(state: &KvsState, db_id: u32, table: &str, idx: &IndexDef) {
    if let Some(rows) = state.get_table_store(db_id, table) {
        let store = index_store(state, db_id, table, &idx.name);
        let mut map = store.write().unwrap();
        map.clear();
        for entry in rows.iter() {
            let rowid = *entry.key();
            if let Some(val) = index_value(idx, entry.value()) {
                map.entry(IndexKey(val)).or_insert_with(Vec::new).push(rowid);
            }
        }
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

/// Equality point lookup (`WHERE col = value`) against a named index.
/// O(log n) via the underlying `BTreeMap`.
pub fn lookup_index(state: &KvsState, db_id: u32, table: &str, index_name: &str, value: &Value) -> Vec<RowId> {
    let store = index_store(state, db_id, table, index_name);
    let map = store.read().unwrap();
    map.get(&IndexKey(value.clone())).cloned().unwrap_or_default()
}

/// Range lookup against a named index: `lower`/`upper` follow
/// `std::ops::Bound` semantics, so this one function covers `<`, `<=`, `>`,
/// `>=`, and `BETWEEN` via a single `BTreeMap::range()` call instead of a
/// full table scan.
pub fn range_lookup(
    state: &KvsState,
    db_id: u32,
    table: &str,
    index_name: &str,
    lower: Bound<Value>,
    upper: Bound<Value>,
) -> Vec<RowId> {
    let store = index_store(state, db_id, table, index_name);
    let map = store.read().unwrap();
    let lower = match lower {
        Bound::Included(v) => Bound::Included(IndexKey(v)),
        Bound::Excluded(v) => Bound::Excluded(IndexKey(v)),
        Bound::Unbounded => Bound::Unbounded,
    };
    let upper = match upper {
        Bound::Included(v) => Bound::Included(IndexKey(v)),
        Bound::Excluded(v) => Bound::Excluded(IndexKey(v)),
        Bound::Unbounded => Bound::Unbounded,
    };
    let mut out = Vec::new();
    for (_, ids) in map.range::<IndexKey, _>((lower, upper)) {
        out.extend(ids.iter().copied());
    }
    out
}

