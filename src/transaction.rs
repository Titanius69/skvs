//! Transactions, implemented as an undo journal rather than MVCC/snapshots.
//!
//! Writes are applied to the live tables immediately (as they always were in
//! this engine); what a transaction adds is a log of how to *undo* each
//! change. `COMMIT` just discards that log (the writes already happened).
//! `ROLLBACK` replays it in reverse, restoring each touched row to what it
//! looked like before the transaction started.
//!
//! `SqlEngine::execute` also uses this to give every autocommit statement
//! (not just ones inside an explicit BEGIN/COMMIT) atomicity: it wraps the
//! statement - and any trigger cascade it fires, since triggers execute
//! through the same `tx_id`-threaded path - in a short-lived internal
//! transaction, so e.g. a multi-row INSERT that fails on row 3 doesn't leave
//! rows 1-2 applied with no way to undo them.

use crate::error::SkvsError;
use crate::index::{update_indexes, IndexOp};
use crate::schema::{Row, RowId};
use crate::state::KvsState;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

#[derive(Clone)]
pub enum UndoOp {
    /// This rowid was newly inserted; undo = delete it.
    Insert { table: String, rowid: RowId },
    /// This row was deleted; undo = put it back exactly as it was.
    Delete { table: String, rowid: RowId, row: Row },
    /// This row was changed in place; undo = restore the pre-update version.
    Update { table: String, rowid: RowId, old_row: Row },
}

struct TxnEntry {
    db_id: u32,
    journal: Mutex<Vec<UndoOp>>,
}

pub struct TxnManager {
    next_id: AtomicU32,
    active: DashMap<u32, TxnEntry>,
}

impl Default for TxnManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnManager {
    pub fn new() -> Self {
        TxnManager { next_id: AtomicU32::new(1), active: DashMap::new() }
    }

    /// Starts a new transaction and returns its id. Every INSERT/UPDATE/
    /// DELETE run with this id attached gets journaled under it until it's
    /// committed or rolled back.
    pub fn begin(&self, db_id: u32) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.active.insert(id, TxnEntry { db_id, journal: Mutex::new(Vec::new()) });
        id
    }

    pub fn record(&self, tx_id: u32, op: UndoOp) {
        if let Some(txn) = self.active.get(&tx_id) {
            txn.journal.lock().unwrap().push(op);
        }
    }

    /// Finishes a transaction successfully: the journal is simply dropped,
    /// since every write in it is already live.
    pub fn commit(&self, tx_id: u32) -> Result<(), SkvsError> {
        self.active
            .remove(&tx_id)
            .map(|_| ())
            .ok_or_else(|| SkvsError::Transaction(format!("No active transaction {}", tx_id)))
    }

    /// Undoes every change made under `tx_id`, in reverse order, and ends it.
    pub fn rollback(&self, state: &KvsState, tx_id: u32) -> Result<(), SkvsError> {
        let (_, entry) = self
            .active
            .remove(&tx_id)
            .ok_or_else(|| SkvsError::Transaction(format!("No active transaction {}", tx_id)))?;
        let ops = entry.journal.into_inner().unwrap();
        undo_all(state, entry.db_id, ops);
        Ok(())
    }

    pub fn is_active(&self, tx_id: u32) -> bool {
        self.active.contains_key(&tx_id)
    }
}

/// Replays `ops` in reverse, restoring rows (and keeping secondary indexes
/// and any fts5 index in sync as it goes) to how they looked before the
/// journaled changes happened. Trigger-cascaded writes are undone
/// automatically here too, since each cascaded INSERT/UPDATE/DELETE pushed
/// its own entry onto the same journal in the order it actually ran.
pub fn undo_all(state: &KvsState, db_id: u32, ops: Vec<UndoOp>) {
    for op in ops.into_iter().rev() {
        match op {
            UndoOp::Insert { table, rowid } => {
                if let Some(store) = state.get_table_store(db_id, &table) {
                    if let Some((_, row)) = store.remove(&rowid) {
                        let _ = update_indexes(state, db_id, &table, &row, rowid, IndexOp::Delete);
                        crate::sql::dml::sync_fts_index(state, db_id, &table, rowid, Some(&row), true);
                    }
                }
            }
            UndoOp::Delete { table, rowid, row } => {
                if let Some(store) = state.get_table_store(db_id, &table) {
                    store.insert(rowid, row.clone());
                    let _ = update_indexes(state, db_id, &table, &row, rowid, IndexOp::Insert);
                    crate::sql::dml::sync_fts_index(state, db_id, &table, rowid, Some(&row), false);
                }
            }
            UndoOp::Update { table, rowid, old_row } => {
                if let Some(store) = state.get_table_store(db_id, &table) {
                    let current = store.get(&rowid).map(|r| r.clone());
                    store.insert(rowid, old_row.clone());
                    let _ = update_indexes(state, db_id, &table, &old_row, rowid, IndexOp::Update {
                        old_row: current.clone().unwrap_or_else(Row::new),
                    });
                    if let Some(current) = current {
                        crate::sql::dml::sync_fts_index_change(state, db_id, &table, rowid, Some(&current), Some(&old_row));
                    }
                }
            }
        }
    }
}
