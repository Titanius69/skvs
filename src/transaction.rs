use crate::state::KvsState;
use crate::schema::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Transaction {
    pub id: u64,
    pub db_id: u32,
    pub journal: HashMap<String, HashMap<RowId, Row>>,
    pub state: Arc<KvsState>,
}

impl Transaction {
    pub fn begin(state: &Arc<KvsState>, db_id: u32) -> Result<Self, crate::error::KvsError> {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Transaction {
            id,
            db_id,
            journal: HashMap::new(),
            state: state.clone(),
        })
    }
    pub fn commit(self) -> Result<(), crate::error::KvsError> { Ok(()) }
    pub fn rollback(self) -> Result<(), crate::error::KvsError> { Ok(()) }
}