use serde::{Serialize, Deserialize};
use std::fs::{OpenOptions};
use std::io::{Write, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use tokio::time;
use anyhow::Result;
use crate::state::KvsState;
use crate::config::Config;
use crate::replication::ReplicationService;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub db_id: u32,
    pub table: String,
    pub op: OpType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OpType {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct WalWriter {
    state: Arc<KvsState>,
    config: Arc<Config>,
    sender: UnboundedSender<Operation>,
    receiver: Arc<tokio::sync::Mutex<UnboundedReceiver<Operation>>>,
    flush_trigger: Arc<tokio::sync::Notify>,
}

impl WalWriter {
    pub fn new(state: Arc<KvsState>, config: &Config) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let flush_trigger = Arc::new(tokio::sync::Notify::new());
        WalWriter {
            state,
            config: Arc::new(config.clone()),
            sender: tx,
            receiver: Arc::new(tokio::sync::Mutex::new(rx)),
            flush_trigger,
        }
    }

    pub fn send_operation(&self, op: Operation) {
        let _ = self.sender.send(op);
    }

    pub fn flush_now(&self) {
        self.flush_trigger.notify_one();
    }

    /// Loads previously-persisted SQL table data (tables created/modified via the SQL
    /// query() API) back into memory. Must be called once at startup, after KvsState is
    /// constructed, before serving any requests.
    pub fn load_snapshot(state: &KvsState, config: &Config) {
        load_sql_snapshot(state, config);
    }

    pub fn start(&self, rt: &tokio::runtime::Runtime) {
        let state = self.state.clone();
        let config = self.config.clone();
        let receiver = self.receiver.clone();
        let flush_trigger = self.flush_trigger.clone();
        let peer_addr = config.peer.as_ref().and_then(|p| p.address.parse().ok());

        rt.spawn(async move {
            let mut batch = Vec::with_capacity(config.wal.batch_size);
            let mut flush_interval = time::interval(time::Duration::from_secs(config.wal.flush_interval_secs));

            loop {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        if !batch.is_empty() {
                            write_batch(&batch, &config).await;
                            if let Some(addr) = &peer_addr {
                                let _ = ReplicationService::send_batch(&batch, addr);
                            }
                            batch.clear();
                        }
                        // SQL-table state (INSERT/UPDATE/DELETE via query()) isn't routed
                        // through the WAL operation channel above - it lives only in
                        // KvsState.dbs. Snapshot it to disk on the same tick so it actually
                        // survives a restart instead of silently staying in-memory-only.
                        save_sql_snapshot(&state, &config);
                    }
                    _ = flush_trigger.notified() => {
                        if !batch.is_empty() {
                            write_batch(&batch, &config).await;
                            if let Some(addr) = &peer_addr {
                                let _ = ReplicationService::send_batch(&batch, addr);
                            }
                            batch.clear();
                        }
                        save_sql_snapshot(&state, &config);
                    }
                    // Receive from channel
                    maybe_op = async {
                        let mut rx = receiver.lock().await;
                        rx.recv().await
                    } => {
                        if let Some(op) = maybe_op {
                            batch.push(op);
                            if batch.len() >= config.wal.batch_size {
                                write_batch(&batch, &config).await;
                                if let Some(addr) = &peer_addr {
                                    let _ = ReplicationService::send_batch(&batch, addr);
                                }
                                batch.clear();
                            }
                        }
                    }
                }
            }
        });
    }
}

async fn write_batch(batch: &[Operation], config: &Config) {
    let mut groups: std::collections::HashMap<u32, Vec<&Operation>> = std::collections::HashMap::new();
    for op in batch {
        groups.entry(op.db_id).or_default().push(op);
    }

    for (db_id, ops) in groups {
        let dir = PathBuf::from(&config.storage.base_dir).join(format!("db_{}", db_id));
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!("Failed to create directory {}: {}", dir.display(), e);
            continue;
        }
        let file_path = dir.join("wal.bin");
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
        {
            Ok(f) => BufWriter::new(f),
            Err(e) => {
                eprintln!("Failed to open WAL file {}: {}", file_path.display(), e);
                continue;
            }
        };

        for op in ops {
            let encoded = match bincode::serialize(op) {
                Ok(bytes) => bytes,
                Err(e) => {
                    eprintln!("Serialization error: {}", e);
                    continue;
                }
            };
            let len = encoded.len() as u32;
            if let Err(e) = file.write_all(&len.to_le_bytes()) {
                eprintln!("Write length error: {}", e);
                break;
            }
            if let Err(e) = file.write_all(&encoded) {
                eprintln!("Write data error: {}", e);
                break;
            }
        }
        if let Err(e) = file.flush() {
            eprintln!("Flush error: {}", e);
        }
        if let Ok(f) = file.into_inner() {
            let _ = f.sync_all();
        }
    }
}

/// Persists every SQL table's rows (KvsState.dbs) to `{base_dir}/db_{id}/tables.bin`.
/// Written to a temp file and renamed into place so a crash mid-write can't corrupt the
/// previous good snapshot.
fn save_sql_snapshot(state: &KvsState, config: &Config) {
    for entry in state.dbs.iter() {
        let db_id = *entry.key();
        let tables = entry.value();
        let schemas = state.schemas.get(&db_id);

        // Snapshot shape: Vec<(table_name, Option<TableSchema>, Vec<(RowId, Row)>)>
        let mut dump: Vec<(String, Option<crate::schema::TableSchema>, Vec<(crate::schema::RowId, crate::schema::Row)>)> = Vec::new();
        for table_entry in tables.iter() {
            let table_name = table_entry.key().clone();
            let rows: Vec<(crate::schema::RowId, crate::schema::Row)> = table_entry
                .value()
                .iter()
                .map(|r| (*r.key(), r.value().clone()))
                .collect();
            let schema = schemas
                .as_ref()
                .and_then(|s| s.get(&table_name))
                .map(|s| (**s).clone());
            dump.push((table_name, schema, rows));
        }

        let dir = PathBuf::from(&config.storage.base_dir).join(format!("db_{}", db_id));
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!("Failed to create directory {}: {}", dir.display(), e);
            continue;
        }

        let encoded = match bincode::serialize(&dump) {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!("Failed to serialize SQL snapshot for db {}: {}", db_id, e);
                continue;
            }
        };

        let final_path = dir.join("tables.bin");
        let tmp_path = dir.join("tables.bin.tmp");
        if let Err(e) = std::fs::write(&tmp_path, &encoded) {
            eprintln!("Failed to write SQL snapshot temp file {}: {}", tmp_path.display(), e);
            continue;
        }
        if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
            eprintln!("Failed to finalize SQL snapshot {}: {}", final_path.display(), e);
        }
    }
}

/// Loads any previously-saved `tables.bin` snapshots back into KvsState.dbs, and restores
/// the rowid generator for each table so new INSERTs don't collide with restored rows.
fn load_sql_snapshot(state: &KvsState, config: &Config) {
    for entry in state.dbs.iter() {
        let db_id = *entry.key();
        let path = PathBuf::from(&config.storage.base_dir)
            .join(format!("db_{}", db_id))
            .join("tables.bin");

        if !path.exists() {
            continue;
        }

        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Failed to read SQL snapshot {}: {}", path.display(), e);
                continue;
            }
        };

        let dump: Vec<(String, Option<crate::schema::TableSchema>, Vec<(crate::schema::RowId, crate::schema::Row)>)> =
            match bincode::deserialize(&bytes) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!(
                        "Incompatible or corrupt SQL snapshot at {}: {}. Quarantining it as .bak so it doesn't block startup; any data it held is lost.",
                        path.display(), e
                    );
                    let backup_path = path.with_extension("bin.bak");
                    let _ = std::fs::rename(&path, &backup_path);
                    continue;
                }
            };

        let tables = entry.value().clone();
        let schemas_map = state
            .schemas
            .entry(db_id)
            .or_insert_with(|| Arc::new(dashmap::DashMap::new()))
            .clone();

        for (table_name, schema, rows) in dump {
            if let Some(schema) = schema {
                schemas_map.insert(table_name.clone(), Arc::new(schema));
            }

            let store = tables
                .entry(table_name.clone())
                .or_insert_with(|| Arc::new(dashmap::DashMap::new()))
                .clone();

            let mut max_rowid: u64 = 0;
            for (row_id, row) in rows {
                if row_id > max_rowid {
                    max_rowid = row_id;
                }
                store.insert(row_id, row);
            }

            // Ensure the next auto-generated rowid continues past whatever we just
            // restored, otherwise a fresh INSERT could overwrite a restored row.
            state.set_min_next_rowid(db_id, &table_name, max_rowid + 1);
        }
    }
}