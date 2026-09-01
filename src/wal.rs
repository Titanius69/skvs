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

/// Persists everything a restart needs to reconstruct in-memory state:
/// - every SQL table's rows + schema (`tables.bin`, as before)
/// - the raw put/get/remove key-value tables (`raw.bin`) - previously these
///   were only ever written to the `wal.bin` operation log and never read
///   back on startup, so `put()`/`get()`/`remove()` data (and the
///   `__indexes__` internal table that secondary indexes live in) was
///   silently lost on every restart.
/// - views and triggers (`views.bin` / `triggers.bin`), which weren't
///   persisted at all before, so any view or trigger had to be recreated by
///   hand after every restart even though the tables they referenced
///   survived.
///
/// Written to temp files and renamed into place so a crash mid-write can't
/// corrupt the previous good snapshot.
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

        write_snapshot_file(&dir, "tables.bin", &dump, db_id, "SQL");

        if let Some(raw_tables) = state.raw_stores.get(&db_id) {
            let raw_dump: Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)> = raw_tables.iter().map(|t| {
                let entries: Vec<(Vec<u8>, Vec<u8>)> = t.value().iter().map(|e| (e.key().clone(), e.value().clone())).collect();
                (t.key().clone(), entries)
            }).collect();
            write_snapshot_file(&dir, "raw.bin", &raw_dump, db_id, "raw key-value");
        }

        if let Some(views) = state.views.get(&db_id) {
            let views_dump: Vec<(String, String)> = views.iter().map(|v| (v.key().clone(), v.value().clone())).collect();
            write_snapshot_file(&dir, "views.bin", &views_dump, db_id, "view");
        }

        if let Some(triggers) = state.triggers.get(&db_id) {
            let triggers_dump: Vec<(String, Vec<crate::schema::TriggerDef>)> = triggers.iter().map(|t| {
                let defs: Vec<crate::schema::TriggerDef> = t.value().iter().map(|d| (**d).clone()).collect();
                (t.key().clone(), defs)
            }).collect();
            write_snapshot_file(&dir, "triggers.bin", &triggers_dump, db_id, "trigger");
        }
    }
}

fn write_snapshot_file<T: Serialize>(dir: &PathBuf, filename: &str, data: &T, db_id: u32, kind: &str) {
    let encoded = match bincode::serialize(data) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Failed to serialize {} snapshot for db {}: {}", kind, db_id, e);
            return;
        }
    };
    let final_path = dir.join(filename);
    let tmp_path = dir.join(format!("{}.tmp", filename));
    if let Err(e) = std::fs::write(&tmp_path, &encoded) {
        eprintln!("Failed to write {} snapshot temp file {}: {}", kind, tmp_path.display(), e);
        return;
    }
    if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
        eprintln!("Failed to finalize {} snapshot {}: {}", kind, final_path.display(), e);
    }
}

/// Loads any previously-saved snapshots (`tables.bin`, `raw.bin`,
/// `views.bin`, `triggers.bin`) back into `KvsState`, restores each table's
/// rowid generator so new INSERTs don't collide with restored rows, and
/// rebuilds the in-memory fts5 inverted index for any table that has one
/// (the index itself isn't snapshotted - it's cheap to recompute from the
/// now-restored row content, and doing it this way means there's only ever
/// one source of truth for what's indexed: the actual row data).
fn load_sql_snapshot(state: &KvsState, config: &Config) {
    for entry in state.dbs.iter() {
        let db_id = *entry.key();
        let base = PathBuf::from(&config.storage.base_dir).join(format!("db_{}", db_id));

        let path = base.join("tables.bin");
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

        let mut fts_to_rebuild: Vec<(String, String)> = Vec::new(); // (table, content_column)

        for (table_name, schema, rows) in dump {
            if let Some(schema) = schema {
                if let Some(content_col) = &schema.fts5_content_column {
                    fts_to_rebuild.push((table_name.clone(), content_col.clone()));
                }
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

            // Secondary indexes are an in-memory-only structure (see
            // `state::IndexStore`) - only row content is persisted - so
            // every index defined on this table has to be rebuilt from the
            // rows that were just restored above.
            if let Some(schema) = schemas_map.get(&table_name) {
                for idx in &schema.indices {
                    crate::index::rebuild_index(state, db_id, &table_name, idx);
                }
            }
        }

        // Raw put()/get()/remove() key-value tables. Without this, every
        // raw-store table's contents (and the `__indexes__` table that
        // secondary indexes live in) were silently lost on every restart.
        let raw_path = base.join("raw.bin");
        if raw_path.exists() {
            if let Ok(bytes) = std::fs::read(&raw_path) {
                match bincode::deserialize::<Vec<(String, Vec<(Vec<u8>, Vec<u8>)>)>>(&bytes) {
                    Ok(raw_dump) => {
                        let raw_tables = state.raw_stores.entry(db_id).or_insert_with(|| Arc::new(dashmap::DashMap::new())).clone();
                        for (table_name, entries) in raw_dump {
                            let store = raw_tables.entry(table_name).or_insert_with(|| Arc::new(dashmap::DashMap::new())).clone();
                            for (k, v) in entries {
                                store.insert(k, v);
                            }
                        }
                    }
                    Err(e) => eprintln!("Incompatible or corrupt raw snapshot at {}: {}. Skipping.", raw_path.display(), e),
                }
            }
        }

        // Views.
        let views_path = base.join("views.bin");
        if views_path.exists() {
            if let Ok(bytes) = std::fs::read(&views_path) {
                if let Ok(views_dump) = bincode::deserialize::<Vec<(String, String)>>(&bytes) {
                    let views_map = state.views.entry(db_id).or_insert_with(|| Arc::new(dashmap::DashMap::new())).clone();
                    for (name, query) in views_dump {
                        views_map.insert(name, query);
                    }
                }
            }
        }

        // Triggers.
        let triggers_path = base.join("triggers.bin");
        if triggers_path.exists() {
            if let Ok(bytes) = std::fs::read(&triggers_path) {
                if let Ok(triggers_dump) = bincode::deserialize::<Vec<(String, Vec<crate::schema::TriggerDef>)>>(&bytes) {
                    let triggers_map = state.triggers.entry(db_id).or_insert_with(|| Arc::new(dashmap::DashMap::new())).clone();
                    for (table_name, defs) in triggers_dump {
                        triggers_map.insert(table_name, defs.into_iter().map(Arc::new).collect());
                    }
                }
            }
        }

        // Rebuild fts5 full-text indexes from the now-restored row content.
        for (table_name, content_col) in fts_to_rebuild {
            state.register_fts_table(db_id, &table_name, Arc::new(crate::fts::FtsVirtualTable::new(&table_name, &content_col))).ok();
            if let Some(fts) = state.get_fts_table(db_id, &table_name) {
                if let Some(store) = state.get_table_store(db_id, &table_name) {
                    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime for fts rebuild");
                    rt.block_on(async {
                        for row_entry in store.iter() {
                            let rowid = *row_entry.key();
                            if let Some(crate::schema::Value::Text(content)) = row_entry.value().get(&content_col) {
                                let _ = fts.insert(rowid, content).await;
                            }
                        }
                    });
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DatabaseConfig, StorageConfig};
    use crate::sql::SqlEngine;
    use crate::schema::Value;

    fn test_config(dir: &std::path::Path) -> Config {
        let mut config = Config::default();
        config.storage = StorageConfig { base_dir: dir.to_string_lossy().to_string() };
        config.databases = vec![DatabaseConfig { id: 0, name: "default".into() }];
        config
    }

    /// End-to-end restart simulation: populate a table, a raw KV entry, a
    /// view, and a trigger, snapshot everything to a temp dir, build a *new*
    /// `KvsState` from scratch and load the snapshot into it, then check
    /// every piece of data actually came back. Before this change, only the
    /// SQL table rows survived a restart - raw put()/get() data, views, and
    /// triggers were silently lost every time the process restarted.
    #[test]
    fn full_snapshot_round_trip_survives_a_restart() {
        let tmp = std::env::temp_dir().join(format!("skvs_wal_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let config = test_config(&tmp);

        let state = KvsState::new(&config.databases);
        SqlEngine::execute(&state, 0, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "INSERT INTO t (id, name) VALUES (1, 'alice')", &[], None).unwrap();
        SqlEngine::execute(&state, 0, "CREATE VIEW v AS SELECT * FROM t", &[], None).unwrap();
        state.put_raw(0, "raw_table", b"k1".to_vec(), b"v1".to_vec());

        save_sql_snapshot(&state, &config);

        // Fresh process, fresh state: nothing but what load_snapshot restores.
        let restored = KvsState::new(&config.databases);
        load_sql_snapshot(&restored, &config);

        let rows = SqlEngine::execute(&restored, 0, "SELECT name FROM t WHERE id = 1", &[], None).unwrap();
        assert_eq!(rows.rows.len(), 1, "SQL table row must survive a restart");
        assert_eq!(rows.rows[0].get("name"), Some(&Value::Text("alice".into())));

        let view_rows = SqlEngine::execute(&restored, 0, "SELECT name FROM v", &[], None).unwrap();
        assert_eq!(view_rows.rows.len(), 1, "view definition must survive a restart");

        let raw = restored.get_raw(0, "raw_table", b"k1");
        assert_eq!(raw, Some(b"v1".to_vec()), "raw put()/get() data must survive a restart");

        // A fresh INSERT after restore must not collide with the restored row's id.
        SqlEngine::execute(&restored, 0, "INSERT INTO t (name) VALUES ('bob')", &[], None).unwrap();
        let all = SqlEngine::execute(&restored, 0, "SELECT id, name FROM t ORDER BY id", &[], None).unwrap();
        assert_eq!(all.rows.len(), 2);
        assert_eq!(all.rows[1].get("id"), Some(&Value::Integer(2)));

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
