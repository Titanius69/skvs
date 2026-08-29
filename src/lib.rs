use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

mod config;
mod constraint;
mod error;
mod fts;
mod index;
mod json;
mod replication;
mod schema;
mod sql;
mod state;
mod transaction;
mod trigger;
mod view;
mod virtual_table;
mod wal;

use config::Config;
use state::KvsState;
use wal::WalWriter;
use replication::ReplicationService;

static STATE: OnceLock<Arc<KvsState>> = OnceLock::new();
static WAL_WRITER: OnceLock<Arc<WalWriter>> = OnceLock::new();
static REPLICATION: OnceLock<Arc<ReplicationService>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn to_js_err<E: std::fmt::Display>(e: E) -> Error {
    Error::from_reason(e.to_string())
}

#[napi]
pub fn init(config_path: Option<String>) -> Result<()> {
    if STATE.get().is_some() {
        return Ok(());
    }

    let rt = Runtime::new().map_err(to_js_err)?;
    RUNTIME.set(rt).map_err(|_| Error::from_reason("Runtime already set"))?;

    let path = config_path.unwrap_or_else(|| "/etc/skvs/config.toml".to_string());
    let config = config::load_config(&path).map_err(to_js_err)?;

    let state = Arc::new(KvsState::new_with_config(
        &config.databases,
        config.memory.max_entries_per_table,
        &format!("{}/overflow", config.storage.base_dir),
    ));

    // Restore any SQL-table data (INSERT/UPDATE/etc. via query()) that was snapshotted to
    // disk on a previous run, before anything starts serving requests.
    WalWriter::load_snapshot(&state, &config);

    let wal = Arc::new(WalWriter::new(state.clone(), &config));
    wal.start(RUNTIME.get().unwrap());

    let repl = Arc::new(ReplicationService::new(state.clone(), &config));
    repl.start(RUNTIME.get().unwrap());

    STATE.set(state).map_err(|_| Error::from_reason("State already set"))?;
    WAL_WRITER.set(wal).map_err(|_| Error::from_reason("WAL already set"))?;
    REPLICATION.set(repl).map_err(|_| Error::from_reason("Replication already set"))?;

    Ok(())
}

#[napi]
pub fn get_db_id_by_name(name: String) -> Option<u32> {
    STATE.get()?.get_db_id(&name)
}

#[napi]
pub fn query(db_id: u32, sql: String, params: Vec<serde_json::Value>) -> Result<serde_json::Value> {
    let state = STATE.get().ok_or_else(|| Error::from_reason("Not initialized"))?;
    let params = params
        .into_iter()
        .map(|v| schema::Value::from_json(v).unwrap_or(schema::Value::Null))
        .collect::<Vec<_>>();
    let result = sql::SqlEngine::execute(state, db_id, &sql, &params, None)
        .map_err(to_js_err)?;

    // Build the JSON response by hand instead of relying on `Value`'s derived
    // `Serialize` impl. That derive produces externally-tagged JSON such as
    // {"Text": "Alice"} / {"Integer": 41}, which is correct for internal
    // (bincode) persistence but is not what HTTP clients expect. `to_json()`
    // turns each cell into a plain JSON scalar instead.
    let rows: Vec<serde_json::Value> = result.rows.iter().map(|row| {
        let map: serde_json::Map<String, serde_json::Value> = row.iter()
            .map(|(k, v)| (k.clone(), v.to_json()))
            .collect();
        serde_json::Value::Object(map)
    }).collect();

    Ok(serde_json::json!({
        "columns": result.columns,
        "rows": rows,
        "affected_rows": result.affected_rows,
    }))
}

#[napi]
pub fn put(db_id: u32, table: String, key: Buffer, value: Buffer) -> Result<()> {
    let state = STATE.get().ok_or_else(|| Error::from_reason("Not initialized"))?;
    let key: Vec<u8> = key.into();
    let value: Vec<u8> = value.into();
    state.put_raw(db_id, &table, key.clone(), value.clone());
    if let Some(wal) = WAL_WRITER.get() {
        wal.send_operation(wal::Operation {
            db_id,
            table,
            op: wal::OpType::Put { key, value },
        });
    }
    Ok(())
}

#[napi]
pub fn get(db_id: u32, table: String, key: Buffer) -> Option<Buffer> {
    let key: Vec<u8> = key.into();
    STATE.get()?.get_raw(db_id, &table, &key).map(Buffer::from)
}

#[napi]
pub fn remove(db_id: u32, table: String, key: Buffer) -> Result<()> {
    let state = STATE.get().ok_or_else(|| Error::from_reason("Not initialized"))?;
    let key: Vec<u8> = key.into();
    state.remove_raw(db_id, &table, &key);
    if let Some(wal) = WAL_WRITER.get() {
        wal.send_operation(wal::Operation {
            db_id,
            table,
            op: wal::OpType::Delete { key },
        });
    }
    Ok(())
}

#[napi]
pub fn flush() -> Result<()> {
    let wal = WAL_WRITER.get().ok_or_else(|| Error::from_reason("WAL not started"))?;
    wal.flush_now();
    Ok(())
}

#[napi]
pub fn get_config() -> serde_json::Value {
    let config = config::load_config_from_default().unwrap_or_else(|_| Config::default());
    serde_json::to_value(&config).unwrap()
}
