use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::path::PathBuf;
use crate::schema::*;
use crate::fts::FtsVirtualTable;
use crate::virtual_table::VirtualTableRegistry;
use crate::error::SkvsError;

pub struct KvsState {
    // Core data
    pub dbs: DashMap<u32, Arc<DashMap<String, Arc<DashMap<RowId, Row>>>>>,
    pub schemas: DashMap<u32, Arc<DashMap<String, Arc<TableSchema>>>>,
    pub rowid_generators: DashMap<u32, Arc<DashMap<String, u64>>>,
    pub db_name_to_id: HashMap<String, u32>,
    pub raw_stores: DashMap<u32, Arc<DashMap<String, Arc<DashMap<Vec<u8>, Vec<u8>>>>>>,

    // Triggers: db_id -> table_name -> Vec<Arc<TriggerDef>>
    pub triggers: DashMap<u32, Arc<DashMap<String, Vec<Arc<TriggerDef>>>>>,
    // Views: db_id -> view_name -> (query string)
    pub views: DashMap<u32, Arc<DashMap<String, String>>>,
    // FTS tables: db_id -> table_name -> Arc<FtsVirtualTable>
    pub fts_tables: DashMap<u32, Arc<DashMap<String, Arc<FtsVirtualTable>>>>,
    // Virtual table registry
    pub virtual_tables: DashMap<u32, Arc<VirtualTableRegistry>>,

    // If > 0, raw_stores tables that grow past this many in-memory entries
    // spill their oldest-seen entries out to disk (see `overflow_dir`) to
    // relieve memory pressure; `get_raw`/`remove_raw` transparently fall
    // back to disk for anything that got evicted.
    max_entries_per_table: usize,
    overflow_dir: PathBuf,
}

impl KvsState {
    pub fn new(databases: &[crate::config::DatabaseConfig]) -> Self {
        Self::new_with_config(databases, 0, "/tmp/skvs-overflow")
    }

    pub fn new_with_config(databases: &[crate::config::DatabaseConfig], max_entries_per_table: usize, overflow_dir: &str) -> Self {
        let dbs = DashMap::new();
        let schemas = DashMap::new();
        let rowid_gens = DashMap::new();
        let raw_stores = DashMap::new();
        let triggers = DashMap::new();
        let views = DashMap::new();
        let fts_tables = DashMap::new();
        let virtual_tables = DashMap::new();
        let mut name_map = HashMap::new();

        for db in databases {
            dbs.insert(db.id, Arc::new(DashMap::new()));
            schemas.insert(db.id, Arc::new(DashMap::new()));
            rowid_gens.insert(db.id, Arc::new(DashMap::new()));
            raw_stores.insert(db.id, Arc::new(DashMap::new()));
            triggers.insert(db.id, Arc::new(DashMap::new()));
            views.insert(db.id, Arc::new(DashMap::new()));
            fts_tables.insert(db.id, Arc::new(DashMap::new()));
            virtual_tables.insert(db.id, Arc::new(VirtualTableRegistry::new()));
            name_map.insert(db.name.clone(), db.id);
        }

        KvsState {
            dbs,
            schemas,
            rowid_generators: rowid_gens,
            db_name_to_id: name_map,
            raw_stores,
            triggers,
            views,
            fts_tables,
            virtual_tables,
            max_entries_per_table,
            overflow_dir: PathBuf::from(overflow_dir),
        }
    }

    pub fn get_db_id(&self, name: &str) -> Option<u32> {
        self.db_name_to_id.get(name).copied()
    }

    pub fn get_table_store(&self, db_id: u32, table: &str) -> Option<Arc<DashMap<RowId, Row>>> {
        self.dbs.get(&db_id)
            .and_then(|db| db.get(table).map(|store| store.clone()))
    }

    pub fn get_schema(&self, db_id: u32, table: &str) -> Option<Arc<TableSchema>> {
        self.schemas.get(&db_id)
            .and_then(|s| s.get(table).map(|schema| schema.clone()))
    }

    pub fn get_next_rowid(&self, db_id: u32, table: &str) -> u64 {
        let gens = self.rowid_generators.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
        let mut gen = gens.entry(table.to_string()).or_insert(1);
        let id = *gen;
        *gen += 1;
        id
    }

    // ---- Raw key-value operations ----
    //
    // When `max_entries_per_table` is set, a table that grows past the limit
    // spills a few of its entries to a plain file per key under
    // `overflow_dir/{db_id}/{table}/{hex(key)}`, freeing the in-memory slot.
    // This is a simple, best-effort mechanism (eviction order isn't strict
    // LRU) rather than a full tiered-storage engine.
    pub fn put_raw(&self, db_id: u32, table: &str, key: Vec<u8>, value: Vec<u8>) {
        let raw_stores = self.raw_stores.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
        let table_store = raw_stores.entry(table.to_string()).or_insert_with(|| Arc::new(DashMap::new()));

        // A key that was previously spilled to disk is now fresh again.
        self.remove_overflow_file(db_id, table, &key);
        table_store.insert(key, value);

        if self.max_entries_per_table > 0 && table_store.len() > self.max_entries_per_table {
            self.evict_to_disk(db_id, table, &table_store);
        }
    }

    fn evict_to_disk(&self, db_id: u32, table: &str, table_store: &DashMap<Vec<u8>, Vec<u8>>) {
        let overflow = table_store.len().saturating_sub(self.max_entries_per_table);
        let mut to_evict = Vec::with_capacity(overflow);
        for entry in table_store.iter().take(overflow) {
            to_evict.push(entry.key().clone());
        }
        for key in to_evict {
            if let Some((_, value)) = table_store.remove(&key) {
                if let Err(e) = self.write_overflow_file(db_id, table, &key, &value) {
                    eprintln!("Failed to spill key to disk, keeping it in memory: {}", e);
                    table_store.insert(key, value);
                }
            }
        }
    }

    fn overflow_path(&self, db_id: u32, table: &str, key: &[u8]) -> PathBuf {
        self.overflow_dir.join(db_id.to_string()).join(table).join(hex::encode(key))
    }

    fn write_overflow_file(&self, db_id: u32, table: &str, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let path = self.overflow_path(db_id, table, key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, value)
    }

    fn remove_overflow_file(&self, db_id: u32, table: &str, key: &[u8]) {
        let path = self.overflow_path(db_id, table, key);
        let _ = std::fs::remove_file(path);
    }

    pub fn get_raw(&self, db_id: u32, table: &str, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(v) = self.raw_stores.get(&db_id)
            .and_then(|db| db.get(table).and_then(|store| store.get(key).map(|v| v.clone())))
        {
            return Some(v);
        }
        // Not in memory: check whether it was spilled to disk.
        std::fs::read(self.overflow_path(db_id, table, key)).ok()
    }

    pub fn remove_raw(&self, db_id: u32, table: &str, key: &[u8]) {
        if let Some(db) = self.raw_stores.get(&db_id) {
            if let Some(store) = db.get(table) {
                store.remove(key);
            }
        }
        self.remove_overflow_file(db_id, table, key);
    }

    // ---- Triggers ----
    pub fn add_trigger(&self, db_id: u32, table: &str, trigger: Arc<TriggerDef>) -> Result<(), SkvsError> {
        let triggers_map = self.triggers.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
        let mut table_triggers = triggers_map.entry(table.to_string()).or_insert_with(Vec::new);
        table_triggers.push(trigger);
        Ok(())
    }

    pub fn get_triggers(&self, db_id: u32, table: &str) -> Vec<Arc<TriggerDef>> {
        self.triggers.get(&db_id)
            .and_then(|map| map.get(table).map(|v| v.clone()))
            .unwrap_or_else(Vec::new)
    }

    // ---- Views ----
    pub fn add_view(&self, db_id: u32, name: &str, query: &str) -> Result<(), SkvsError> {
        let views_map = self.views.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
        views_map.insert(name.to_string(), query.to_string());
        Ok(())
    }

    pub fn remove_view(&self, db_id: u32, name: &str) -> Result<(), SkvsError> {
        if let Some(map) = self.views.get(&db_id) {
            map.remove(name);
            Ok(())
        } else {
            Err(SkvsError::Schema(format!("View {} not found", name)))
        }
    }

    pub fn get_view_definition(&self, db_id: u32, name: &str) -> Option<String> {
        self.views.get(&db_id)
            .and_then(|map| map.get(name).map(|v| v.clone()))
    }

    // ---- FTS ----
    pub fn register_fts_table(&self, db_id: u32, name: &str, table: Arc<FtsVirtualTable>) -> Result<(), SkvsError> {
        let fts_map = self.fts_tables.entry(db_id).or_insert_with(|| Arc::new(DashMap::new()));
        fts_map.insert(name.to_string(), table);
        Ok(())
    }

    pub fn get_fts_table(&self, db_id: u32, name: &str) -> Option<Arc<FtsVirtualTable>> {
        self.fts_tables.get(&db_id)
            .and_then(|map| map.get(name).map(|v| v.clone()))
    }

    // ---- Virtual tables ----
    pub fn get_virtual_table_registry(&self, db_id: u32) -> Arc<VirtualTableRegistry> {
        self.virtual_tables.entry(db_id)
            .or_insert_with(|| Arc::new(VirtualTableRegistry::new()))
            .clone()
    }
}