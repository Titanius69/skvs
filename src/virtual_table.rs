use crate::state::KvsState;
use crate::schema::*;
use crate::error::SkvsError;
use std::sync::Arc;
use async_trait::async_trait;

#[async_trait]
pub trait VirtualTable: Send + Sync {
    fn name(&self) -> &str;
    async fn create(&self, args: &[String]) -> Result<(), SkvsError>;
    async fn drop(&self) -> Result<(), SkvsError>;
    async fn insert(&self, row: Row) -> Result<RowId, SkvsError>;
    async fn update(&self, rowid: RowId, row: Row) -> Result<(), SkvsError>;
    async fn delete(&self, rowid: RowId) -> Result<(), SkvsError>;
    async fn select(&self, constraints: &[Constraint]) -> Result<Vec<Row>, SkvsError>;
}

#[derive(Debug, Clone)]
pub struct Constraint {
    pub column: String,
    pub operator: ConstraintOperator,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub enum ConstraintOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Like,
    Glob,
    Match,
}

pub struct VirtualTableRegistry {
    tables: Arc<dashmap::DashMap<String, Arc<dyn VirtualTable>>>,
}

impl VirtualTableRegistry {
    pub fn new() -> Self {
        VirtualTableRegistry {
            tables: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub fn register(&self, name: &str, table: Arc<dyn VirtualTable>) -> Result<(), SkvsError> {
        if self.tables.contains_key(name) {
            return Err(SkvsError::Schema(format!("Virtual table {} already exists", name)));
        }
        self.tables.insert(name.to_string(), table);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn VirtualTable>> {
        self.tables.get(name).map(|entry| entry.clone())
    }

    pub fn remove(&self, name: &str) -> Option<Arc<dyn VirtualTable>> {
        self.tables.remove(name).map(|(_, v)| v)
    }
}