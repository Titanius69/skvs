use serde::{Deserialize, Serialize};
use indexmap::IndexMap;

pub type RowId = u64;
pub type ColumnName = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn from_json(v: serde_json::Value) -> Option<Self> {
        match v {
            serde_json::Value::Null => Some(Value::Null),
            serde_json::Value::Bool(b) => Some(Value::Integer(if b { 1 } else { 0 })),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() { Some(Value::Integer(i)) }
                else if let Some(f) = n.as_f64() { Some(Value::Real(f)) }
                else { None }
            }
            serde_json::Value::String(s) => Some(Value::Text(s)),
            // Tagged-blob convention: `{"__blob__": "<base64>"}`. This is how
            // a caller (server.js) sends a real BLOB parameter over JSON/HTTP
            // - a bare JSON string always becomes Value::Text, since JSON has
            // no separate binary type and we can't otherwise tell "this
            // string is base64 for a blob" from "this string is just text".
            serde_json::Value::Object(ref obj) if obj.len() == 1 && obj.contains_key("__blob__") => {
                match obj.get("__blob__") {
                    Some(serde_json::Value::String(b64)) => {
                        base64::decode(b64).ok().map(Value::Blob)
                    }
                    _ => None,
                }
            }
            serde_json::Value::Array(arr) => {
                let json_str = serde_json::to_string(&arr).ok()?;
                Some(Value::Blob(json_str.into_bytes()))
            }
            serde_json::Value::Object(obj) => {
                let json_str = serde_json::to_string(&obj).ok()?;
                Some(Value::Blob(json_str.into_bytes()))
            }
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Integer(i) => serde_json::Value::Number((*i).into()),
            Value::Real(f) => {
                if let Some(n) = serde_json::Number::from_f64(*f) {
                    serde_json::Value::Number(n)
                } else {
                    serde_json::Value::Null
                }
            }
            Value::Text(s) => serde_json::Value::String(s.clone()),
            Value::Blob(b) => serde_json::Value::String(base64::encode(b)),
        }
    }

    /// Ordering used by `WHERE`/`ORDER BY`/`BETWEEN`/index range logic.
    ///
    /// Values of the same "numeric family" (Integer/Real) compare
    /// numerically even when their variants differ - this matters a lot in
    /// practice, since a column stored as REAL (e.g. `price REAL`) is very
    /// commonly compared against an integer literal (`WHERE price > 100`).
    /// Comparing across genuinely different types (e.g. text vs. integer)
    /// falls back to a fixed type-rank ordering (SQLite's NULL < INTEGER/REAL
    /// < TEXT < BLOB) so results are at least stable/total instead of the
    /// previous behavior of silently reporting every cross-type comparison
    /// as `Equal` (which made e.g. `WHERE real_column > 100` always false).
    pub fn compare(&self, other: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Integer(i1), Value::Integer(i2)) => i1.cmp(i2),
            (Value::Real(f1), Value::Real(f2)) => f1.partial_cmp(f2).unwrap_or(Ordering::Equal),
            (Value::Integer(i1), Value::Real(f2)) => {
                (*i1 as f64).partial_cmp(f2).unwrap_or(Ordering::Equal)
            }
            (Value::Real(f1), Value::Integer(i2)) => {
                f1.partial_cmp(&(*i2 as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::Text(s1), Value::Text(s2)) => s1.cmp(s2),
            (Value::Blob(b1), Value::Blob(b2)) => b1.cmp(b2),
            _ => self.type_rank().cmp(&other.type_rank()),
        }
    }

    fn type_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Integer(_) | Value::Real(_) => 1,
            Value::Text(_) => 2,
            Value::Blob(_) => 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub not_null: bool,
    pub default: Option<Value>,
    pub unique: bool,
    pub check_expr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    #[serde(with = "indexmap::map::serde_seq")]
    pub columns: IndexMap<String, ColumnDef>,
    pub rowid_column: Option<String>,
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub indices: Vec<IndexDef>,
    pub triggers: Vec<TriggerDef>,
    /// Table-level `UNIQUE(col, ...)` / `PRIMARY KEY(col, ...)` constraints
    /// (as opposed to a `col TYPE PRIMARY KEY` column option, which sets
    /// `ColumnDef.primary_key`/`unique` directly). Each entry is a group of
    /// column names that together must be unique; `is_primary` groups are
    /// additionally NOT NULL.
    #[serde(default)]
    pub unique_groups: Vec<UniqueGroup>,
    /// Table-level `CHECK (...)` constraints (as opposed to a per-column
    /// `CHECK` option, stored in `ColumnDef.check_expr`).
    #[serde(default)]
    pub table_checks: Vec<String>,
    /// Set for a `CREATE VIRTUAL TABLE ... USING fts5(...)` table: the name
    /// of the column whose text content is indexed for full-text search.
    /// Used to rebuild the in-memory FTS inverted index after a restart,
    /// since only the raw rows (not the index) are persisted.
    #[serde(default)]
    pub fts5_content_column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueGroup {
    pub columns: Vec<String>,
    pub is_primary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyDef {
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FkAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub partial_where: Option<String>,
    pub is_expression: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDef {
    pub name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table: String,
    pub for_each_row: bool,
    pub condition: Option<String>,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TriggerTiming { Before, After, InsteadOf }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TriggerEvent { Insert, Update, Delete }

pub type Row = IndexMap<String, Value>;