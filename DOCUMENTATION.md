# SKVS — Technical Documentation

This document is written so that another AI assistant (or a human contributor)
can read it once and then correctly extend, debug, or reuse this codebase
without re-deriving its architecture from scratch. It describes **what
actually exists in the code**, not the original wishlist — where something is
a stub or unimplemented, that is called out explicitly.

If you are an AI being asked to "add feature X to SKVS", read this whole file
first, then look at the specific source file(s) named in the relevant
section before writing code.

---

## 1. What this project is

SKVS ("Super Key-Value Store") is a small SQL database engine:

- **Storage/engine core**: Rust crate (`crate-type = ["cdylib"]`), compiled as
  a native Node.js addon via [napi-rs](https://napi.rs) (`napi` /
  `napi-derive` / `napi-build`).
- **Node.js binding**: `src/lib.rs` exposes a handful of `#[napi]` functions
  (see §5). napi auto-converts Rust `snake_case` function names to JS
  `camelCase` — always call them as `skvs.getDbIdByName(...)`, not
  `skvs.get_db_id_by_name(...)`.
- **HTTP layer**: `server.js`, a small Express app that wraps the native
  addon and adds an IP allow-list + shared-secret auth layer.
- **Persistence**: an append-only write-ahead log (WAL) per database, batched
  and flushed periodically (`src/wal.rs`). On restart, `init()` reads the
  config but currently **does not replay the WAL yet** — see §8 "Known gaps".
- **Replication**: best-effort UDP fire-and-forget between exactly two
  servers (`src/replication.rs`), matching the original design goal
  ("eventual consistency between two servers").

The data model is: one server process hosts N independent **databases**
(namespaces), each identified by a numeric `db_id` and a string name, both
declared in `config.toml`. Within a database you can have both:

1. **SQL tables** — schema'd, row-oriented, `rowid`-keyed (`IndexMap<String, Value>`
   rows keyed by a `u64` rowid). This is what `CREATE TABLE` / `INSERT` /
   `SELECT` etc. operate on.
2. **Raw key/value pairs** — schemaless `Vec<u8> -> Vec<u8>`, addressed by an
   arbitrary "table" name string. This is what `put`/`get`/`remove` (both the
   napi functions and the HTTP `/table/:tableName/row/:rowKey` routes)
   operate on. **SQL tables and raw KV "tables" are stored completely
   separately** (`state.dbs` vs `state.raw_stores`) — naming one the same as
   the other does not connect them.

---

## 2. Repository layout

```
skvs/
├── Cargo.toml            # Rust crate manifest — pinned dependency versions, see §9
├── build.rs               # napi_build::setup()
├── config.toml             # EXAMPLE config — copy and edit, don't commit real secrets
├── package.json
├── server.js               # Express HTTP API (the only "server" — there is no Rust HTTP server)
├── test_skvs.py            # legacy Python smoke-test script (kept for reference)
└── src/
    ├── lib.rs               # napi bindings: init/query/put/get/remove/flush/getDbIdByName/getConfig
    ├── config.rs             # Config struct + TOML loader (serde + toml crate)
    ├── state.rs              # KvsState: all in-memory data, DashMap-based
    ├── schema.rs             # Value, Row, TableSchema, ColumnDef, IndexDef, TriggerDef, enums
    ├── error.rs              # SkvsError (thiserror) — the one error type used everywhere
    ├── wal.rs                # WalWriter: async batched WAL writer + replication trigger
    ├── replication.rs        # ReplicationService: UDP send/receive of Operation batches
    ├── transaction.rs        # Transaction struct — STUB, not wired into the SQL engine (see §8)
    ├── constraint.rs          # validate_constraints(): NOT NULL / UNIQUE / PK / FK / CHECK(stub)
    ├── trigger.rs              # fire_triggers(): BEFORE/AFTER INSERT/UPDATE/DELETE trigger execution
    ├── index.rs                # update_indexes()/lookup_index(): secondary index maintenance
    ├── view.rs                  # create_view/drop_view/resolve_view — views are just stored SELECT text
    ├── json.rs                  # json_extract/json_array/json_object/json_set — NOT wired into SQL yet
    ├── virtual_table.rs        # VirtualTable trait + VirtualTableRegistry (used by fts.rs)
    ├── fts.rs                   # FtsVirtualTable — simple inverted-index full text search
    └── sql/
        ├── mod.rs               # SqlEngine::execute() — parses SQL, dispatches by Statement variant
        ├── ddl.rs                # CREATE/DROP TABLE, ALTER TABLE(stub), CREATE/DROP INDEX, VIEW
        ├── dml.rs                # INSERT / UPDATE / DELETE + evaluate_expr() (placeholder binding)
        └── select.rs             # SELECT: FROM/JOIN/WHERE/GROUP BY/ORDER BY/LIMIT/DISTINCT
```

---

## 3. Core data types (`src/schema.rs`)

```rust
pub enum Value { Null, Integer(i64), Real(f64), Text(String), Blob(Vec<u8>) }
pub type Row = IndexMap<String, Value>;   // insertion-ordered column map
pub type RowId = u64;
pub type ColumnName = String;

pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,           // Integer | Real | Text | Blob
    pub primary_key: bool,
    pub auto_increment: bool,          // parsed but NOT enforced (see §8)
    pub not_null: bool,
    pub default: Option<Value>,
    pub unique: bool,
    pub check_expr: Option<String>,    // parsed but NOT evaluated (see §8)
}

pub struct TableSchema {
    pub name: String,
    pub columns: IndexMap<String, ColumnDef>,
    pub rowid_column: Option<String>,   // name of the PK column, if any
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub indices: Vec<IndexDef>,
    pub triggers: Vec<TriggerDef>,      // NOTE: triggers actually live in KvsState.triggers, not here
}

pub struct IndexDef { pub name: String, pub columns: Vec<String>, pub unique: bool, .. }
pub struct ForeignKeyDef { pub column: String, pub ref_table: String, pub ref_column: String, on_delete: FkAction, on_update: FkAction }
pub enum FkAction { NoAction, Cascade, SetNull, Restrict, SetDefault }
pub enum TriggerTiming { Before, After, InsteadOf }
pub enum TriggerEvent { Insert, Update, Delete }
pub struct TriggerDef { .. }   // see src/trigger.rs for exact fields
```

`Value` implements a `.compare()` method (used for `ORDER BY`, `>`, `<`, etc.)
and `PartialEq`. It deliberately does **not** implement `Eq`/`Hash` (because
of the `f64` field), so anything that needs to key a `HashMap` by `Value`
(e.g. `GROUP BY`) uses a `format!("{:?}", value)` string key instead — see
`sql/select.rs::apply_group_by`.

---

## 4. In-memory state (`src/state.rs` — `KvsState`)

Everything lives in `DashMap`s keyed first by `db_id: u32`:

| Field                | Type                                                          | Purpose |
|-----------------------|----------------------------------------------------------------|---------|
| `dbs`                | `DashMap<u32, Arc<DashMap<String, Arc<DashMap<RowId, Row>>>>>` | SQL table storage: db → table name → rowid → row |
| `schemas`            | `DashMap<u32, Arc<DashMap<String, Arc<TableSchema>>>>`         | db → table name → schema |
| `rowid_generators`   | `DashMap<u32, Arc<DashMap<String, u64>>>`                      | db → table name → next rowid counter |
| `raw_stores`         | `DashMap<u32, Arc<DashMap<String, Arc<DashMap<Vec<u8>, Vec<u8>>>>>>` | db → "table" → raw KV store |
| `triggers`           | `DashMap<u32, Arc<DashMap<String, Vec<Arc<TriggerDef>>>>>`     | db → table name → triggers |
| `views`              | `DashMap<u32, Arc<DashMap<String, String>>>`                   | db → view name → SELECT text |
| `fts_tables`         | `DashMap<u32, Arc<DashMap<String, Arc<FtsVirtualTable>>>>`     | db → fts table name → index |
| `virtual_tables`     | `DashMap<u32, Arc<VirtualTableRegistry>>`                      | db → generic virtual table registry |
| `db_name_to_id`      | `HashMap<String, u32>`                                        | name → id lookup, built once at `init()` |

`KvsState::new(&config.databases)` pre-creates empty maps for every `[[databases]]`
entry in the config. **Databases cannot currently be added at runtime** —
they must all be declared in `config.toml` before `init()` is called.

Secondary indexes (`src/index.rs`) are implemented on top of `raw_stores`
under the reserved table name `"__indexes__"`, keyed by
`"{table}:{index_name}:{value-as-string}"` → a bincode-encoded `Vec<RowId>`.
This is intentionally simple (single-column indexes only) and is **not yet
consulted by the query planner** — `SELECT` always does a full table scan
(see §8). The index machinery exists and is correctly kept up to date on
INSERT/UPDATE/DELETE; wiring it into `SELECT`'s WHERE-clause evaluation for
equality lookups is the natural next step (see §8).

---

## 5. Native (napi) API — `src/lib.rs`

All of these are called from JS as `require('./skvs.node').<camelCaseName>(...)`.

| Rust fn (snake_case)     | JS name (camelCase)   | Signature | Notes |
|--------------------------|------------------------|-----------|-------|
| `init`                  | `init`                | `(configPath?: string) => void` | Idempotent — second call is a no-op. Loads config, builds `KvsState`, starts the WAL writer and replication service on a fresh Tokio runtime. |
| `get_db_id_by_name`     | `getDbIdByName`       | `(name: string) => number \| null` | Look up a configured database's numeric id. |
| `query`                 | `query`               | `(dbId: number, sql: string, params: any[]) => QueryResult` | Runs one SQL statement. See §6 for `QueryResult` shape and §7 for parameter conversion rules. Throws a JS error (via `napi::Error`) on parse/exec failure. |
| `put`                   | `put`                 | `(dbId: number, table: string, key: Buffer, value: Buffer) => void` | Raw KV write (bypasses SQL/schema entirely). Also queues the write to the WAL. |
| `get`                   | `get`                 | `(dbId: number, table: string, key: Buffer) => Buffer \| null` | Raw KV read. |
| `remove`                | `remove`              | `(dbId: number, table: string, key: Buffer) => void` | Raw KV delete. Also queues to WAL. |
| `flush`                 | `flush`               | `() => void` | Forces the WAL writer to write+fsync its current batch immediately. |
| `get_config`            | `getConfig`           | `() => object` | Returns the loaded config as JSON (or a `Config::default()` if not initialized from the default path). Mostly useful for debugging. |

Key implementation details:
- `key`/`value`/return values for `put`/`get`/`remove` are **`napi::bindgen_prelude::Buffer`**,
  which maps to a JS `Buffer`, not a plain array. Always pass real `Buffer`
  instances from JS (`Buffer.from(...)`), not `number[]`.
- Errors are surfaced as real JS `Error` objects (`napi::Error` via
  `Error::from_reason(...)`) — a `to_js_err()` helper in `lib.rs` converts any
  `Display`-able Rust error (including `SkvsError`) into one.
- State is stored in `static OnceLock<Arc<KvsState>>` etc. — this means
  **one `KvsState`/WAL writer/replication service per process**, initialized
  exactly once by the first `init()` call. There is no way to run two
  independent instances in the same Node process.

---

## 6. `QueryResult` shape

`src/sql/mod.rs`:

```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,             // Row = IndexMap<String, Value>
    pub affected_rows: Option<u64>, // Some(n) for INSERT/UPDATE/DELETE, None for SELECT/DDL
}
```

Serialized to JSON, a `Value` is tagged by variant name, e.g.
`{"Integer": 30}`, `{"Text": "Alice"}`, `{"Blob": "<base64 in json.rs helpers, raw bytes elsewhere>"}`, `null` maps to... actually `Value::Null` serializes as the string `"Null"` by default serde derive (unit variant) — **check this if you rely on it**, and prefer testing empirically since serde's default enum representation for unit variants is a bare string.

Example real response (from a working smoke test):
```json
{
  "affected_rows": null,
  "columns": ["*"],
  "rows": [
    {"id": {"Integer": 1}, "name": {"Text": "Alice"}, "age": {"Integer": 31}}
  ]
}
```

`columns` for a `SELECT *` is currently just `["*"]` (the projection is not
expanded to the real column list) — don't rely on it for column names, use
the keys of each row object instead.

---

## 7. HTTP API — `server.js`

Express app. Every route requires:
1. Client IP present in `config.http.trusted_ips` (checked with
   `ip-range-check`, so both plain IPs and CIDR ranges work), **and**
2. Header `x-api-key: <config.http.secret_key>`.

Failing either returns `403 {"error": "..."}`. No other auth exists (no
per-user accounts, no TLS termination — put this behind a reverse proxy /
VPN for anything beyond a local network).

| Method & path | Body | Behavior |
|---|---|---|
| `POST /api/db/:dbName/query` | `{ "sql": string, "params"?: any[] }` | Runs the SQL, returns the `QueryResult` JSON. `params` are plain JS values (numbers/strings/null/booleans); `Buffer` values are base64-encoded to a string before being sent to Rust (`Value::from_json` will store base64 strings as `Value::Text`, **not** as `Value::Blob** — see §8 note on binary params). |
| `PUT /api/db/:dbName/table/:tableName/row/:rowKey` | `{ "value": string }` (base64, or falls back to utf8 if base64-decoding throws) | Raw KV put. `rowKey` is taken as a UTF-8 string and turned into bytes — you cannot address arbitrary binary keys through this route, only through the native API directly. |
| `GET /api/db/:dbName/table/:tableName/row/:rowKey` | — | Raw KV get. Response: `{"value": "<base64>"}` or `404`. |
| `DELETE /api/db/:dbName/table/:tableName/row/:rowKey` | — | Raw KV delete. |
| `POST /api/flush` | — | Forces a WAL flush. |

`getDbId(name)` caches name→id lookups in a `Map` for the process lifetime
(fine, since the DB list is static after `init()`).

---

## 8. Known gaps / things that are stubs (read before extending)

Be explicit with yourself and the user about these — don't assume they work:

1. **No WAL replay on startup.** `wal.rs` writes append-only batches to
   `{base_dir}/db_{id}/wal.bin` (bincode-encoded `Vec<Operation>` per batch,
   flushed on `flush_interval_secs` or when `batch_size` is reached, or via
   explicit `flush()`), but `init()` never reads that file back into
   `KvsState` on startup. **Data does not survive a process restart yet.**
   This is the single most valuable "make it actually work like a database"
   improvement to make next: read `{base_dir}/db_{id}/wal.bin`, deserialize
   each batch, and replay `OpType::Put`/`Delete` into `raw_stores` (and, if
   you extend the WAL to log SQL-level row changes too, into `dbs`).
2. **Transactions are not wired up.** `src/transaction.rs::Transaction` has
   `begin`/`commit`/`rollback`, but nothing in `sql::mod::SqlEngine` ever
   constructs one — `tx_id: Option<u64>` is threaded through
   `execute_insert/update/delete` and into `fire_triggers` purely so trigger
   code has a transaction id to log against, but there's no atomicity or
   rollback behavior. Treat every statement as auto-committed immediately.
3. **The query planner always does a full table scan.** Indexes
   (`index.rs`) are correctly maintained on every write, but `select.rs`
   never calls `lookup_index`. For anything beyond toy datasets, wiring
   equality/range predicates in `evaluate_where_ctx`/`execute_select` to
   check for a matching index first would be the highest-value follow-up
   after WAL replay.
4. **JOINs only support `INNER JOIN ... ON` and `CROSS JOIN`.** `LEFT`/`RIGHT`/`FULL
   OUTER JOIN` currently silently fall through to doing nothing extra (see the
   catch-all `_ =>` arm in `execute_joins`) — the join effectively becomes a
   cross join filtered by nothing, which is almost certainly not what you
   want. Fix by tracking unmatched left rows and padding with NULLs.
5. **`ALTER TABLE` is a no-op stub** (`ddl::alter_table`) — parses but does
   nothing. Needs real add/drop/rename-column logic against `TableSchema`
   and existing rows.
6. **`CHECK` constraints and `AUTO_INCREMENT`/auto-generated PKs are parsed
   but not enforced.** `ColumnDef.check_expr` is stored as a string but never
   evaluated in `constraint.rs`. Auto-increment PK generation is not tied to
   `get_next_rowid` — inserts require explicit values for all columns.
7. **JSON helpers (`json.rs`) are not exposed as SQL functions.** They exist
   as free Rust functions (`json_extract`, `json_array`, `json_object`,
   `json_set`) but nothing in `sql/select.rs`/`dml.rs` calls them from a
   parsed `Expr::Function`. To use them from SQL you'd extend
   `eval_expr_to_value`/`evaluate_aggregate` to recognize `json_extract(...)`
   etc. the same way `fts_match(...)` is already special-cased in
   `evaluate_where_ctx`.
8. **FTS (`fts.rs`) is invoked only via the special `fts_match(table, 'query')`
   function inside a WHERE clause** (see `evaluate_where_ctx` in
   `sql/select.rs`) — there is no `CREATE VIRTUAL TABLE ... USING fts` SQL
   syntax wired up; you currently have to call
   `state.register_fts_table(db_id, name, Arc::new(FtsVirtualTable::new()))`
   from Rust and populate/index it yourself. Read `src/fts.rs` for its exact
   API before using it.
9. **Binary parameters over HTTP are lossy.** `server.js` base64-encodes
   `Buffer` params before sending them to `query()`, but
   `Value::from_json` (in `schema.rs`) turns a JSON string into
   `Value::Text`, **not** `Value::Blob`. If you need true BLOB parameters
   over HTTP, either (a) add a tagged-value convention
   (`{"__blob__": "<base64>"}`) that both `server.js` and
   `Value::from_json` understand, or (b) use the native API directly with
   real `Buffer`s via `put`/raw tables instead of SQL params.
10. **Replication is fire-and-forget UDP with no ack, retry, or ordering
    guarantee**, matching the original design goal ("eventual consistency"),
    not a consensus protocol. Don't expect strong consistency across two
    servers, and don't add a third peer without redesigning `config.rs`'s
    single-`peer` model (`PeerConfig` is a single `Option<PeerConfig>`, not a
    list).
11. **No authentication beyond a single shared secret + IP allow-list.**
    There is no per-key ACL, no TLS, no rate limiting.

None of the above prevented the smoke tests below from passing — the "happy
path" for a single table, single database, single server works.

---

## 9. Build/toolchain notes

`Cargo.toml` pins several dependency versions tightly. This was done to get
the crate compiling under an old system Rust (1.75) inside a sandbox; **on a
modern toolchain (1.8x+, which is what you should use for real development)
you can likely loosen these back to caret ranges** (`"1"`, `"2"`, etc.) if you
want the latest patch releases. If you do loosen them, the ones most likely
to cause fresh breakage again are:

- `sqlparser` — **do not casually bump this**. The whole `src/sql/*` module
  is written against the exact AST shape of **0.40.0**. Newer sqlparser
  versions (0.4x+) have changed `Statement::Insert`/`Update`/`Delete` field
  names and `Query`/`Select` structure before; bumping this requires
  re-auditing every `match stmt { Statement::... }` in `sql/ddl.rs`,
  `sql/dml.rs`, and `sql/select.rs` against the new AST (check
  `~/.cargo/registry/src/*/sqlparser-<version>/src/ast/mod.rs` and
  `.../src/ast/query.rs` directly — that's the ground truth, not memory of
  older API shapes).
- `napi` / `napi-derive` / `napi-derive-backend` — these three **must be a
  mutually compatible trio**. `napi-derive` 2.14.0 requires `syn 1.x`
  internally, and pairs with `napi-derive-backend` around `1.0.53`–`1.0.55`
  (NOT `1.0.60`, which switched to `syn 2` internally and produces
  `syn::Type` vs `syn::ty::Type` mismatch errors when paired with
  `napi-derive` 2.14.0). If you bump `napi` to a much newer 2.x/3.x, bump all
  three together to versions released around the same time, not
  independently.
- `indexmap` — code uses `#[serde(with = "indexmap::map::serde_seq")]` (note:
  `map::serde_seq`, not the bare `indexmap::serde_seq` path used in some
  older docs/examples).

To rebuild cleanly:
```bash
rm -f Cargo.lock
cargo build --release
```

---

## 10. How to extend this safely (checklist for an AI agent)

1. Read this file, then the specific `.rs` file(s) you're touching, in full
   — many functions here are short and self-contained; don't guess field
   names or enum variants, `grep`/`view` the actual struct/enum definitions
   in `schema.rs`/`state.rs`/`config.rs` first.
2. If touching anything in `src/sql/`, cross-check the relevant
   `sqlparser::ast` type in the local registry checkout
   (`~/.cargo/registry/src/*/sqlparser-0.40.0/src/ast/`) rather than
   remembering an API shape — sqlparser's AST has changed substantially
   across versions and it's easy to write code for the wrong version.
3. Keep `SkvsError` as the one error type for internal Rust code; convert to
   `napi::Error` only at the `#[napi]` boundary in `lib.rs` (via
   `to_js_err`), and to HTTP JSON only in `server.js`'s `catch` blocks.
4. If you add a new `#[napi]` function, remember JS callers must use its
   camelCase form.
5. After any change, rebuild (`cargo build --release`), copy the artifact to
   `skvs.node`, and run a smoke test (see `test_skvs.py` for the shape of a
   Python-based test, or write a quick Node script that requires
   `./skvs.node` directly and calls `init`/`query`/`put`/`get`). Confirm
   `cargo build --release` produces **zero errors** (warnings for genuinely
   dead/unused stub code — `transaction.rs`, unused `json.rs` helpers,
   `lookup_index`, etc. — are expected and fine).
6. Prefer fixing the gaps in §8 in this order if asked for "make it more
   production ready": (1) WAL replay on startup, (2) index-aware query
   planning, (3) proper OUTER JOIN semantics, (4) real transactions, (5)
   JSON/FTS SQL function wiring, (6) CHECK/auto-increment enforcement.
