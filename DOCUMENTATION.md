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
- **Persistence**: an append-only write-ahead log (WAL) per database
  (`src/wal.rs`) for the raw op-log/replication feed, **plus** a periodic
  full snapshot (SQL tables + schemas, raw KV tables, views, and triggers)
  written to `{base_dir}/db_{id}/{tables,raw,views,triggers}.bin` on the same
  timer. `init()` loads that snapshot back into `KvsState` on startup and
  rebuilds any fts5 index from the restored rows — **all of the above now
  survives a process restart** (see §8 for exactly what still doesn't).
- **Transactions**: an undo-journal based `BEGIN`/`COMMIT`/`ROLLBACK`
  (`src/transaction.rs`, `KvsState.txns`). Writes still apply immediately;
  a transaction just remembers how to undo them. Every autocommit
  INSERT/UPDATE/DELETE (including ones a trigger cascades into) also gets
  wrapped in a short-lived internal transaction for statement-level
  atomicity, even without an explicit `BEGIN`. See §8 for what this does
  and doesn't give you (no isolation between concurrent transactions).
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
    ├── wal.rs                # WalWriter: async batched WAL writer + full-state snapshot save/load
    ├── replication.rs        # ReplicationService: UDP send/receive of Operation batches
    ├── transaction.rs        # TxnManager: real undo-journal BEGIN/COMMIT/ROLLBACK (see §8)
    ├── expr.rs                # eval()/count_placeholders(): the ONE shared scalar-expression
    │                           #   evaluator - arithmetic, CASE, LIKE, BETWEEN, IN, JSON/string
    │                           #   functions, etc. Used by dml.rs, select.rs, and constraint.rs
    │                           #   alike so WHERE/SET/projection/CHECK all agree on semantics.
    ├── constraint.rs          # validate_constraints(): NOT NULL / UNIQUE / PK / FK / CHECK, all enforced
    ├── trigger.rs              # fire_triggers(): BEFORE/AFTER INSERT/UPDATE/DELETE trigger execution
    ├── index.rs                # update_indexes()/lookup_index(): secondary index maintenance,
    │                           #   now also consulted by SELECT/UPDATE/DELETE for simple equality lookups
    ├── view.rs                  # create_view/drop_view/resolve_view — views are just stored SELECT text
    ├── json.rs                  # json_extract/json_array/json_object/json_set — wired into SQL via expr.rs
    ├── virtual_table.rs        # VirtualTable trait + VirtualTableRegistry (used by fts.rs)
    ├── fts.rs                   # FtsVirtualTable — simple inverted-index full text search
    └── sql/
        ├── mod.rs               # SqlEngine::execute() — parses SQL, dispatches by Statement variant,
        │                        #   handles BEGIN/COMMIT/ROLLBACK and wraps autocommit DML in a
        │                        #   short-lived transaction for atomicity
        ├── ddl.rs                # CREATE/DROP TABLE, real ALTER TABLE, CREATE/DROP INDEX, VIEW,
        │                        #   CREATE VIRTUAL TABLE ... USING fts5
        ├── dml.rs                # INSERT (multi-row, INSERT...SELECT) / UPDATE / DELETE, journaling
        └── select.rs             # SELECT: FROM/JOIN (incl. LEFT/RIGHT/FULL OUTER)/WHERE/GROUP BY/
                                   #   ORDER BY/LIMIT/DISTINCT
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
    pub auto_increment: bool,          // parsed from AUTOINCREMENT/AUTO_INCREMENT; note that an
                                        // omitted `INTEGER PRIMARY KEY` value auto-fills from the
                                        // rowid counter regardless of this flag (SQLite rowid-alias
                                        // behavior) - this field is metadata, not what triggers that.
    pub not_null: bool,
    pub default: Option<Value>,        // the actual literal, evaluated once at DDL time
    pub unique: bool,
    pub check_expr: Option<String>,    // enforced on every INSERT/UPDATE (see constraint.rs)
}

pub struct TableSchema {
    pub name: String,
    pub columns: IndexMap<String, ColumnDef>,
    pub rowid_column: Option<String>,   // name of the (first) PK column, if any
    pub foreign_keys: Vec<ForeignKeyDef>,
    pub indices: Vec<IndexDef>,
    pub triggers: Vec<TriggerDef>,      // NOTE: triggers actually live in KvsState.triggers, not here
    pub unique_groups: Vec<UniqueGroup>, // table-level PRIMARY KEY(...)/UNIQUE(...) (composite keys)
    pub table_checks: Vec<String>,       // table-level CHECK(...) expressions
    pub fts5_content_column: Option<String>, // Some(col) for a CREATE VIRTUAL TABLE ... USING fts5 table
}

pub struct UniqueGroup { pub columns: Vec<String>, pub is_primary: bool }
pub struct IndexDef { pub name: String, pub columns: Vec<String>, pub unique: bool, .. }
pub struct ForeignKeyDef { pub column: String, pub ref_table: String, pub ref_column: String, on_delete: FkAction, on_update: FkAction }
pub enum FkAction { NoAction, Cascade, SetNull, Restrict, SetDefault }
pub enum TriggerTiming { Before, After, InsteadOf }
pub enum TriggerEvent { Insert, Update, Delete }
pub struct TriggerDef { .. }   // see src/trigger.rs for exact fields
```

`Value::compare()` (used for `ORDER BY`, `>`, `<`, `BETWEEN`, etc.) compares
`Integer`/`Real` numerically across the two variants (so a `REAL` column
compared against an integer literal, e.g. `WHERE price > 100`, works
correctly) and treats `NULL` as less than everything else; genuinely
different types (e.g. text vs. integer) fall back to a fixed type-rank
ordering so the comparison is at least total/stable. `Value` deliberately
does **not** implement `Eq`/`Hash` (because of the `f64` field), so anything
that needs to key a `HashMap` by `Value` (e.g. `GROUP BY`) uses a
`format!("{:?}", value)` string key instead — see `sql/select.rs::apply_group_by`.

Scalar SQL expressions (arithmetic, `CASE`, string functions, `json_extract`,
comparisons, `LIKE`/`BETWEEN`/`IN`, ...) are evaluated by the single shared
`src/expr.rs::eval()` function, used identically by `WHERE`, `SET`,
`SELECT`-list projection, `ORDER BY`/`GROUP BY` keys, and `CHECK` constraint
evaluation. There used to be three separate, mutually-inconsistent
expression evaluators (one in `dml.rs`, two in `select.rs`) — if you're
adding a new function or operator, add it **once**, in `expr.rs`, and every
caller gets it.

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
| `txns`               | `TxnManager`                                                    | active transaction id → undo journal (see §8) |
| `db_name_to_id`      | `HashMap<String, u32>`                                        | name → id lookup, built once at `init()` |

`KvsState::new(&config.databases)` pre-creates empty maps for every `[[databases]]`
entry in the config. **Databases cannot currently be added at runtime** —
they must all be declared in `config.toml` before `init()` is called.

Secondary indexes (`src/index.rs`) are implemented on top of `raw_stores`
under the reserved table name `"__indexes__"`, keyed by
`"{table}:{index_name}:{value-as-string}"` → a bincode-encoded `Vec<RowId>`.
This is intentionally simple (single-column indexes only). The index
machinery is correctly kept up to date on INSERT/UPDATE/DELETE, and is now
consulted for two things: (1) `SELECT`/`UPDATE`/`DELETE` when the entire
`WHERE` clause is exactly `col = <literal-or-param>` and `col` has a
matching index (see `select.rs::rows_for_simple_where`), and (2) UNIQUE/
PRIMARY KEY constraint checks on a single indexed column. Anything more
complex than a single top-level equality (compound `AND`/`OR` trees, range
predicates, multi-column indexes used for partial prefixes, etc.) still
falls back to a full table scan — this is a targeted optimization for the
common point-lookup case, not a general query planner/optimizer.

---

## 5. Native (napi) API — `src/lib.rs`

All of these are called from JS as `require('./skvs.node').<camelCaseName>(...)`.

| Rust fn (snake_case)     | JS name (camelCase)   | Signature | Notes |
|--------------------------|------------------------|-----------|-------|
| `init`                  | `init`                | `(configPath?: string) => void` | Idempotent — second call is a no-op. Loads config, builds `KvsState`, restores the last snapshot (see §1/§8), starts the WAL writer and replication service on a fresh Tokio runtime. |
| `get_db_id_by_name`     | `getDbIdByName`       | `(name: string) => number \| null` | Look up a configured database's numeric id. |
| `query`                 | `query`               | `(dbId: number, sql: string, params: any[], txId?: number) => QueryResult` | Runs one SQL statement. Pass `txId` (from a prior `BEGIN`/`beginTransaction`) to make this statement part of that transaction; omit it (or pass `null`/`undefined`) to auto-commit. See §6 for `QueryResult` shape and §7 for parameter conversion rules. Throws a JS error (via `napi::Error`) on parse/exec failure. |
| `begin_transaction`     | `beginTransaction`    | `(dbId: number) => number` | Starts a transaction, returns its id. Equivalent to `query(dbId, "BEGIN", [])` and reading `.tx_id` off the result — this is just a more convenient direct entry point. |
| `commit_transaction`    | `commitTransaction`   | `(txId: number) => void` | Ends the transaction; every write made under it stays applied (it already was — see §8 on how transactions actually work here). |
| `rollback_transaction`  | `rollbackTransaction` | `(txId: number) => void` | Undoes every change made under `txId`, in reverse order, including anything a trigger cascaded into. |
| `put`                   | `put`                 | `(dbId: number, table: string, key: Buffer, value: Buffer) => void` | Raw KV write (bypasses SQL/schema entirely). Also queues the write to the WAL and is included in the periodic full snapshot. |
| `get`                   | `get`                 | `(dbId: number, table: string, key: Buffer) => Buffer \| null` | Raw KV read. |
| `remove`                | `remove`              | `(dbId: number, table: string, key: Buffer) => void` | Raw KV delete. Also queues to WAL. |
| `flush`                 | `flush`               | `() => void` | Forces the WAL writer to write+fsync its current batch immediately (does not by itself force the periodic full snapshot — that's on its own timer, see `wal.rs::save_sql_snapshot`). |
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
    pub tx_id: Option<u32>,         // Some(id) only for a `BEGIN` result - see §8 on transactions
}
```

The napi `query()` binding does **not** return `QueryResult`'s derived
`Serialize` output directly — `lib.rs::query()` builds the JSON by hand,
turning each `Value` into a plain JSON scalar via `Value::to_json()`
(`Integer`/`Real` → JSON number, `Text` → JSON string, `Blob` → base64
string, `Null` → JSON `null`), not an externally-tagged enum. So a row comes
back over the native API / HTTP as e.g. `{"id": 1, "name": "Alice"}`, not
`{"id": {"Integer": 1}, ...}` — the tagged form only shows up if you
serialize a `QueryResult`/`Value` some other way (e.g. inside the bincode
snapshot files, or if you add a new code path that uses `serde_json::to_value`
on a `Value` directly instead of `.to_json()`).

Example real response (from a working smoke test):
```json
{
  "affected_rows": null,
  "columns": ["id", "name", "age"],
  "rows": [{"id": 1, "name": "Alice", "age": 31}],
  "tx_id": null
}
```

`columns` reflects the real output column names, including for `SELECT *`
(and a mix like `SELECT id, *`) — it's read off the actual result rows after
projection, falling back to the table's schema only when the query matched
zero rows and there's nothing to read column names off of.

---

## 6a. Transactions

`BEGIN`/`COMMIT`/`ROLLBACK` (as literal SQL text) and the dedicated
`begin_transaction`/`commit_transaction`/`rollback_transaction` napi
functions are backed by `src/transaction.rs::TxnManager`, held at
`KvsState.txns`. The design is an **undo journal, not MVCC/snapshot
isolation**:

- Writes are applied to the live tables immediately, exactly like an
  autocommit statement. `BEGIN` just starts recording an undo entry (old row
  value, or "this rowid didn't exist before") for every subsequent
  INSERT/UPDATE/DELETE tagged with that transaction's id — including ones a
  trigger cascades into, since triggers execute through the same
  `tx_id`-threaded call path.
- `COMMIT` simply discards the journal — the writes already happened.
- `ROLLBACK` replays the journal in reverse, restoring each touched row
  (and keeping secondary indexes / any fts5 index in sync as it goes).
- **There is no isolation between concurrent transactions.** Because writes
  apply immediately rather than to a private snapshot, a second connection
  (or a second `tx_id`) sees an in-progress transaction's uncommitted writes
  right away, same as if it were already committed. If you need real
  isolation (e.g. read-committed/snapshot semantics, or blocking a second
  writer from touching a row an open transaction has touched), that's a
  substantially bigger change (row/table locking or an MVCC layer) — not
  present here.
- Every **autocommit** INSERT/UPDATE/DELETE (i.e. no explicit `BEGIN`) is
  *also* wrapped in a short-lived internal transaction by
  `SqlEngine::execute` purely for atomicity: if a multi-row `INSERT`, or an
  `UPDATE` that fires a trigger which itself fails, dies partway through,
  everything it already applied gets rolled back automatically. `SELECT`
  and DDL statements skip this wrapper (nothing to roll back / not worth
  the overhead).

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
| `POST /api/db/:dbName/query` | `{ "sql": string, "params"?: any[], "txId"?: number }` | Runs the SQL, returns the `QueryResult` JSON (including `tx_id`, set only by a `BEGIN`). `params` are plain JS values (numbers/strings/null/booleans); a `Buffer` value is sent as a tagged `{"__blob__": "<base64>"}` object, which `Value::from_json` on the Rust side decodes into a real `Value::Blob` (a bare base64 *string* would decode as `Value::Text` instead — see §8). |
| `POST /api/db/:dbName/transaction/begin` | — | `{ "txId": number }`. Pass that id as `txId` on subsequent `/query` calls (including the eventual commit/rollback below, or a `COMMIT`/`ROLLBACK` `/query` call using the same `txId`). |
| `POST /api/db/:dbName/transaction/:txId/commit` | — | Commits. `204` on success. |
| `POST /api/db/:dbName/transaction/:txId/rollback` | — | Rolls back. `204` on success. |
| `PUT /api/db/:dbName/table/:tableName/row/:rowKey` | `{ "value": string }` (base64, or falls back to utf8 if base64-decoding throws) | Raw KV put. `rowKey` is taken as a UTF-8 string and turned into bytes — you cannot address arbitrary binary keys through this route, only through the native API directly. |
| `GET /api/db/:dbName/table/:tableName/row/:rowKey` | — | Raw KV get. Response: `{"value": "<base64>"}` or `404`. |
| `DELETE /api/db/:dbName/table/:tableName/row/:rowKey` | — | Raw KV delete. |
| `POST /api/flush` | — | Forces a WAL batch flush. |

`getDbId(name)` caches name→id lookups in a `Map` for the process lifetime
(fine, since the DB list is static after `init()`).

---

## 8. Known gaps / things to be aware of (read before extending)

Be explicit with yourself and the user about these — don't assume they work:

1. **Transactions give atomicity + durability-of-intent, not isolation.**
   See §6a. Concurrent transactions see each other's uncommitted writes
   immediately; there's no locking or MVCC. Fine for a single-writer-at-a-time
   workload or advisory use; not a substitute for real isolation levels if
   you need them.
2. **The query planner only speeds up a single top-level equality
   predicate.** `SELECT`/`UPDATE`/`DELETE` use a matching index when the
   *entire* `WHERE` clause is exactly `col = <literal-or-param>` (see
   `select.rs::rows_for_simple_where`/`simple_equality`). Anything more
   (`AND`-chains, ranges, `OR`, multi-column indexes used for a prefix
   match) falls back to a full table scan — correct, just not fast on large
   tables. Extending `simple_equality` to recurse through top-level `AND`
   and gather multiple equality predicates (intersecting the candidate
   rowid sets) is the natural next step here.
3. **Composite/multi-column foreign keys and indexes aren't supported** —
   `ForeignKeyDef`/`IndexDef` each carry a single column
   (`index.rs::index_value` explicitly takes `idx.columns.first()`).
   Multi-column `UNIQUE`/`PRIMARY KEY` **is** supported (`TableSchema.unique_groups`),
   just not multi-column FKs or indexes.
4. **`ALTER TABLE ... ADD CONSTRAINT` / `DROP CONSTRAINT` aren't
   implemented** (`ddl::alter_table`'s catch-all `_ =>` returns an
   `Unsupported` error for anything beyond ADD/DROP/RENAME COLUMN and RENAME
   TO). Adding those means growing `TableSchema` mutation logic for
   `unique_groups`/`foreign_keys`/`table_checks`, matching what
   `create_table` already does for the initial `CREATE TABLE` parse.
5. **`fts.rs`'s `search_with_rank` (BM25-ish scoring) is unused** — only
   `fts_match(table, 'query')`'s plain unranked `search()` is wired into
   `WHERE`. There's no `ORDER BY rank`/relevance sorting for FTS results.
6. **FTS is invoked only via the special `fts_match(table, 'query')`
   function inside a `WHERE` clause** (`select.rs::evaluate_where_ctx`) —
   there's no `MATCH` operator syntax. `CREATE VIRTUAL TABLE ... USING
   fts5(col, ...)` IS wired up (`ddl::create_virtual_table`) and the index
   is correctly kept in sync on INSERT/UPDATE/DELETE (including removing
   stale tokens on UPDATE, not just adding new ones) and rebuilt from row
   content after a restart.
7. **Replication is fire-and-forget UDP with no ack, retry, or ordering
   guarantee**, matching the original design goal ("eventual consistency"),
   not a consensus protocol. Don't expect strong consistency across two
   servers, and don't add a third peer without redesigning `config.rs`'s
   single-`peer` model (`PeerConfig` is a single `Option<PeerConfig>`, not a
   list).
8. **No authentication beyond a single shared secret + IP allow-list.**
   There is no per-key ACL, no TLS, no rate limiting.
9. **`INSERT ... SELECT` maps source columns to target columns
   positionally**, in the SELECT's projection order — same as standard SQL,
   but worth remembering if the two tables' column orders don't line up the
   way you expect.
10. **The `simple_equality`/index-lookup fast path and the `CHECK`-constraint
    re-parser (`constraint.rs::eval_check`) both re-parse small bits of SQL
    text on every call** (`table_checks`/`check_expr` are stored as rendered
    SQL text, not a parsed AST, to keep `TableSchema` - and its bincode
    snapshot format - plain data). Fine for the scale this is built for; if
    profiling ever shows this hot, cache the parsed `Expr` alongside the
    string instead of changing the stored format.

None of the above prevented `cargo test` (19 tests) or the full HTTP-level
`test_skvs.py` smoke suite (9 tests) from passing — see §9.

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
3. If you're adding a new operator/function to any scalar expression
   context (WHERE, SET, projection, CHECK, ORDER/GROUP BY key), add it once
   in `src/expr.rs::eval()` — and mirror the change in
   `count_placeholders()` if the new node type can contain a `?`
   placeholder, or UPDATE's SET/WHERE param-offset split
   (`sql/dml.rs::execute_update`) will silently misalign. Don't add another
   parallel evaluator in `dml.rs`/`select.rs`.
4. Keep `SkvsError` as the one error type for internal Rust code; convert to
   `napi::Error` only at the `#[napi]` boundary in `lib.rs` (via
   `to_js_err`), and to HTTP JSON only in `server.js`'s `catch` blocks.
5. If you add a new `#[napi]` function, remember JS callers must use its
   camelCase form.
6. After any change, rebuild (`cargo build --release`), copy the artifact to
   `skvs.node`, and run: `cargo test --release` (unit tests, fast, no server
   needed), then start the server against a throwaway `SKVS_CONFIG` pointing
   at a scratch `storage.base_dir` and run `python3 test_skvs.py` against it
   for an end-to-end HTTP-level check. Confirm `cargo build --release`
   produces **zero errors** (a handful of dead-code warnings for genuinely
   unused stub infrastructure — `virtual_table.rs`'s generic registry,
   `TxnManager::is_active`, etc. — are expected and fine).
7. If asked to make this "more production ready" further, the highest-value
   remaining items are, roughly in order: (1) extend the index-aware lookup
   in `select.rs` to handle top-level `AND`-chains of equalities, not just a
   single one; (2) multi-column indexes/foreign keys; (3) real
   isolation between concurrent transactions (currently atomicity/durability
   only - see §6a/§8.1) if concurrent writers are a real requirement; (4)
   `ALTER TABLE ADD/DROP CONSTRAINT`; (5) FTS ranking (`search_with_rank`
   exists but isn't wired to `ORDER BY`).
