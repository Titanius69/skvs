# SKVS — Super Key-Value Store

A fast, embedded, multi-database SQL engine written in **Rust**, exposed to **Node.js** via
[napi-rs](https://napi.rs), and served over a small **Express HTTP API**.

Think "SQLite, but multi-tenant, in-memory-first, with a write-ahead log and
peer-to-peer replication, and a plain HTTP interface instead of a file format."

- 🦀 Core storage + SQL engine in Rust (`DashMap`-backed, lock-free reads)
- 🧠 Multiple independent databases ("namespaces") per server instance, defined in `config.toml`
- 📝 SQL subset: `CREATE/DROP TABLE`, `CREATE/DROP INDEX`, `CREATE/DROP VIEW`,
  `INSERT`, `UPDATE`, `DELETE`, `SELECT` (`WHERE`, `JOIN`, `GROUP BY`, `ORDER BY`,
  `LIMIT`/`OFFSET`, `DISTINCT`, aggregates)
- 🔑 Raw key/value API alongside SQL (`put`/`get`/`remove`) for cases where you
  don't need a schema
- 🧾 Write-ahead log (WAL) with batched, periodic `fsync`
- 🔁 Peer-to-peer UDP replication between two servers (eventual consistency)
- 🌐 HTTP API (Node.js/Express) protected by an IP allow-list + shared secret key
- ⚙️ Triggers, constraints (FK/unique/check), JSON helpers, and a simple FTS
  (full-text search) virtual table

> ⚠️ **Status:** this is a hobby/learning project, not a production database.
> It has been fixed up to compile and pass basic smoke tests (see
> [DOCUMENTATION.md](./DOCUMENTATION.md) for exactly what's implemented and
> what's still a stub). Read that file before relying on any given feature.

---

## Quick start

### 1. Prerequisites

- Rust (stable, recent — 1.75 is too old for some transitive deps; 1.8x+ recommended)
- Node.js 18+ and npm
- `napi-cli` is **not required** to build — plain `cargo build --release` produces
  a `.so`/`.dll`/`.dylib` that you rename to `skvs.node` (see below).

### 2. Build the native addon

```bash
cargo build --release
# Linux:
cp target/release/libskvs.so ./skvs.node
# macOS:
cp target/release/libskvs.dylib ./skvs.node
# Windows:
copy target\release\skvs.dll skvs.node
```

(If you have `@napi-rs/cli` installed, `npm run build` does this for you via
`cargo napi build --release`.)

### 3. Configure

Copy `config.toml` and edit it — at minimum set `storage.base_dir` to a
writable directory and change `http.secret_key`:

```toml
[server]
id = 1
replication_port = 9999

[peer]
address = "192.168.1.10:9999"   # the OTHER server's replication address

[storage]
base_dir = "/var/lib/skvs"

[wal]
flush_interval_secs = 1
batch_size = 1000

[http]
port = 3000
trusted_ips = ["127.0.0.1", "::1", "192.168.1.0/24"]
secret_key = "change-me-to-something-long-and-random"

[[databases]]
id = 0
name = "default"

[[databases]]
id = 1
name = "user_data"
```

### 4. Install Node dependencies and run

```bash
npm install
SKVS_CONFIG=/path/to/config.toml npm start
```

### 5. Talk to it

```bash
curl -X POST http://127.0.0.1:3000/api/db/default/query \
  -H "Content-Type: application/json" \
  -H "x-api-key: change-me-to-something-long-and-random" \
  -d '{"sql": "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"}'

curl -X POST http://127.0.0.1:3000/api/db/default/query \
  -H "Content-Type: application/json" \
  -H "x-api-key: change-me-to-something-long-and-random" \
  -d '{"sql": "INSERT INTO users (id, name, age) VALUES (?, ?, ?)", "params": [1, "Alice", 30]}'

curl -X POST http://127.0.0.1:3000/api/db/default/query \
  -H "Content-Type: application/json" \
  -H "x-api-key: change-me-to-something-long-and-random" \
  -d '{"sql": "SELECT * FROM users WHERE age > 18"}'
```

Every request needs both:
- a source IP that matches an entry in `http.trusted_ips` (single IPs or CIDR ranges), **and**
- an `x-api-key` header equal to `http.secret_key`.

See [DOCUMENTATION.md](./DOCUMENTATION.md) for the full HTTP API reference,
the exact SQL surface that's supported, the JS/native API, and the internal
architecture — written so you can hand it to another AI assistant (or a new
contributor) and get useful, accurate help extending this project.

## Project layout

```
skvs/
├── Cargo.toml           # Rust crate manifest (napi cdylib)
├── build.rs              # napi build script
├── src/
│   ├── lib.rs             # napi bindings exposed to Node (init/query/put/get/...)
│   ├── config.rs          # config.toml loader
│   ├── state.rs           # in-memory state: DashMap-based multi-DB storage
│   ├── schema.rs          # Value/Row/TableSchema/ColumnDef/IndexDef/TriggerDef types
│   ├── error.rs           # SkvsError (thiserror)
│   ├── wal.rs             # write-ahead log writer (batched, async)
│   ├── replication.rs     # UDP peer-to-peer replication
│   ├── transaction.rs     # (stub) transaction journal
│   ├── constraint.rs      # PK/unique/FK/NOT NULL/CHECK validation
│   ├── trigger.rs         # BEFORE/AFTER INSERT/UPDATE/DELETE triggers
│   ├── index.rs           # secondary index maintenance (backed by the raw KV store)
│   ├── view.rs             # CREATE/DROP VIEW support
│   ├── json.rs             # json_extract/json_array/json_object/json_set helpers
│   ├── virtual_table.rs   # virtual table trait + registry (used by FTS)
│   ├── fts.rs              # simple full-text search virtual table
│   └── sql/
│       ├── mod.rs          # SqlEngine::execute — statement dispatch
│       ├── ddl.rs          # CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, VIEW
│       ├── dml.rs          # INSERT / UPDATE / DELETE
│       └── select.rs       # SELECT: WHERE, JOIN, GROUP BY, ORDER BY, LIMIT
├── server.js               # Express HTTP API wrapping the native addon
├── package.json
├── config.toml             # example configuration
└── test_skvs.py            # (legacy) Python smoke test script
```

## License

Apache-2.0 (see `Cargo.toml`).
