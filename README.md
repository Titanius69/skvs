# SKVS — Super Key-Value Store

A fast, embedded, multi-database SQL engine written in **Rust**, exposed to **Node.js** via [napi-rs](https://napi.rs), and served through a small **Express HTTP API**.

Think of it as **"SQLite, but multi-tenant, in-memory-first, with a write-ahead log and peer-to-peer replication, exposed through HTTP instead of a traditional database file format."**

* 🦀 Core storage + SQL engine written in Rust
* 🧠 Multiple independent databases ("namespaces") per server instance
* 📝 SQL support including `CREATE/DROP/ALTER TABLE`, indexes, views, `INSERT` (multi-row, `INSERT...SELECT`), `UPDATE`, `DELETE`, `SELECT`, `WHERE`, `JOIN` (`INNER`/`LEFT`/`RIGHT`/`FULL OUTER`/`CROSS`), `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`, and aggregates
* ⚡ Indexed lookups use an in-memory `BTreeMap` per index — equality (`=`) *and* range (`<`, `<=`, `>`, `>=`, `BETWEEN`) predicates on an indexed column are both O(log n), not a full table scan
* ⚡ Equi-JOINs (`a.col = b.col`, `INNER`/`LEFT`) use a hash join (O(n+m)) instead of a nested loop (O(n·m)) when it's safe to; anything more complex falls back to the always-correct nested loop
* ⚡ Per-table row-id generation is a lock-free atomic counter, so concurrent inserts into the same table don't serialize on it
* 📊 Every query response carries a `timing` breakdown (`engine_us`/`params_us`/`encode_us`/`total_us`) straight from the Rust engine; `server.js` logs it per request alongside its own FFI/HTTP overhead — see [Performance & timing](#performance--timing) below
* 🔒 Enforced `NOT NULL`, `UNIQUE`, `PRIMARY KEY` (including composite), `FOREIGN KEY`, and `CHECK` constraints
* ↩️ Real `BEGIN`/`COMMIT`/`ROLLBACK` transactions (undo-journal based; every autocommit statement is also atomic on partial failure) — see [DOCUMENTATION.md §6a](./DOCUMENTATION.md#6a-transactions) for exactly what guarantees this does and doesn't give you
* 💾 SQL tables, raw key/value data, views, triggers, and secondary indexes all survive a process restart (periodic full-state snapshot, loaded back on startup; indexes are rebuilt from the restored rows rather than persisted separately)
* 🔑 Raw key/value API alongside SQL
* 🧾 Write-ahead log (WAL) with batched periodic `fsync`
* 🔁 Peer-to-peer UDP replication between two servers
* 🌐 HTTP API using Node.js + Express
* 🔐 IP allow-list and shared API secret authentication
* ⚙️ Triggers, constraints, JSON functions (`json_extract`/`json_array`/`json_object`/`json_set`), arithmetic/`CASE`/`LIKE`/`BETWEEN`/`IN` expressions, and a simple FTS5-style full-text search virtual table

> ⚠️ **Status:** This started as a hobby/learning project and has had a thorough correctness pass: constraint enforcement, real transactions, full-restart persistence, OUTER JOINs, a real index engine (equality + range, hash joins), and a number of previously-silent bugs (see [DOCUMENTATION.md §8](./DOCUMENTATION.md#8-known-gaps--things-to-be-aware-of-read-before-extending)) are now fixed and covered by tests (`cargo test`: 21 passing; `test_skvs.py` end-to-end HTTP suite: 9 passing). It is **still not a hardened, horizontally-scalable production database** — see DOCUMENTATION.md §8 for the specific remaining gaps (transaction isolation, query-planner scope beyond the fast paths described here, replication guarantees, auth model) before relying on it for anything with real stakes.
>
> See [DOCUMENTATION.md](./DOCUMENTATION.md) for the exact implementation status of individual features and known limitations.

---

## Performance & timing

Every response from `POST /api/db/:dbName/query` carries a `timing` object
straight from the Rust engine, and `server.js` logs one line per query to
the console with it:

```
[skvs] db=mydb sql="SELECT * FROM users WHERE age > 18" engine=42us params=1us encode=8us rust_total=53us ffi_http_overhead=120us http_total=173us
```

* **`engine_us`** — parse + plan + execute inside the Rust engine only, with
  no JSON/FFI marshaling. This is the number that reflects the database
  itself.
* **`params_us`** — converting the JS-supplied params into Rust `Value`s.
* **`encode_us`** — converting the Rust result rows back into JSON for the
  trip to Node.
* **`rust_total_us`** — the whole `query()` napi call (`params_us +
  engine_us + encode_us` + a little overhead).
* **`ffi_http_overhead`** — computed by `server.js`, not Rust: the gap
  between `http_total` (measured around the whole `skvs.query(...)` call
  from the JS side) and `rust_total`. This is the FFI boundary crossing
  itself plus Express/body-parser/JSON.stringify — usually the majority of
  a *fast* query's total latency once the engine itself is no longer the
  bottleneck.

What makes the engine fast for indexed access:

* A `WHERE col = value` or `WHERE col > value` / `BETWEEN` on a column with
  a `CREATE INDEX` is answered from an in-memory `BTreeMap` (O(log n))
  instead of scanning the table.
* `a JOIN b ON a.col = b.col` (`INNER`/`LEFT`) builds a hash index on one
  side once and probes it (O(n+m)) instead of comparing every row of `a`
  against every row of `b` (O(n·m)). Anything the fast path can't safely
  recognize (multi-table join chains, non-equality conditions, unqualified
  columns) still works — it just falls back to the nested loop.
* Row-id generation per table is a single lock-free atomic increment, so
  concurrent inserts into the same table aren't serialized behind it.

None of this changes query *results* — every fast path is purely an
optimization over the same full re-evaluation of the `WHERE`/`ON` clause, so
switching between the fast path and the fallback can never change what rows
come back.

---

## Quick Start

### 1. Requirements

You need:

* **Rust** stable toolchain
* **Node.js 18+**
* **npm**
* A C/C++ build environment supported by Rust on your operating system

You do **not** need to install `napi-cli` globally.

The project can use `napi-rs` through `npx`.

---

### 2. Build the native Node.js addon

Install the project dependencies:

```bash
npm install
```

Then build the native addon:

```bash
npx napi build --release
```

The resulting `.node` native module will be generated by `napi-rs`.

The native addon is **platform-specific**. A binary built on Windows cannot normally be used on Linux, and vice versa.

For example:

```text
Windows x64  → Windows native addon
Linux x64    → Linux native addon
macOS        → macOS native addon
```

If you distribute SKVS to multiple platforms, build the native addon separately for each target platform.

> **Note:** Do not rename a normal Rust `.dll`, `.so`, or `.dylib` manually unless the project is specifically configured for that workflow. `npx napi build --release` is the recommended build command for the Node.js addon.

---

### 3. Configure SKVS

Copy the example configuration and edit it as needed.

At minimum, configure:

* `storage.base_dir`
* `http.port`
* `http.secret_key`
* `http.trusted_ips`

Example:

```toml
[server]
id = 1
replication_port = 9999

[peer]
address = "192.168.1.10:9999"

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

On Windows, for example:

```toml
[storage]
base_dir = "C:/skvs/data"
```

---

### 4. Start the server

If `config.toml` is in the project directory, you can use:

```bash
npm start
```

Or specify a custom configuration file.

#### Linux / macOS

```bash
SKVS_CONFIG=/path/to/config.toml npm start
```

#### Windows CMD

```cmd
set SKVS_CONFIG=C:\path\to\config.toml && npm start
```

#### Windows PowerShell

```powershell
$env:SKVS_CONFIG="C:\path\to\config.toml"; npm start
```

The server will start on the port configured in `config.toml`.

---

## HTTP API

Every request requires:

1. A source IP matching an entry in `http.trusted_ips`
2. An `x-api-key` header matching `http.secret_key`

### Create a table

```bash
curl -X POST http://127.0.0.1:3000/api/db/default/query \
  -H "Content-Type: application/json" \
  -H "x-api-key: change-me-to-something-long-and-random" \
  -d '{"sql":"CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"}'
```

### Insert data

```bash
curl -X POST http://127.0.0.1:3000/api/db/default/query \
  -H "Content-Type: application/json" \
  -H "x-api-key: change-me-to-something-long-and-random" \
  -d '{"sql":"INSERT INTO users (id, name, age) VALUES (?, ?, ?)","params":[1,"Alice",30]}'
```

### Query data

```bash
curl -X POST http://127.0.0.1:3000/api/db/default/query \
  -H "Content-Type: application/json" \
  -H "x-api-key: change-me-to-something-long-and-random" \
  -d '{"sql":"SELECT * FROM users WHERE age > 18"}'
```

### Transactions

```bash
# Start a transaction
TX=$(curl -s -X POST http://127.0.0.1:3000/api/db/default/transaction/begin \
  -H "x-api-key: change-me-to-something-long-and-random" | jq -r .txId)

# Run statements as part of it by passing txId
curl -X POST http://127.0.0.1:3000/api/db/default/query \
  -H "Content-Type: application/json" \
  -H "x-api-key: change-me-to-something-long-and-random" \
  -d "{\"sql\":\"UPDATE users SET age = age + 1 WHERE id = 1\",\"txId\":$TX}"

# Then either:
curl -X POST http://127.0.0.1:3000/api/db/default/transaction/$TX/commit \
  -H "x-api-key: change-me-to-something-long-and-random"
# ...or, to undo everything done under $TX:
curl -X POST http://127.0.0.1:3000/api/db/default/transaction/$TX/rollback \
  -H "x-api-key: change-me-to-something-long-and-random"
```

Every plain (no `txId`) INSERT/UPDATE/DELETE is also atomic on its own if it
partially fails (e.g. a multi-row `INSERT` where a later row violates a
constraint) — you don't need an explicit transaction just to avoid a
half-applied statement. See [DOCUMENTATION.md §6a](./DOCUMENTATION.md#6a-transactions)
for what these transactions do and don't guarantee (in short: atomicity and
durability of the outcome, not isolation from concurrent transactions).

---

## Raw Key/Value API

SKVS also provides a raw key/value interface for use cases where a relational schema is unnecessary.

Available operations include:

* `put`
* `get`
* `remove`

These operations are exposed through the native Rust/Node.js API and can also be accessed through the HTTP server.

---

## Databases

A single SKVS server can contain multiple independent databases.

Example:

```toml
[[databases]]
id = 0
name = "default"

[[databases]]
id = 1
name = "users"

[[databases]]
id = 2
name = "cache"
```

Each database has its own tables and data namespace.

---

## Replication

SKVS supports peer-to-peer UDP replication between servers.

Configure the local server:

```toml
[server]
id = 1
replication_port = 9999
```

And specify the peer:

```toml
[peer]
address = "192.168.1.20:9999"
```

Replication is designed around eventual consistency and is currently intended for experimentation and development rather than production workloads.

---

## Native Addon and Platforms

SKVS contains native Rust code and therefore the compiled `.node` addon is **not universal**.

A Windows build should be used on Windows, while a Linux build should be used on Linux.

For development, simply build the addon on the machine where SKVS will run:

```bash
npm install
npx napi build --release
npm start
```

For distributing SKVS to multiple platforms, create separate native builds for each supported platform and architecture.

---

## Development

Install dependencies:

```bash
npm install
```

Build the native addon:

```bash
npx napi build --release
```

Run the server:

```bash
npm start
```

Run the available smoke tests:

```bash
python test_skvs.py
```

Run the Rust unit test suite:

```bash
cargo test
```

For details about the SQL implementation, native API, HTTP API, WAL, replication, constraints, triggers, views, JSON functions, FTS, and known limitations, see [DOCUMENTATION.md](./DOCUMENTATION.md).

---

## License

Apache-2.0. See [Cargo.toml](./Cargo.toml) for the project license information.
