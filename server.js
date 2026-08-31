const express = require('express');
const bodyParser = require('body-parser');
const ipRangeCheck = require('ip-range-check');
const toml = require('toml');
const fs = require('fs');
require('dotenv').config();

const skvs = require('./skvs.node');

const configPath = process.env.SKVS_CONFIG || '/etc/skvs/config.toml';
let config;
try {
  const content = fs.readFileSync(configPath, 'utf8');
  config = toml.parse(content);
} catch (err) {
  console.error('Failed to load config:', err);
  process.exit(1);
}

try {
  skvs.init(configPath);
} catch (err) {
  console.error('Rust init error:', err);
  process.exit(1);
}

const app = express();
app.use(bodyParser.json({ limit: '10mb' }));

const trustedIps = config.http.trusted_ips || ['127.0.0.1'];
const secretKey = config.http.secret_key || 'change-me';

function checkIp(req, res, next) {
  let clientIp = req.ip || req.connection.remoteAddress;
  if (clientIp.startsWith('::ffff:')) {
    clientIp = clientIp.slice(7);
  }
  const allowed = trustedIps.some(range => ipRangeCheck(clientIp, range));
  if (!allowed) {
    return res.status(403).json({ error: 'Access denied: IP not allowed' });
  }
  next();
}

function checkApiKey(req, res, next) {
  const key = req.headers['x-api-key'];
  if (!key || key !== secretKey) {
    return res.status(403).json({ error: 'Access denied: invalid API key' });
  }
  next();
}

app.use(checkIp);
app.use(checkApiKey);

const dbNameCache = new Map();

function getDbId(name) {
  if (dbNameCache.has(name)) {
    return dbNameCache.get(name);
  }
  const id = skvs.getDbIdByName(name);
  if (id !== undefined && id !== null) {
    dbNameCache.set(name, id);
    return id;
  }
  return null;
}

app.post('/api/db/:dbName/query', async (req, res) => {
  const { dbName } = req.params;
  const { sql, params, txId } = req.body;

  const dbId = getDbId(dbName);
  if (dbId === null) {
    return res.status(404).json({ error: 'Database not found' });
  }

  // A bare Buffer can't round-trip through JSON as a BLOB: the Rust side
  // only ever sees a JSON string and has no way to tell "this string is
  // base64 for binary data" apart from "this string is just text", so it
  // used to land as a Value::Text (silently corrupting binary parameters
  // for BLOB columns). Tagging it as {"__blob__": "<base64>"} lets
  // Value::from_json on the Rust side recognize it and decode a real
  // Value::Blob instead.
  const rustParams = (params || []).map(p => {
    if (Buffer.isBuffer(p)) return { __blob__: p.toString('base64') };
    return p; // numbers, strings, null and booleans pass straight through
  });

  try {
    const result = skvs.query(dbId, sql, rustParams, txId ?? null);
    res.json(result);
  } catch (err) {
    console.error('SQL error:', err);
    res.status(400).json({ error: err.message });
  }
});

// Transactions: begin one here, thread the returned txId through the `txId`
// field of subsequent /query calls (including BEGIN/COMMIT/ROLLBACK issued
// as literal SQL text, which also read/return txId the same way), then
// commit or roll it back. Since HTTP requests are stateless, the
// transaction's identity has to travel in the request/response bodies like
// this rather than being tied to a persistent connection.
app.post('/api/db/:dbName/transaction/begin', (req, res) => {
  const { dbName } = req.params;
  const dbId = getDbId(dbName);
  if (dbId === null) {
    return res.status(404).json({ error: 'Database not found' });
  }
  try {
    const txId = skvs.beginTransaction(dbId);
    res.json({ txId });
  } catch (err) {
    console.error('BEGIN error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/db/:dbName/transaction/:txId/commit', (req, res) => {
  const txId = Number(req.params.txId);
  try {
    skvs.commitTransaction(txId);
    res.status(204).send();
  } catch (err) {
    console.error('COMMIT error:', err);
    res.status(400).json({ error: err.message });
  }
});

app.post('/api/db/:dbName/transaction/:txId/rollback', (req, res) => {
  const txId = Number(req.params.txId);
  try {
    skvs.rollbackTransaction(txId);
    res.status(204).send();
  } catch (err) {
    console.error('ROLLBACK error:', err);
    res.status(400).json({ error: err.message });
  }
});

app.put('/api/db/:dbName/table/:tableName/row/:rowKey', (req, res) => {
  const { dbName, tableName, rowKey } = req.params;
  const { value } = req.body;
  if (!value) {
    return res.status(400).json({ error: '"value" field is required' });
  }

  const dbId = getDbId(dbName);
  if (dbId === null) {
    return res.status(404).json({ error: 'Database not found' });
  }

  const keyBuffer = Buffer.from(rowKey, 'utf8');
  let valueBuffer;
  if (Buffer.isBuffer(value)) {
    valueBuffer = value;
  } else if (typeof value === 'string') {
    try {
      valueBuffer = Buffer.from(value, 'base64');
    } catch {
      valueBuffer = Buffer.from(value, 'utf8');
    }
  } else {
    return res.status(400).json({ error: 'Unsupported value type' });
  }

  try {
    skvs.put(dbId, tableName, keyBuffer, valueBuffer);
    res.status(204).send();
  } catch (err) {
    console.error('PUT error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/db/:dbName/table/:tableName/row/:rowKey', (req, res) => {
  const { dbName, tableName, rowKey } = req.params;
  const dbId = getDbId(dbName);
  if (dbId === null) {
    return res.status(404).json({ error: 'Database not found' });
  }

  const keyBuffer = Buffer.from(rowKey, 'utf8');
  try {
    const result = skvs.get(dbId, tableName, keyBuffer);
    if (result === null || result === undefined) {
      return res.status(404).json({ error: 'Row not found' });
    }
    res.json({ value: result.toString('base64') });
  } catch (err) {
    console.error('GET error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.delete('/api/db/:dbName/table/:tableName/row/:rowKey', (req, res) => {
  const { dbName, tableName, rowKey } = req.params;
  const dbId = getDbId(dbName);
  if (dbId === null) {
    return res.status(404).json({ error: 'Database not found' });
  }

  const keyBuffer = Buffer.from(rowKey, 'utf8');
  try {
    skvs.remove(dbId, tableName, keyBuffer);
    res.status(204).send();
  } catch (err) {
    console.error('DELETE error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/flush', (req, res) => {
  try {
    skvs.flush();
    res.status(204).send();
  } catch (err) {
    console.error('Flush error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const port = config.http.port || 3000;
app.listen(port, () => {
  console.log(`skvs HTTP server running on port ${port}`);
});