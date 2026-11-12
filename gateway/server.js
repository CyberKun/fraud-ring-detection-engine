import express from 'express';
import grpc from '@grpc/grpc-js';
import loader from '@grpc/proto-loader';
import { WebSocketServer, WebSocket } from 'ws';
import { randomBytes } from 'node:crypto';

const rpc = grpc.loadPackageDefinition(
  loader.loadSync('/proto/fraud_engine.proto', {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  }),
).fraud.rpc;
const router = new rpc.FraudEngine(
  process.env.ROUTER_ADDRESS || 'router:5200',
  grpc.credentials.createInsecure(),
);
const dev = process.env.FRAUD_DEV_MODE === '1';
const app = express();
app.use(express.json({ limit: '2kb' }));
const invalid = (message) => Object.assign(new Error(message), { status: 400 });
function id(s) {
  if (
    typeof s !== 'string' ||
    !/^(0|[1-9]\d{0,19})$/.test(s) ||
    BigInt(s) > 0xffffffffffffffffn
  )
    throw invalid('Expected an unsigned 64-bit integer');
  return s;
}
function query(req) {
  const q = {
    tenant_id: id(req.params.tenant),
    vertex_id: id(req.params.vertex),
  };
  if (req.query.min_lsn !== undefined) q.min_lsn = id(req.query.min_lsn);
  return q;
}
function limit(req) {
  const n = Number(id(req.query.max_nodes ?? '2000'));
  if (n < 1 || n > 2000) throw invalid('max_nodes must be between 1 and 2000');
  return n;
}
function call(name, req, metadataCallback) {
  return new Promise((resolve, reject) => {
    const rpc = router[name](
      req,
      { deadline: Date.now() + 10000 },
      (err, result) => (err ? reject(err) : resolve(result)),
    );
    if (metadataCallback)
      rpc.once('status', (status) => metadataCallback(status.metadata));
  });
}
app.get('/api/config', (_, res) => res.json({ dev_mode: dev }));
app.get('/api/components/:tenant/:vertex/stats', async (req, res) => {
  let address;
  const stats = await call('ComponentStats', query(req), (meta) => {
    address = meta.get('replica-address')[0];
  });
  const health = await call('GetClusterHealth', {}).catch(() => null);
  const shard = health?.shards.find(
    (s) =>
      s.replica_role === 'LEADER' &&
      s.members.some((m) => m.address === address),
  );
  const lag = shard
    ? BigInt(shard.applied_lsn) - BigInt(stats.applied_lsn)
    : null;
  res.json({
    ...stats,
    lag_records: lag === null ? null : String(lag > 0n ? lag : 0n),
  });
});
app.get('/api/components/:tenant/:vertex/subgraph', async (req, res) =>
  res.json(
    await call('GetComponentSubgraph', {
      ...query(req),
      max_nodes: limit(req),
    }),
  ),
);
app.get('/api/audit/:tenant/:vertex', async (req, res) =>
  res.json(
    await call('AuditAt', {
      ...query(req),
      timestamp_ms: id(req.query.timestamp_ms),
      max_nodes: limit(req),
    }),
  ),
);
app.get('/api/cluster/health', async (_, res) =>
  res.json(await call('GetClusterHealth', {})),
);
app.use('/api/dev', (req, res, next) => {
  if (!dev) return res.sendStatus(403);
  if (
    !req.is('application/json') ||
    req.headers['sec-fetch-site'] === 'cross-site'
  )
    return res.sendStatus(415);
  next();
});
app.post('/api/dev/kill-leader/:shard', async (req, res) => {
  const shard = Number(id(req.params.shard));
  if (shard > 0xffffffff) throw invalid('Invalid shard ID');
  const health = await call('GetClusterHealth', {});
  const current = health.shards.find(
    (s) => s.shard_id === shard && s.replica_role === 'LEADER',
  );
  if (!current) return res.status(409).json({ error: 'No active leader' });
  const epoch = id(req.body?.epoch ?? current.epoch);
  res.json(await call('DevStopLeader', { shard_id: shard, epoch }));
});
app.post('/api/dev/seed-demo', async (req, res) => {
  const tenant_id = id(req.body?.tenant_id ?? '1');
  const n = randomBytes(6).readUIntBE(0, 6);
  const base = BigInt(n) * 16n,
    account = (i) => String(base + BigInt(i)),
    card = (i) => String((1n << 62n) | BigInt(account(i))),
    device = String((2n << 62n) | base);
  const expiry_ms = String(Date.now() + 3600000);
  let sequence = 0;
  const source = () => ({
    producer_id: String(base),
    sequence: String(++sequence),
  });
  const update = (i, flag) =>
    call('UpdateVertex', {
      tenant_id,
      vertex_id: account(i),
      volume: String(1140 + 425 * i),
      is_flagged: flag,
      source: source(),
    });
  const link = (u, v, edge_type) =>
    call('UpsertEdge', {
      tenant_id,
      u,
      v,
      edge_type,
      expiry_ms,
      source: source(),
    });
  for (let i = 0; i < 7; i++) await update(i, false);
  for (let i = 0; i < 7; i++) {
    await link(account(i), card(i % 2), 1);
    await link(account(i), device, 2);
    if (i) await link(account(i - 1), account(i), 3);
  }
  await update(0, true);
  const last = await update(1, true);
  res.json({ tenant_id, vertex_id: account(1), applied_lsn: last.applied_lsn });
});
app.use((err, req, res, next) => {
  const status =
    err.status ||
    { 3: 400, 5: 404, 7: 403, 8: 429, 9: 409, 14: 503, 4: 504 }[err.code] ||
    502;
  res.status(status).json({ error: err.details || err.message });
});
const server = app.listen(8080, '0.0.0.0');
const wss = new WebSocketServer({
  server,
  path: '/api/alerts/stream',
  maxPayload: 1024,
});
wss.on('connection', (ws, req) => {
  let tenant;
  try {
    tenant = id(
      new URL(req.url, 'http://gateway').searchParams.get('tenant') ?? '1',
    );
  } catch {
    ws.close(1008, 'Invalid tenant');
    return;
  }
  const stream = router.SubscribeAlerts({ tenant_id: tenant });
  stream.on('metadata', () => {
    if (ws.readyState === WebSocket.OPEN)
      ws.send(JSON.stringify({ type: 'ready', tenant_id: tenant }));
  });
  stream.on('data', (event) => {
    if (ws.readyState !== WebSocket.OPEN) return;
    if (ws.bufferedAmount > 1024 * 1024) {
      ws.close(1013, 'Subscriber fell behind');
      return;
    }
    ws.send(JSON.stringify({ type: 'alert', event }));
  });
  stream.on('error', () => ws.close(1011, 'Router stream unavailable'));
  stream.on('end', () => ws.close(1000, 'Router stream ended'));
  ws.on('error', () => stream.cancel());
  ws.on('close', () => stream.cancel());
});
