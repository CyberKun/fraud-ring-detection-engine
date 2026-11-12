import { useEffect, useRef, useState, FormEvent, ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import {
  Activity,
  ArrowRight,
  ArrowUpRight,
  Boxes,
  ChevronRight,
  CircleDot,
  Clock3,
  Database,
  FlaskConical,
  GitFork,
  Radio,
  Search,
  Server,
  ShieldAlert,
  TriangleAlert,
  Workflow,
} from 'lucide-react';
import '@fontsource/inter/400.css';
import '@fontsource/inter/600.css';
import '@fontsource/inter/700.css';
import '@fontsource/jetbrains-mono/400.css';
import {
  api,
  post,
  number,
  time,
  ratio,
  Alert,
  Audit,
  Graph,
  Shard,
  Stats,
} from './api';
import { Network } from './Graph';
import './style.css';
const nav = [
  { name: 'Live alerts', icon: Radio },
  { name: 'Component explorer', icon: Workflow },
  { name: 'Audit time travel', icon: Clock3 },
  { name: 'Cluster health', icon: Server },
];
function useQuery<T>(path: string, refresh = 0) {
  const [state, set] = useState<{
    data?: T;
    error?: string;
    missing?: boolean;
    path: string;
  }>({ path: '' });
  useEffect(() => {
    if (!path) return;
    const abort = new AbortController();
    set((prev) => (prev.path === path ? prev : { path }));
    api<T>(path, { signal: abort.signal })
      .then((data) => set({ data, path }))
      .catch((e) => {
        if (!abort.signal.aborted)
          set({ error: e.message, missing: e.status === 404, path });
      });
    return () => abort.abort();
  }, [path, refresh]);
  return state.path === path ? state : { path };
}
function Notice({
  children,
  error = false,
}: {
  children: ReactNode;
  error?: boolean;
}) {
  return (
    <div
      role={error ? 'alert' : 'status'}
      className={'notice ' + (error ? 'error' : '')}
    >
      {error ? <TriangleAlert size={19} /> : <CircleDot size={18} />}
      <span>{children}</span>
    </div>
  );
}
function Metrics({ stats }: { stats: Stats }) {
  return (
    <dl>
      {[
        ['Flagged ratio', ratio(stats.flagged_ratio)],
        ['Accounts', number(stats.account_count)],
        ['Flagged accounts', number(stats.flagged_accounts)],
        ['Vertices', number(stats.vertex_count)],
        ['Volume', number(stats.volume)],
      ].map(([k, v]) => (
        <div key={k}>
          <dt>{k}</dt>
          <dd>{v}</dd>
        </div>
      ))}
    </dl>
  );
}
function Investigation({
  audit,
  tenant,
  vertex,
  anchor,
  lsn,
}: {
  audit: boolean;
  tenant: string;
  vertex: string;
  anchor: number;
  lsn: string;
}) {
  const [t, T] = useState(tenant),
    [v, V] = useState(vertex),
    [selection, S] = useState({ t: tenant, v: vertex }),
    [at, A] = useState(anchor),
    [sample, U] = useState(anchor),
    [rev, R] = useState(0);
  const [date, D] = useState(() =>
    new Date(anchor - new Date(anchor).getTimezoneOffset() * 60000)
      .toISOString()
      .slice(0, 23),
  );
  useEffect(() => {
    const timer = setTimeout(() => U(at), 130);
    return () => clearTimeout(timer);
  }, [at]);
  const min = selection.t === tenant && selection.v === vertex ? lsn : '0';
  const base = selection.v
    ? 'components/' + selection.t + '/' + selection.v
    : '';
  const graph = useQuery<Graph>(
    !audit && base ? base + '/subgraph?max_nodes=2000&min_lsn=' + min : '',
    rev,
  );
  const stats = useQuery<Stats>(
    !audit && base ? base + '/stats?min_lsn=' + min : '',
    rev,
  );
  const history = useQuery<Audit>(
    audit && base
      ? 'audit/' +
          selection.t +
          '/' +
          selection.v +
          '?timestamp_ms=' +
          sample +
          '&max_nodes=2000&min_lsn=' +
          min
      : '',
    rev,
  );
  const g = audit ? history.data?.subgraph : graph.data,
    s = audit ? history.data?.stats : stats.data,
    err = audit ? history.error : graph.error || stats.error;
  const missing = audit ? history.missing : graph.missing || stats.missing;
  const role = audit ? history.data?.replica_role : s?.replica_role,
    applied = audit ? history.data?.applied_lsn : s?.applied_lsn;
  const update = (e: FormEvent) => {
    e.preventDefault();
    S({ t, v });
    R((n) => n + 1);
  };
  function changeTime(n: number) {
    A(n);
    D(
      new Date(n - new Date(n).getTimezoneOffset() * 60000)
        .toISOString()
        .slice(0, 23),
    );
  }
  return (
    <>
      <form className="query" onSubmit={update}>
        <label>
          Tenant
          <input
            required
            pattern="[0-9]+"
            value={t}
            onChange={(e) => T(e.target.value)}
          />
        </label>
        <label>
          Vertex ID
          <input
            required
            pattern="[0-9]+"
            placeholder="Enter vertex ID"
            value={v}
            onChange={(e) => V(e.target.value)}
          />
        </label>
        <button>
          <Search size={16} /> Inspect
        </button>
      </form>
      {audit && (
        <section className="timeline">
          <div>
            <h3>Historical timestamp</h3>
            <input
              aria-label="Historical timestamp"
              type="datetime-local"
              step=".001"
              value={date}
              onChange={(e) => {
                D(e.target.value);
                const n = new Date(e.target.value).getTime();
                if (Number.isFinite(n) && n >= 0) A(n);
              }}
            />
          </div>
          <input
            type="range"
            aria-label="Audit timestamp"
            min={Math.max(0, Math.min(anchor - 60000, at))}
            max={Math.max(anchor + 1000, at)}
            value={at}
            onChange={(e) => changeTime(Number(e.target.value))}
          />
          {history.data?.first_alert_timestamp_ms && (
            <p className="first">
              <Clock3 size={15} /> First alert{' '}
              <time>
                {new Date(
                  Number(history.data.first_alert_timestamp_ms),
                ).toLocaleString()}
              </time>
            </p>
          )}
        </section>
      )}
      {!base ? (
        <Notice>
          Choose an alert or enter a tenant and vertex ID to inspect a
          component.
        </Notice>
      ) : missing ? (
        <Notice>
          {audit
            ? 'No component exists at this timestamp.'
            : 'No component found for this tenant and vertex.'}
        </Notice>
      ) : err ? (
        <Notice error>
          {err}. Check the vertex and timestamp, then inspect again.
        </Notice>
      ) : !g || !s ? (
        <Notice>Loading component…</Notice>
      ) : !g.vertices.length ? (
        <Notice>No vertices at this timestamp.</Notice>
      ) : (
        <div className="split">
          <Network graph={g} focus={selection.v} />
          <aside className="panel">
            <h3>Component stats</h3>
            <Metrics stats={s} />
            <div className="source">
              <p>
                Read source <b>{role}</b>
              </p>
              <p>
                Applied LSN <b>{applied}</b>
              </p>
              <p>
                Read barrier <b>{min}</b>
              </p>
              {!audit && (
                <p>
                  Lag at sample{' '}
                  <b>
                    {s.lag_records == null
                      ? 'Unknown'
                      : s.lag_records + ' records'}
                  </b>
                </p>
              )}
            </div>
          </aside>
        </div>
      )}
    </>
  );
}
function Health({ dev }: { dev: boolean }) {
  const [rev, R] = useState(0),
    [action, Act] = useState(''),
    [pending, P] = useState(false),
    [confirm, C] = useState<number | null>(null);
  useEffect(() => {
    const n = setInterval(() => R((i) => i + 1), 3500);
    return () => clearInterval(n);
  }, []);
  const q = useQuery<{ shards: Shard[] }>('cluster/health', rev);
  async function kill(s: Shard) {
    P(true);
    C(null);
    try {
      await post('dev/kill-leader/' + s.shard_id, { epoch: s.epoch });
      Act('Leader stop requested for shard ' + s.shard_id);
      R((i) => i + 1);
    } catch (e) {
      Act(String(e));
    } finally {
      P(false);
    }
  }
  return (
    <>
      {action && <Notice>{action}</Notice>}
      {q.error ? (
        <Notice error>{q.error}</Notice>
      ) : !q.data ? (
        <Notice>Loading shard topology…</Notice>
      ) : !q.data.shards.length ? (
        <Notice>No shards registered.</Notice>
      ) : (
        <div className="shards">
          {q.data.shards.map((s) => (
            <article className="panel shard" key={s.shard_id}>
              <header>
                <h3>
                  <Boxes size={18} /> Shard {s.shard_id}
                </h3>
                <span
                  className={
                    'pill ' + (s.replica_role === 'LEADER' ? 'green' : 'amber')
                  }
                >
                  {s.replica_role === 'LEADER' ? 'Elected' : 'Unavailable'}
                </span>
              </header>
              <p className="mono">
                Epoch {s.epoch} · {s.ingestion_rate.toFixed(1)} records/s
              </p>
              <table>
                <thead>
                  <tr>
                    <th>Replica</th>
                    <th>Applied LSN</th>
                    <th>Lag</th>
                  </tr>
                </thead>
                <tbody>
                  {s.members.map((m) => (
                    <tr key={m.address}>
                      <td>
                        {m.address}
                        <small>{m.replica_role}</small>
                      </td>
                      <td>
                        {m.replica_role === 'UNAVAILABLE' ? '—' : m.applied_lsn}
                      </td>
                      <td>{m.replica_role === 'UNAVAILABLE' ? '—' : m.lag}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {dev &&
                (confirm === s.shard_id ? (
                  <div className="actions">
                    <button onClick={() => C(null)}>Cancel</button>
                    <button className="danger" onClick={() => kill(s)}>
                      Confirm stop
                    </button>
                  </div>
                ) : (
                  <button
                    className="danger"
                    disabled={
                      pending ||
                      s.replica_role !== 'LEADER' ||
                      s.members.filter((m) => m.replica_role !== 'UNAVAILABLE')
                        .length < 3
                    }
                    onClick={() => C(s.shard_id)}
                  >
                    Kill leader
                  </button>
                ))}
            </article>
          ))}
        </div>
      )}
      <section className="panel graf">
        <h3>
          <Activity size={18} /> WAL fsync latency
        </h3>
        <iframe
          title="Grafana WAL fsync latency"
          src="/grafana/d-solo/ringwatch/replica-latency?orgId=1&panelId=1&from=now-15m&to=now&theme=dark&refresh=5s"
        />
      </section>
    </>
  );
}
function App() {
  const [page, G] = useState(0),
    [tenant, T] = useState('1'),
    [input, I] = useState('1'),
    [alerts, Q] = useState<Alert[]>([]),
    [status, St] = useState('Connecting'),
    [selected, S] = useState<Alert | null>(null),
    [seed, B] = useState(false),
    [message, M] = useState(''),
    [start, Start] = useState(Date.now),
    [active, A] = useState(0);
  const cfg = useQuery<{ dev_mode: boolean }>('config');
  const dev = cfg.data?.dev_mode === true;
  const rows = useRef<(HTMLButtonElement | null)[]>([]);
  useEffect(() => {
    let ended = false,
      timer: ReturnType<typeof setTimeout>,
      ws: WebSocket;
    Q([]);
    S(null);
    M('');
    Start(Date.now());
    A(0);
    function connect() {
      St('Connecting');
      ws = new WebSocket(
        (location.protocol === 'https:' ? 'wss://' : 'ws://') +
          location.host +
          '/api/alerts/stream?tenant=' +
          tenant,
      );
      ws.onmessage = (e) => {
        if (ended) return;
        const data = JSON.parse(e.data);
        if (data.type === 'ready') {
          St('Connected');
          return;
        }
        if (data.type === 'alert') {
          const a: Alert = data.event;
          Q((prev) => [a, ...prev].slice(0, 200));
        }
      };
      ws.onclose = () => {
        if (!ended) {
          St('Reconnecting');
          timer = setTimeout(connect, 1400);
        }
      };
    }
    connect();
    return () => {
      ended = true;
      clearTimeout(timer);
      ws.close();
    };
  }, [tenant]);
  async function seedDemo() {
    B(true);
    M('');
    try {
      await post('dev/seed-demo', { tenant_id: tenant });
      M('Demo ring submitted');
    } catch (e) {
      M(String(e));
    } finally {
      B(false);
    }
  }
  const latest = alerts[0],
    flagged = alerts.filter((a) => a.stats.flagged_ratio >= 0.5).length;
  function open(a: Alert) {
    S(a);
    G(1);
  }
  const preview = useQuery<Graph>(
    page === 0 && latest
      ? 'components/' +
          tenant +
          '/' +
          latest.vertex_id +
          '/subgraph?max_nodes=2000&min_lsn=' +
          latest.lsn
      : '',
  );
  return (
    <div className="shell">
      <aside className="rail">
        <div className="brand">
          <span className="mark">
            <GitFork size={24} />
          </span>
          Ringwatch
        </div>
        <div className="rail-label">Operations console</div>
        <nav>
          {nav.map((n, i) => (
            <button
              key={n.name}
              className={page === i ? 'current' : ''}
              aria-current={page === i ? 'page' : undefined}
              onClick={() => G(i)}
            >
              <n.icon size={18} />
              {n.name}
              {i === 0 && alerts.length > 0 && <b>{alerts.length}</b>}
            </button>
          ))}
        </nav>
        <div className="rail-bottom">
          <span className={'dot ' + (status === 'Connected' ? 'on' : '')} />
          <div>
            Alert stream<small>{status}</small>
          </div>
        </div>
      </aside>
      <div className="work">
        <header className="bar">
          <span>
            Workspace <ChevronRight size={14} />{' '}
            <strong>{nav[page].name}</strong>
          </span>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              T(input);
            }}
          >
            <Database size={14} />
            <label>
              Tenant{' '}
              <input
                aria-label="Active tenant"
                required
                pattern="[0-9]+"
                value={input}
                onChange={(e) => I(e.target.value)}
              />
            </label>
            {input !== tenant && <button>Apply</button>}
          </form>
        </header>
        <main>
          <div className="title">
            <div>
              <h1>{nav[page].name}</h1>
              <p>
                {
                  [
                    'Monitor fraud-rule crossings as they arrive.',
                    'Inspect connections and account risk within a component.',
                    'Replay a component at a historical timestamp.',
                    'Monitor replica positions, elections and write latency.',
                  ][page]
                }
              </p>
            </div>
            {dev && page === 0 && (
              <button
                className="primary"
                onClick={seedDemo}
                disabled={seed || status !== 'Connected'}
              >
                <FlaskConical size={16} />
                {seed ? 'Seeding…' : 'Seed demo ring'}
              </button>
            )}
          </div>
          {message && page === 0 && <small role="status">{message}</small>}
          {page === 0 ? (
            <>
              <div className="summary">
                {[
                  [Radio, 'Buffered alerts', String(alerts.length)],
                  [ShieldAlert, 'Ratio ≥ 50%', String(flagged)],
                  [
                    Clock3,
                    'Latest alert',
                    latest ? time(latest.event_time_ms) : '—',
                  ],
                ].map(([Icon, label, value]) => {
                  const Image = Icon as typeof Radio;
                  return (
                    <section key={String(label)}>
                      <span>
                        <Image size={17} />
                        {String(label)}
                      </span>
                      <strong>{String(value)}</strong>
                    </section>
                  );
                })}
              </div>
              <section className="feed">
                <header>
                  <h3>
                    <span className="dot on" /> Alert feed
                  </h3>
                  <span className="mono muted">Newest first</span>
                </header>
                {status !== 'Connected' && (
                  <Notice error={status === 'Reconnecting'}>
                    {status === 'Reconnecting'
                      ? 'Stream interrupted. Reconnecting…'
                      : 'Opening alert stream…'}
                  </Notice>
                )}
                {alerts.length === 0 ? (
                  <div className="empty">
                    <div className="radar">
                      <Radio size={34} />
                    </div>
                    <h2>Waiting for alerts</h2>
                    <p>
                      Qualifying components for tenant {tenant} will appear
                      here.
                    </p>
                  </div>
                ) : (
                  <div className="feed-scroll">
                    <div className="row labels">
                      <span>Event time</span>
                      <span>Vertex ID</span>
                      <span>Flagged ratio</span>
                      <span>Accounts</span>
                      <span>Volume</span>
                      <span />
                    </div>
                    {alerts.map((a, i) => (
                      <button
                        ref={(el) => {
                          rows.current[i] = el;
                        }}
                        key={a.epoch + ':' + a.lsn + ':' + a.vertex_id}
                        className="row"
                        tabIndex={i === active ? 0 : -1}
                        onFocus={() => A(i)}
                        onKeyDown={(e) => {
                          if (['ArrowUp', 'ArrowDown'].includes(e.key)) {
                            e.preventDefault();
                            rows.current[
                              Math.max(
                                0,
                                Math.min(
                                  alerts.length - 1,
                                  i + (e.key === 'ArrowDown' ? 1 : -1),
                                ),
                              )
                            ]?.focus();
                          }
                        }}
                        onClick={() => open(a)}
                      >
                        <span>{time(a.event_time_ms)}</span>
                        <span>{a.vertex_id}</span>
                        <span
                          className={
                            'pill ' +
                            (a.stats.flagged_ratio >= 0.5 ? 'red' : 'amber')
                          }
                        >
                          {ratio(a.stats.flagged_ratio)}
                        </span>
                        <span className="pill neutral">
                          {number(a.stats.account_count)}
                        </span>
                        <span className="pill neutral">
                          {number(a.stats.volume)}
                        </span>
                        <ArrowUpRight size={16} />
                      </button>
                    ))}
                  </div>
                )}
              </section>
              {latest && (
                <section className="preview">
                  <div className="section-heading">
                    <h2>Latest component</h2>
                    <button onClick={() => open(latest)}>
                      Open explorer <ArrowRight size={14} />
                    </button>
                  </div>
                  {preview.data ? (
                    <Network graph={preview.data} focus={latest.vertex_id} />
                  ) : (
                    <Notice error={!!preview.error}>
                      {preview.error || 'Loading graph…'}
                    </Notice>
                  )}
                </section>
              )}
              <footer className="session">
                <Radio size={14} /> Listening since {time(start)}{' '}
                <span>Buffer · 200 alerts</span>
              </footer>
            </>
          ) : page === 3 ? (
            <Health dev={dev} />
          ) : (
            <Investigation
              key={page + ':' + tenant + ':' + selected?.lsn}
              audit={page === 2}
              tenant={tenant}
              vertex={selected?.vertex_id || latest?.vertex_id || ''}
              anchor={Number(
                selected?.event_time_ms || latest?.event_time_ms || start,
              )}
              lsn={selected?.lsn || latest?.lsn || '0'}
            />
          )}
        </main>
      </div>
    </div>
  );
}
createRoot(document.getElementById('root')!).render(<App />);
