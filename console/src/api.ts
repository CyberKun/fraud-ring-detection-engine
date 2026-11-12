export type Stats = {
  lag_records?: string | null;
  vertex_count: string;
  account_count: string;
  flagged_accounts: string;
  flagged_ratio: number;
  volume: string;
  applied_lsn: string;
  replica_role: string;
};
export type Vertex = {
  vertex_id: string;
  entity_type: 'ACCOUNT' | 'CARD' | 'DEVICE';
  volume: string;
  is_flagged: boolean;
};
export type Graph = {
  vertices: Vertex[];
  edges: { u: string; v: string }[];
  total_vertices: string;
  truncated: boolean;
  applied_lsn: string;
  replica_role: string;
};
export type Alert = {
  tenant_id: string;
  vertex_id: string;
  event_time_ms: string;
  epoch: string;
  lsn: string;
  stats: Stats;
};
export type Audit = {
  stats: Stats;
  subgraph: Graph;
  first_alert_timestamp_ms?: string;
  applied_lsn: string;
  replica_role: string;
};
export type Member = {
  address: string;
  replica_role: string;
  applied_lsn: string;
  epoch: string;
  lag: string;
};
export type Shard = Member & {
  shard_id: number;
  ingestion_rate: number;
  members: Member[];
};
export async function api<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const res = await fetch('/api/' + path, options);
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw Object.assign(new Error(data.error || res.statusText), {
      status: res.status,
    });
  }
  return res.json();
}
export const post = <T>(path: string, body: unknown) =>
  api<T>(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
export const number = (n: string | number) =>
  typeof n === 'number' ? n.toLocaleString() : BigInt(n).toLocaleString();
export const time = (s: string | number) =>
  new Date(Number(s)).toLocaleTimeString([], { hour12: false });
export const ratio = (r: number) => (r * 100).toFixed(1) + '%';
