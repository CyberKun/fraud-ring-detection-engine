# ROLE
You are a senior distributed-systems and algorithms engineer. Build the project below in this repo, phase by phase. Correctness first, then performance, then distribution. Never claim a phase is done unless its acceptance gate passes and you have shown the command output.

# PROJECT
Streaming Fraud-Ring Detection Engine: a distributed service that maintains connected components of a payment-entity graph (accounts, cards, devices) under edge insertions, TTL extensions, TTL expirations, and unpredictable manual deletions. It answers `Connected(u,v)`, `ComponentStats(u)`, and `GetComponentSubgraph(u)` in logarithmic/output-sensitive time and raises alerts on suspicious components.

# LANGUAGE, BUILD, AND TOOLING
- Engine: C++20, CMake, no exceptions in hot paths.
- Polymorphism rule: ZERO virtual dispatch inside tree operations (ETT, LCT, and DSU nodes must be flat value types stored in contiguous vectors/slabs). Expose a C++20 `concept DynamicGraphEngine` and wrap engines in `std::variant<OfflineEngine, LctEngine, HdtEngine>` (or restrict virtual dispatch strictly to the coarse outer service/RPC boundary).
- Build environment: Provide a reproducible `Dockerfile.dev` using system apt packages (`build-essential`, `cmake`, `ninja-build`, `libgrpc++-dev`, `libprotobuf-dev`, `protobuf-compiler-grpc`, `librdkafka-dev`, `libgtest-dev`, `libbenchmark-dev`, `libcurl4-openssl-dev`, `nlohmann-json3-dev`) so CMake configures and builds cleanly without fragile `FetchContent` compilation of gRPC or etcd.
- etcd client: Implement etcd lease, keepalive, CAS election, and watch/poll directly against etcd's built-in v3 HTTP/JSON API (`/v3/lease/grant`, `/v3/lease/keepalive`, `/v3/kv/txn`, `/v3/kv/range`) using `libcurl` + `nlohmann/json`. Do not pull in third-party C++ etcd wrapper libraries.
- Prometheus metrics: Expose a lightweight HTTP `/metrics` endpoint in standard Prometheus text exposition format (via `prometheus-cpp` or a clean lock-free counter/histogram serializer).
- Infra: Docker Compose (Kafka/Redpanda, etcd, Prometheus, Grafana). Add kind/k8s manifests only in Phase 9.
- Tooling: load generator and fuzz-report scripts in Python 3.
- Commit after every passing milestone with a descriptive message. Keep `PROGRESS.md` updated (done / in progress / blocked / measured numbers).

# DATA MODEL & EVENT SEMANTICS (shared across all engines)

## Vertex ID Encoding (`uint64_t`)
- The top 2 bits of every `VertexId` (`bits 63..62`) encode `EntityType`:
  - `00` = Account
  - `01` = Card
  - `10` = Device (or IP)
- The lower 62 bits hold the entity's unique numeric identifier.
- `account_count` is derived directly from `EntityType` (`1` if Account, `0` if Card/Device) without external lookups.

## Fraud Aggregates (per vertex & component)
- `vertex_count`: `1` on every real vertex (`0` on LCT edge-dummy nodes and ETT dart nodes). Used for HDT's level-size invariant ($|T_u| \le V / 2^i$) and smaller-side selection.
- `account_count`: `1` on Account vertices, `0` on Card/Device vertices.
- `volume`: `uint64_t` integer paise/cents (`0` by default).
- `flagged_accounts`: `1` only on Account vertices where `is_flagged == true`, `0` otherwise (Card/Device vertices always have `flagged_accounts = 0`).
- `flagged_ratio`: derived as `static_cast<double>(flagged_accounts) / account_count` (0.0 if `account_count == 0`). NEVER divide by `vertex_count`.
- Alert rule: A component satisfies the alert rule when `account_count >= min_account_count` (e.g., 5) AND `flagged_ratio >= min_flagged_ratio` (e.g., 0.25).
- Alert emission & per-vertex cooldown rule: Before applying a mutation touching `u` (and `v`), record whether `u`'s component (and `v`'s component) currently satisfies the alert rule. After the mutation, if a resulting component satisfies the alert rule AND (at least one pre-mutation side did NOT satisfy the alert rule, OR `VertexUpdate` just newly flagged an account) AND the touched vertex's cooldown (`last_alert_ms + cooldown_ms <= event_time_ms`) has expired, emit an `AlertEvent` and update the cooldown timestamp on the touched vertex(es). Never use component high-water marks, as component root IDs change across tree rotations and splits/merges.

## Total Expiry Order
- All expiry comparisons across the entire system use the strict composite key `(expiry_ms, handle_id)`.
- Both LCT-MSF path-min queries and timing wheel emission order use `(expiry_ms, handle_id)` so tie-breaking is 100% deterministic and a covering non-tree edge with an earlier or equal timestamp is guaranteed to expire before the tree edge it covers.

## Unified Event Format & Monotonic Time
Every WAL record and event carries `(epoch, lsn, event_time_ms)` and optional `(producer_id, sequence)` for ingestion deduplication.
- Monotonic event time: No wall-clock reads (`std::chrono::system_clock::now()`) are allowed inside engine state transitions. The leader assigns `event_time_ms = max(local_wall_clock_ms, last_wal_event_time_ms)` so time never goes backwards across leader failovers.
- `Tick(event_time_ms)`: The leader emits `Tick(event_time_ms)` records into the WAL at a fixed interval (e.g., every 100ms) so timing-wheel expirations fire deterministically even on idle tenants.
- `EdgeUpsert(u, v, edge_type, expiry_ms)`: normalize `(u, v)` as `(min(u,v), max(u,v), edge_type)`.
- `EdgeDelete(u, v, edge_type)`: unpredictable manual deletion (card unlinked, chargeback reversal, analyst prune).
- `VertexUpdate(v, volume, is_flagged)`: OVERWRITES `v`'s `volume` and `is_flagged` state (`is_flagged` is forced to `false` if `EntityType(v) != Account`). If `v` does not exist yet, initializes `v`.
- `Expire(edge_handle, version)`: emitted deterministically by the leader's timing wheel when `(expiry_ms, handle_id)` is due at `event_time_ms` and written to the WAL.
- `HubSuppress(v)` / `HubRestore(v)`: emitted deterministically by the leader's hub policy layer and written to the WAL.
- Read-only queries: `Connected(u, v)`, `ComponentStats(u)`, `GetComponentSubgraph(u, max_nodes)`, `AuditAt(time_ms, u, max_nodes)`.

## Shared Graph Layer (above the engines)
- Deduplicated edges: hash map `NormalizedEdgeKey(u, v, type) -> EdgeRecord{handle_id, version, expiry_ms, is_shadow}`. At most one live edge per key.
- Repeat transaction = TTL extension: If `EdgeUpsert` arrives for an already-live key with `new_expiry_ms > old_expiry_ms`, bump `version`, update `expiry_ms`, schedule a new timing-wheel entry, and invoke the engine's `extend_ttl(handle_id, new_expiry_ms)` path rather than inserting a duplicate edge.
- Timing wheel with lazy invalidation: stores `(expiry_ms, handle_id, version)` ordered by `(expiry_ms, handle_id)`. When `event_time_ms` advances (on mutations or `Tick`), entries with `version != current_version` or deleted handles are silently discarded. Valid expirations emit `Expire(handle_id, version)` into the WAL.
- Manual delete: removes the key from the active map, invalidates pending wheel entries by bumping/tombstoning the handle version, and invokes engine `delete_edge(handle_id)`.

## Hub Suppression Policy (above the engines)
- Applies to `Card` and `Device` vertices (never `Account` vertices).
- Suppress: When a non-suppressed Device/Card vertex's active degree reaches `tau_high` (default 50), emit `HubSuppress(v)` to the WAL. Remove all its structural edges from the connectivity engine, converting them into `is_shadow = true` entries in the dedup map.
- Shadow tracking: While `v` is suppressed, new `EdgeUpsert`, `EdgeDelete`, and `Expire` events touching `v` update only its shadow edges in the dedup map without touching the connectivity forest.
- Restore: When `v`'s shadow degree falls to or below `tau_low` (default 35, providing hysteresis), emit `HubRestore(v)` to the WAL. Re-insert each remaining live shadow edge through the engine's normal `insert_edge` path (some endpoints may already be connected via other paths).
- State explicitly in docs that hub suppression is a signal-quality policy layer, so the forest intentionally represents hub-filtered connectivity.

# ENGINES

## Brute-Force Reference Oracle (for Tier-1 verification)
- Maintains an explicit flat adjacency list and vertex weight map. Computes `Connected`, `ComponentStats`, and `GetComponentSubgraph` via direct BFS/DFS from scratch on every query.

## Engine A: Offline Replay (Segment Tree over Time + Rollback DSU)
- Takes a bounded history of events over `[0, T)`.
- Decomposes each structural edge's active lifespan `[t_add, t_remove)` into $\mathcal{O}(\log T)$ nodes of a segment tree over event indices.
- Decomposes each vertex's weight lifespan `[t_update_i, t_update_{i+1})` (or applies vertex weight deltas with a rollback stack at leaves) so dynamic `VertexUpdate` events are accurately reflected at every point in time.
- Runs a single DFS over the segment tree with a union-by-rank DSU supporting `snapshot()` and `rollback()`, maintaining all four fraud aggregates (`vertex_count`, `account_count`, `volume`, `flagged_accounts`).
- Serves `AuditAt(tenant, time_ms, vertex_id, max_nodes)` and acts as the historical correctness oracle for Engines B and C.

## Engine B: LCT-MSF with Repair (Specialized Baseline)
- Maintains a Maximum Spanning Forest keyed by `(expiry_ms, handle_id)` using a splay-based Link-Cut Tree, plus a raw per-vertex adjacency list holding all live non-tree edges.
- Edge dummy vertices: Every tree edge is represented as a dummy vertex in the LCT holding `(expiry_ms, handle_id)` for path-min queries and `0` for all four fraud aggregates (`vertex_count = 0`).
- Bounded slab allocator: Because only tree edges live in the LCT, at most $V - 1$ dummy nodes exist at once. Allocate dummy nodes from a fixed slab of size $V - 1$ and recycle them on tree-edge eviction or deletion so LCT memory is strictly bounded at $\le 2V - 1$ nodes regardless of edge density.
- Component aggregates via `virtual_agg`: Each LCT node stores `self_agg`, `preferred_sub_agg`, and `virtual_agg` (the sum of `preferred_sub_agg` across all dashed/non-preferred children). Update `virtual_agg` inside `access(v)` whenever a preferred right child is dashed or a dashed child becomes preferred. After `access(v)`, the component's total aggregate is `v.preferred_sub_agg`. (`makeroot` path reversal does not affect sums because addition is commutative; `path_min_expiry` is tracked only along preferred paths.)
- Insert `(u, v, expiry_ms, handle_id)`: If `u` and `v` are unconnected, allocate a dummy node and link in the LCT. If already connected, query `path_min_expiry(u, v)` by `(expiry_ms, handle_id)`. If `(expiry_ms, handle_id)` is strictly greater than the minimum-expiry edge $e_{\min}$ on the cycle, cut $e_{\min}$, move $e_{\min}$ to the non-tree adjacency list, and link the new edge; otherwise store the new edge in the non-tree adjacency list.
- TTL extension (`extend_ttl`):
  - If tree edge: update dummy node's `(expiry_ms, handle_id)` and splay/pull to refresh `path_min_expiry`.
  - If non-tree edge: query `path_min_expiry(u, v)` on its cycle; if the extended `(expiry_ms, handle_id)` beats $e_{\min}$, swap them.
- Predictable expiry (`Expire` of a tree edge): Cut the dummy node and recycle it. Under pure TTL (and with `(expiry_ms, handle_id)` total ordering), no valid replacement edge can exist.
- Repair on manual deletion (`EdgeDelete` of a tree edge) OR `HubSuppress`:
  - Note: `HubSuppress` always runs this repair path on removed tree edges regardless of whether the tenant is configured for pure TTL or mixed deletions.
  1. Cut the tree edge and recycle its dummy node.
  2. Call `access(u)` and `access(v)` to read the exact `vertex_count` of both split subtrees in $\mathcal{O}(\log V)$.
  3. Directly enumerate the vertices of ONLY the smaller split subtree $T_{\text{small}}$ (no lockstep traversal needed).
  4. Scan all non-tree edges incident to $T_{\text{small}}$ whose other endpoint lies outside $T_{\text{small}}$. Pick the crossing candidate with the **MAXIMUM `(expiry_ms, handle_id)`** (scanning all candidates, not stopping at the first) to preserve the MSF invariant.
  5. If found, promote it from the non-tree adjacency list into the LCT.

## Engine C: HDT with Layered Euler Tour Trees (General Live Engine)
- Hierarchy of $L = \lfloor \log_2 V \rfloor + 1$ levels (`0 .. L-1`). Every live structural edge has a level $\ell \in [0, L-1]$ that only increases.
- Forest $F_i$ contains all tree edges of level $\ge i$, stored as an edge-based Euler Tour Tree (randomized treap or splay BST over the Euler tour).
- Avoiding degree-based double counting:
  - Each vertex $v$ has ONE permanent self-loop ETT node `(v, v)` at each active level holding `vertex_count = 1`.
  - Each undirected tree edge `(u, v)` at level $\ell$ creates two directed dart ETT nodes `(u, v)` and `(v, u)` in forests $F_0 \dots F_\ell$, each carrying `0` for all vertex aggregates.
  - Because `(v, v)` is permanent and never removed during edge cuts or reroots, canonical-node reassignment is never needed.
- Where aggregates live:
  - `vertex_count` is maintained in ETT nodes across ALL levels $F_0 \dots F_{L-1}$ (required to pick the smaller split subtree at level $i$).
  - `account_count`, `volume`, and `flagged_accounts` are maintained ONLY in $F_0$ (whose components are the true connected components).
  - In $F_i$, each ETT node maintains two boolean subtree bitflags: `has_level_i_tree_edge` (true if its ETT subtree contains a dart node whose exact edge level is $i$) and `has_level_i_nontree_edge` (true if its ETT subtree contains a self-loop `(v, v)` with at least one incident level-$i$ non-tree edge), enabling $\mathcal{O}(\log V)$ amortized discovery during replacement search.
- Operations:
  - `Insert(u, v)`: If `ETT_root_F0(u) != ETT_root_F0(v)`, link `(u, v)` as a level-0 tree edge in $F_0$. Otherwise add `(u, v)` to the level-0 non-tree adjacency lists of `u` and `v` and update `has_level_i_nontree_edge` in $F_0$.
  - `ExtendTTL(handle, new_expiry_ms)`: No structural change in HDT; the shared timing wheel + dedup map handles the updated expiry.
  - `Delete(handle)` of a non-tree edge at level $\ell$: Remove from `u` and `v`'s level-$\ell$ non-tree adjacency lists and update `has_level_i_nontree_edge` in $F_\ell$.
  - `Delete(handle)` of a level-$\ell$ tree edge:
    1. Cut `(u, v)` from forests $F_0$ through $F_\ell$.
    2. For $i = \ell$ down to $0$:
       a. In $F_i$, compare `vertex_count` of the two split ETT roots and let $T_u$ be the smaller subtree ($|T_u| \le |T_v|$).
       b. Using `has_level_i_tree_edge` in $F_i$, visit all level-$i$ tree edges inside $T_u$, increment their level to $i+1$, and link them into $F_{i+1}$.
       c. Using `has_level_i_nontree_edge` in $F_i$, iterate over level-$i$ non-tree edges incident to vertices in $T_u$:
          - If candidate edge $e^* = (x, y)$ connects $T_u$ to $T_v$ (i.e., `ETT_root_Fi(x) != ETT_root_Fi(y)`): remove $e^*$ from the level-$i$ non-tree adjacency list, make $e^*$ a level-$i$ tree edge, link $e^*$ into forests **$F_0$ through $F_i$ ONLY** ($F_{i+1} \dots F_\ell$ remain split), and **STOP** the search.
          - Otherwise (both endpoints lie in $T_u$): move the candidate edge from level $i$'s non-tree list to level $i+1$'s non-tree list and update flags in $F_{i+1}$.
  - `Connected(u, v)`: $\mathcal{O}(\log V)$ by comparing `ETT_root_F0(u) == ETT_root_F0(v)`.
  - `ComponentStats(u)`: $\mathcal{O}(\log V)$ by finding `ETT_root_F0(u)` and reading its root aggregates.

# DISTRIBUTED ARCHITECTURE
- Sharding & Partitioning:
  - Tenants never share graph state.
  - Kafka partitions are the unit of shard assignment: the shard map in etcd assigns `kafka_partition -> shard_id`, and `tenant_id -> kafka_partition` uses a deterministic hash, with explicit override pins in the shard map for hot tenants consulted by a custom producer partitioner.
- Replica group per shard: 3 processes (1 leader + 2 followers). The leader is the single writer for its shard.
  - Leader pipeline: validates request -> checks producer `(producer_id, sequence)` dedup table -> assigns monotonic `(epoch, lsn)` and `event_time_ms = max(local_clock_ms, last_wal_event_time_ms)` -> processes due timing-wheel expirations & hub policy -> appends event batch to local WAL (`fsync` with batched group commit, CRC32 checksum per frame) -> replicates WAL frames to followers over gRPC -> applies to in-memory engine.
  - Ack modes: configurable as `ASYNC` (ack after local leader WAL fsync + apply) or `QUORUM` (ack after leader + at least 1 of 2 followers have fsynced the WAL frame).
  - Follower reads: Followers apply the identical deterministic WAL stream (`Tick`, `Expire`, `HubSuppress`, and `HubRestore` arrive as explicit WAL records, so followers never run independent timing wheels) and serve read-only queries with `applied_lsn` headers.
  - Snapshots & compaction: Periodic binary snapshot of engine state + dedup map + timing wheel + producer sequence table + `(last_epoch, applied_lsn)` via atomic temp-file write + `rename`. On startup, load latest snapshot, replay WAL tail, and truncate obsolete WAL segments. Corrupt trailing WAL frames (from a torn write) are truncated cleanly on recovery.
- Leader election, epoch fencing & log reconciliation:
  - Managed via etcd leases (`/shards/<shard_id>/leader`). Each election yields a strictly increasing `epoch`.
  - Every WAL record and replication RPC carries `(epoch, lsn)`. Followers immediately reject any RPC with `msg.epoch < follower.current_epoch`.
  - Before a newly elected leader accepts any client or Kafka writes, it MUST execute 3-step log reconciliation:
    1. Query the last persisted `(epoch, lsn)` from reachable peers in the replica group.
    2. If a peer has a more advanced log (ordered lexicographically by `(epoch, lsn)`), fetch and append the missing WAL suffix from that peer so the new leader holds the most advanced log in the quorum.
    3. Instruct followers to truncate any uncommitted WAL suffix that conflicts with the new leader's log at the divergence point, then replicate the leader's authoritative tail.
  - Document in `PLAN.md` how quorum intersection (2 of 3 replicas for write ack + 2 of 3 replicas queried during election reconciliation) guarantees zero lost acknowledged `QUORUM` writes.
- Stateless Router:
  - Watches `/shard_map` and `/shards/*/leader` in etcd.
  - Routes writes (`UpsertEdge`, `DeleteEdge`, `UpdateVertex`) to the shard leader and load-balances reads (`Connected`, `ComponentStats`, `GetComponentSubgraph`, `AuditAt`) across replicas (respecting optional `min_lsn` read-your-writes barriers).
- Kafka Ingestion & Crash-Safe Deduplication:
  - Shard leader's consumer commits Kafka offsets only AFTER the corresponding WAL batch is fsynced (at-least-once delivery).
  - Every ingested WAL record persists `(producer_id, sequence)`. The engine rebuilds its producer deduplication map from snapshots + WAL replay on recovery, ensuring effectively-once state transitions across crashes and failovers.
- Backpressure: Bounded queues per shard; leader sheds load with `RESOURCE_EXHAUSTED` and increments `shed_requests_total` when uncommitted queue depth or replication lag exceeds watermarks.

# gRPC API CONTRACT
Define in `proto/fraud_engine.proto`:
- `UpsertEdge(UpsertEdgeRequest) returns (MutationResponse)` (returns `epoch`, `applied_lsn`, `alert_triggered`)
- `DeleteEdge(DeleteEdgeRequest) returns (MutationResponse)`
- `UpdateVertex(UpdateVertexRequest) returns (MutationResponse)`
- `Connected(ConnectedRequest) returns (ConnectedResponse)` (includes `connected`, `applied_lsn`, `replica_role`)
- `ComponentStats(ComponentStatsRequest) returns (ComponentStatsResponse)` (includes `vertex_count`, `account_count`, `volume`, `flagged_accounts`, `flagged_ratio`, `applied_lsn`)
- `GetComponentSubgraph(SubgraphRequest) returns (SubgraphResponse)` (returns up to `max_nodes` vertices with entity type, volume, `is_flagged`, plus active structural edges between them, `truncated` boolean, and `total_vertices`)
- `SubscribeAlerts(SubscribeAlertsRequest) returns (stream AlertEvent)`
- `AuditAt(AuditAtRequest) returns (AuditAtResponse)` (queries Engine A for `tenant_id`, `timestamp_ms`, `vertex_id`, returning both `ComponentStats` and historical `SubgraphResponse`, plus the timestamp when the component first crossed the alert rule)
- `GetClusterHealth(HealthRequest) returns (ClusterHealthResponse)` (per-shard leader/follower status, `epoch`, `applied_lsn`, lag, ingestion rate)

# TELEMETRY
Expose Prometheus metrics and commit a provisioning-ready Grafana dashboard JSON (`deploy/grafana/dashboard.json`):
- Histograms for p50/p99/p99.9 latency by op (`insert`, `extend_ttl`, `expire`, `manual_delete`, `connected`, `component_stats`, `subgraph`).
- Engine internals: `hdt_replacement_search_levels_traversed`, `hdt_nontree_edges_scanned_total`, `hdt_edge_promotions_total`, `lct_repair_triggered_total`, `lct_repair_subtree_vertices_scanned`, `hub_suppressed_active_count`, `hub_transition_events_total`.
- Distributed internals: `wal_fsync_latency_seconds`, `wal_bytes_written_total`, `replica_lsn_lag`, `snapshot_duration_seconds`, `failover_duration_seconds`, `ingestion_queue_depth`, `shed_requests_total`, `engine_memory_bytes`.

# TESTING (non-negotiable)
1. Unit tests (GoogleTest): ETT split/merge/reroot/self-loop weights, LCT access/makeroot/`virtual_agg`/slab recycling, DSU rollback, timing wheel `(expiry_ms, handle_id)` ordering & lazy invalidation, dedup map, hub hysteresis (`tau_high` / `tau_low`).
2. Two-Tier Differential Fuzzer (with automatic delta-debugging shrinker that dumps minimal failing event traces):
   - **Tier 1 (Exact Step-by-Step Oracle):** $V \le 200$, $100,000$ mutations across random and adversarial generators (repeated tree-edge cuts, cycle saturation, hub formation/restoration, heavy repeat-transaction TTL extensions racing expirations with equal timestamps). Compares Brute-Force Oracle, Engine B, and Engine C after EVERY mutation, and verifies Engine A across historical timestamps at the end of each batch.
   - **Tier 2 (High-Scale Differential):** $V = 50,000$, $10,000,000$ mutations comparing Engine B and Engine C online (and Engine A on sampled checkpoints), with Brute-Force disabled.
3. Determinism test: Replay one WAL containing 500k mixed events (including `Tick`, `Expire`, `HubSuppress`/`HubRestore`, and duplicate `(producer_id, sequence)` records) on two fresh instances; in-memory state checksums and serialized snapshots must be byte-identical.
4. Chaos & Recovery tests in Docker Compose:
   - Torn-WAL recovery: append corrupt trailing bytes to a WAL segment, restart, verify clean truncation and exact state match.
   - Divergent-log & failover chaos: inject uncommitted entries on an isolated leader before killing/partitioning it, elect a new leader, verify 3-step log reconciliation truncates the stale uncommitted suffix on rejoining, verify zero acknowledged `QUORUM` writes are lost, and verify duplicate Kafka deliveries are deduplicated via WAL-restored sequence numbers.

# FRONTEND (ops & investigation console — Phase 8)
Purpose: demo and debugging tool, not a product. Time-box this phase and do not let it delay engine or distributed work.
- Stack: React 18 + TypeScript + Vite, Tailwind CSS, `d3-force` rendered on HTML5 `<canvas>` (never SVG/DOM nodes) so 5,000+ nodes stay smooth.
- Backend access: Add a thin BFF gateway in front of the Router exposing REST for queries and a WebSocket for the alert stream. The browser never speaks raw gRPC.
- Views:
  1. **Live Alerts:** Streaming feed of `AlertEvent`s (account_count, flagged_ratio, volume), newest first; click opens Component Explorer.
  2. **Component Explorer:** Canvas force-directed graph of one component (`GetComponentSubgraph`, cap rendering at ~2,000 nodes; show "+N more" badge beyond). Node shape & color by entity type (account = circle `#4F8CFF`, card = square `#A78BFA`, device = diamond `#2DD4BF`); node size by $\log(1 + \text{volume})$; `#F04438` red ring for flagged accounts. Side panel: `ComponentStats` and follower `applied_lsn` staleness age.
  3. **Audit Time Travel:** Tenant + vertex input, time slider calling `AuditAt` to render the component as it looked at time $t$, highlighting the moment it first crossed the alert threshold.
  4. **Cluster Health:** Shard topology (leader/follower per shard, `epoch`), `applied_lsn` and replica lag, ingestion rate, embedded Grafana panel for p99 latency, and a dev-mode "Kill Leader" button wired to the chaos harness.
- Design tokens (dark theme default, CSS variables / Tailwind theme):
  - Background: `#0B0F14`, Surface: `#121820`, Elevated: `#19212B`, Border: `#243040`
  - Text: `#E6EDF3`, Muted text: `#8B98A5`, Primary accent: `#4F8CFF`
  - Success: `#2FBF71`, Warning: `#F5A524`, Danger: `#F04438`
  - Entity colors: account `#4F8CFF`, card `#A78BFA`, device `#2DD4BF`
  - Fonts: `Inter` for UI, `JetBrains Mono` for IDs, LSNs, and numbers. Base size 14px, 8px spacing grid, 8px corner radius, 1px borders, no gradients, no heavy shadows.
- Accessibility & quality: Never rely on color alone (use shape + text labels); WCAG AA contrast; keyboard-navigable lists with visible focus rings; loading, empty, and error states for every view; no hardcoded mock data in the final build (seed through the real load generator).

# BENCHMARKS (Phase 9)
- Baselines: naive BFS per query; full DSU rebuild per expiry batch; Engine B (LCT-MSF with repair); Engine C (HDT).
- Workloads: random sparse, power-law (hub accounts/devices), adversarial tree-edge cuts, varying window and event rate, high repeat-transaction rate.
- Unpredictable-deletion crossover sweep: Sweep the manual deletion fraction from `0%` to `10%` (`0.0%`, `0.1%`, `0.5%`, `1%`, `2%`, `5%`, `10%`) and plot latency crossover between LCT-MSF-with-repair and HDT. Do not tune workloads toward an expected outcome; if no crossover appears, report that honestly.
- Distributed benchmarks: throughput scaling vs. shard count (1, 2, 4, 8 shards), replication lag under load, failover time, and p99 latency under steady load and during failover.
- Output CSVs and plots to `/bench/results/` with the exact command and git commit hash recorded. NEVER invent or estimate numbers; only report what you measured.

# PHASES AND GATES (execute strictly in order; stop at each gate)
- **Phase 0 — Plan:** Read this prompt, propose the repo layout, exact data-structure node layouts, WAL binary format, 3-step log reconciliation protocol, and interfaces (`Engine`, `Event`, `WAL`, `Router`) in `PLAN.md` and `DECISIONS.md`. Set up `Dockerfile.dev` and root `CMakeLists.txt`. **GATE:** `cmake -B build && cmake --build build` succeeds. **STOP after Phase 0 and wait for my approval of `PLAN.md` before writing Phase 1 code.**
- **Phase 1 — Core Layer & Brute-Force Oracle:** Implement `VertexId` encoding, `Event` format, dedup map, timing wheel (`(expiry_ms, handle_id)` total order + lazy invalidation), hub suppression policy layer, and Brute-Force Oracle. **GATE:** Unit tests green; record measured throughput in `PROGRESS.md`.
- **Phase 2 — Engine A (Offline Replay):** Implement segment tree over time + rollback DSU with edge and `VertexUpdate` intervals. **GATE:** Matches Brute-Force Oracle on 100k events ($V=200$) and runs 1M random events ($V=20,000$); record the measured runtime in `PROGRESS.md`.
- **Phase 3 — Engine B (LCT-MSF with Repair):** Implement splay-based LCT, $2V-1$ slab allocator, `virtual_agg`, `(expiry_ms, handle_id)` path-min, and max-expiry crossing-edge repair (used for both manual deletes and hub suppression). **GATE:** Differential fuzzer green for Tier-1 (100k mutations vs Brute-Force & Engine A) and Tier-2 (10M mutations on $V=50,000$ vs Engine A checkpoints); record measured numbers in `PROGRESS.md`.
- **Phase 4 — Engine C (ETT + Layered HDT):** Implement edge-based ETT with permanent `(v, v)` self-loop nodes, level bitflags, and layered HDT replacement search. **GATE:** Differential fuzzer green for Tier-1 (100k mutations vs all engines) and Tier-2 (10M mutations on $V=50,000$ vs Engine B and Engine A). If HDT does not pass, do not proceed; report exact shrunk failing seeds. Record measured numbers in `PROGRESS.md`.
- **Phase 5 — Single-Node Service:** Implement WAL (with `Tick`, `Expire`, `(producer_id, sequence)`), snapshots, crash recovery, gRPC API (`proto/fraud_engine.proto`), and Prometheus metrics. **GATE:** Determinism test (500k WAL replay) + torn-WAL crash-recovery test pass; record measured numbers in `PROGRESS.md`.
- **Phase 6 — Replication, etcd Election & Log Reconciliation:** Implement 1-leader/2-follower WAL streaming (`ASYNC` and `QUORUM`), etcd lease election with epoch fencing, 3-step log reconciliation with follower suffix truncation, and follower reads. **GATE:** Chaos tests for leader kill, divergent uncommitted suffix truncation, and network partition pass with zero lost acknowledged `QUORUM` writes; record failover time in `PROGRESS.md`.
- **Phase 7 — Router, Shard Map & Kafka Ingestion:** Implement stateless consistent-hashing router (`partition -> shard`, custom producer partitioner for hot-tenant pins) and Kafka consumer with offset-commit-after-fsync, WAL-backed producer deduplication, and backpressure shedding. **GATE:** Multi-tenant integration test across 4 shards passes; record measured throughput in `PROGRESS.md`.
- **Phase 8 — BFF Gateway & React Canvas Ops Console:** Implement the thin REST/WebSocket BFF and 4-view React/TypeScript/Tailwind/Canvas UI wired into `docker-compose.yml`. **GATE:** `docker compose up` starts the full stack including UI; scripted demo injects a synthetic fraud ring that produces an alert in Live Alerts and opens in Component Explorer, Audit Time Travel, and Cluster Health.
- **Phase 9 — Benchmarks, Profiling, Grafana & Final README:** Run all benchmark suites, record `perf`/flamegraph findings in `PROFILING.md`, commit Grafana dashboard JSON and k8s manifests, and write the final `README.md` with architecture diagram, measured results, and an honest limitations section. **GATE:** Reproducible with one documented command.

# RULES
- Work in small, reviewable commits. Run the full test suite (`ctest --output-on-failure`) before every commit.
- If a requirement here is ambiguous or contradicts itself, ask one concise question. Otherwise choose the simpler option and record the decision in `DECISIONS.md`.
- After completing Phase 0 (`PLAN.md`), stop and wait for my explicit approval before starting Phase 1.
- Do not add features not listed here. Do not use external graph or connectivity libraries; implement ETT, LCT, HDT, and Rollback DSU yourself.
- Keep hot-path code allocation-free where possible. Profile before optimizing (`perf`, flamegraphs) and record findings in `PROFILING.md`.
- In `README.md`, state exactly which parts are implemented and tested. Anything not passing its gate is listed as unfinished.
