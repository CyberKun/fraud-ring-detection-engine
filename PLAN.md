# Streaming fraud-ring detection engine

## Repository and ownership

This is the Phase 0 design. Later milestones introduce directories as their first implementation lands.

| Path | Responsibility |
| --- | --- |
| src/core, src/engines | Tenant events, scheduling, policy, reference BFS, historical and live indexes |
| src/storage, src/replica | WAL, snapshots, durable election promises, replication and recovery |
| proto, src/service, src/router | Public and peer gRPC contracts, tenant routing, ingestion, metrics |
| gateway, console | REST/WebSocket gateway and React investigation console |
| tests, tools | Unit, differential, deterministic replay and chaos tests, shrinker and load generator |
| deploy, bench/results | Compose stack, dashboards, final Kubernetes manifests and measured benchmarks |

## Event and engine interfaces

One serialized writer per shard owns tenant states. External identities and timestamps use unsigned 64-bit integers. Vertex type occupies the top two bits. Each tenant maps external IDs to dense 32-bit indices. Forests use zero as the null node. All tree links are indices into contiguous slabs. Each engine has its own structural-edge index. Shared policy owns the authoritative normalized edge and incident-handle maps, including shadow edges.

```cpp
// Proposed value interfaces, shown without implementation.
using Payload = std::variant<EdgeUpsert, EdgeDelete, VertexUpdate, Tick,
                             Expire, HubSuppress, HubRestore>;
struct Event {
    uint64_t tenant, epoch, lsn, event_time_ms;
    std::optional<ProducerSequence> source;
    Payload payload;
};
template<class E>
concept DynamicGraphEngine = requires(E& e, VertexId u, VertexId v,
                                      Handle h, Weight w, Expiry x, Removal r) {
    { e.set_vertex(u, w) } -> std::same_as<Status>;
    { e.insert_edge(h, u, v, x) } -> std::same_as<Status>;
    { e.extend_ttl(h, x) } -> std::same_as<Status>;
    { e.delete_edge(h, r) } -> std::same_as<Status>;
    { e.connected(u, v) } -> std::same_as<bool>;
    { e.stats(u) } -> std::same_as<Result<Aggregate>>;
};
```

Select the live engine with a variant of LCT and HDT. The BFS reference satisfies the same concept in tests. OfflineEngine accepts bounded structural/weight intervals and timestamped queries. Subgraph extraction asks the selected index for up to K component vertices and walks structural adjacency for induced edges. Budget is proportional to returned vertices and inspected incident edges. Stats supply the exact total even when truncated. No vertex enumeration for ordinary live connectivity or stats.

## Tenant state and transitions

EdgeRecord fields are handle:u64, version:u64, expiry:u64, endpoints:u32[2], type:u32 and shadow:bool. Handles increase monotonically per tenant. Vertex records contain external ID, volume:u64, flag:bool, suppressed:bool, optional last_alert_ms:u64 and active degree. Maintain structural and live adjacency separately. Keep vertices after their last edge expires. The four unsigned 64-bit aggregate fields in order are vertex_count, account_count, volume and flagged_accounts. Validate volume against a checked tenant-wide total before mutation. Only accounts contribute flags. Ratio divides by account_count with a zero-denominator result of zero.

Use a bucketed timing wheel with an ordered index of nonempty absolute-time buckets. Drain due entries sorted by (expiry, handle), checking version against the current map. Only the leader schedules records. At each monotonic batch time drain valid expiries, apply each resulting policy transition, then process the input. A longer TTL increments the version. Manual deletion retires the handle. Hub policy applies only to cards/devices and counts all live keys. At degree 50 suppress, at 35 or below restore. Update edges in handle order. A surviving edge becomes structural only when both endpoints are unsuppressed. The forest intentionally represents hub-filtered connectivity for signal quality.

Stage the batch on a bounded mutation journal or equivalent isolated writer view. Preflight capacity and produce explicit policy events before persistence. Publish complete applied batches to readers. For each logical mutation capture pre-rule values at its affected vertices, recompute distinct resulting components, and emit once per qualifying result if an eligible touched vertex has cooled down. Newly flagged accounts also qualify. A hub transition touches its neighbors as well as the hub. Update eligible touched vertices in the emitted component. Store cooldown on vertices. Use subtraction after monotonicity checks for cooldown arithmetic. Alert IDs use shard, LSN and deterministic ordinal.

## Forest storage and algorithms

* **Rollback DSU.** Arrays of nodes {parent:u32, size:u32, sum:Aggregate}. No path compression. For union save losing root, winning root, old winning size and aggregate. For weight changes save root and prior aggregate. Roll back to a stack length in reverse order. Partition half-open structural and weight lifetimes over event indices in a segment tree. Apply each bucket on DFS entry, answer leaves, roll back on exit. Initial vertex presence is a weight interval too. Maintain rollback component membership lists and active adjacency for historical subgraphs. Resolve a timestamp to the last complete batch at or before it. For each queried vertex, find its earliest historical qualifying component by evaluating threshold crossings in time order, including splits.
* **LCT node.** {parent:u32, child:u32[2], reversed:bool, self:Aggregate, sum:Aggregate, virtual_sum:Aggregate, own_key:optional<(u64 expiry,u64 handle)>, min_key_node:u32}. Keep the path minimum over children and own key only. Sum includes virtual contributions. During access add the detached right child's sum to virtual_sum and subtract the newly preferred child's sum. Push reversals before rotations. Real vertices have no key. Dummies have zero weight. Reserve at most V real slots and V−1 recyclable dummy slots for configured vertex capacity V. Track tree adjacency separately to enumerate a split side. Exchange a cycle minimum only for a strictly larger key, including on non-tree TTL extension. Ordered expiry needs only cut. For every other tree removal compare split counts, visit the smaller side and promote the **greatest** crossing key among all candidates.
* **ETT treap node.** {left,right,parent,priority,tie_id,tour_size:u32, vertex:u32, handle:u64, self_count,sum_count:u32, local_flags,subtree_flags:u8}. Level zero also stores self and subtree fraud weights in parallel arrays. Permanent self-loops count once. Both dart orientations count zero. The two flag bits mark an exact-level tree dart and an incident exact-level non-tree candidate. Pull ORs local and children's bits. Generate repeatable priorities from a stored deterministic seed and allocation sequence. Reroot by rotating the tour using split and merge. Cut at the two dart positions, remove them and rejoin the outside pieces.
* **HDT edge.** {u,v,level:u32, tree:bool} plus indexed dart pairs per participating level and endpoint adjacency membership for non-tree edges. With configured V, allocate levels 0 through floor(log2 V) lazily. Each level-i tree spans at most floor(V/2^i) vertices. On tree deletion cut all occupied levels. Descend from the removed level, select the smaller split by root count, use flag-guided searches to promote exact-level internal tree edges first, then internal non-tree edges. The first crossing edge becomes a tree edge only in forests 0 through i. Higher levels stay split. Assert promotion bounds in tests. Represent graph self-edges separately as redundant structural edges to avoid top-level singleton promotion.

## Binary durability and service boundary

WAL methods are append_batch(records), sync_through(lsn), read_suffix(after), truncate_after(lsn) and recover(snapshot). Errors are status values. A frame is one atomic leader batch. All integers are little-endian, packed field-by-field. Header is magic "FRE1" (4 bytes), format:u16=1, flags:u16=0, payload_bytes:u32, count:u32, epoch:u64, first_lsn:u64. Payload consists of count records, each length:u32 followed by tenant:u64, event_time_ms:u64, tag:u8, has_source:u8, optional producer:u64 and sequence:u64, then fixed fields. Record LSN is first_lsn + ordinal. Tags 1..7 follow the Payload declaration order. Upsert carries u,v:u64, type:u32, expiry:u64. Delete omits expiry. VertexUpdate carries v,volume:u64 and flagged:u8. Tick has no fields. Expire carries handle,version:u64. Each hub record carries v:u64. Trailer is IEEE CRC32:u32 over header and payload. Cap frame bytes and counts before allocation. Verify lengths, tags, continuity and CRC. Truncate an incomplete or corrupt final frame. Quarantine interior corruption.

Batch multiple frames per fsync. Follower append RPCs carry leader epoch and preceding log identity. Followers verify the prefix, fsync, persist progress and acknowledge. Replay identical explicit records without regenerating derived events. Canonical binary snapshots at frame boundaries include sorted graph/vertex records, allocator counters, cooldowns, scheduler entries, producer sequence state, policy configuration, log identity and durable commit boundary. Reconstruct equivalent forest indexes from structural edges in stable order. Write temp, fsync, rename and sync the directory before removing covered live WAL segments. Retain archived event history for AuditAt within the advertised retention window.

## Election and reconciliation

Use libcurl and nlohmann/json against etcd v3 lease grant/keepalive, transaction and range endpoints. CAS an absent leader key attached to the lease. Use the successful transaction revision as the new epoch. Poll leader keys and shard map by revision. Loss of lease stops admissions. Election alone does not authorize appending.

1. Obtain a durable promise for the new epoch and the last **entry** (epoch, LSN) from at least two of three replicas including self. Serialize promises against append/fsync and reject stale RPCs from then on, including after restart. A promise never changes the reported last entry term.
2. Select the most advanced promised log lexicographically by last entry term and LSN. Locate its last common frame with the candidate by matching prefix identities, fetch its authoritative suffix or snapshot, and fsync locally. Re-check the lease and epoch. An unavailable required suffix stops promotion.
3. Reconcile peers against that prefix, truncate conflicting uncommitted tails at frame boundaries, install missing frames and fsync on a majority before serving. A conflict below a known committed boundary is a fatal invariant violation. Reconcile returning peers before counting their acknowledgments.

**Quorum reasoning.** Within an epoch a single writer extends one prefix. A write acknowledged by two durable replicas intersects every later two-replica promise set. Among logs eligible in the next successful election, a longer same-term log extends the shorter one. By induction, any log with a later entry term came from a reconciled writer that inherited all earlier quorum-acknowledged prefixes. Durable promises prevent an older writer from collecting a new majority after that point. Thus selecting the freshest eligible log preserves acknowledged QUORUM writes. These premises require fixed membership, durable storage and prefix-checked appends. ASYNC acknowledges local fsync and apply and can lose a suffix on failover.

Router methods resolve(tenant), mutate(request), read(request,min_lsn) and subscribe(tenant) use an immutable revisioned shard map. Stable tenant hash or explicit tenant pin selects a Kafka partition, mapped to a shard. Writes target its fenced leader. Reads select an eligible replica and return applied_lsn. Persist tenant-scoped producer high-water marks in the same batch as accepted mutations. Commit Kafka offsets only after the configured durability boundary. Bound pending bytes, requests and replication lag, reporting RESOURCE_EXHAUSTED on overload.

## Acceptance sequence

Follow the numbered gates in the supplied project brief. Phase 1 tests core transitions and measures throughput. Phase 2 checks historical answers on 100k events and measures 1M. Phases 3 and 4 add differential verification after every mutation and sampled historical checks at 10M scale, recording failing seeds and minimized traces. Phase 5 adds gRPC, metrics, byte-identical 500k replay and torn-tail recovery. Phase 6 exercises quorum failover, partitions and divergent tails. Phase 7 validates four-shard tenant isolation and redelivery. Phase 8 demonstrates all four console views against real service data. Phase 9 records benchmark commands, commit hashes, CSVs, profiles and measured crossover results. Each milestone updates PROGRESS.md and runs CTest before commit.
