# Streaming fraud-ring detection engine

Phase 1 provides an in-memory, single-writer tenant graph and a breadth-first reference engine. It accepts typed vertex updates, edge upserts, deletes and ticks. Each accepted command returns its ordered event records and alerts. Queries provide connectivity, component aggregates and bounded induced subgraphs. Hub suppression is a signal-quality policy. Queries report hub-filtered connectivity, while active degree counts all live incident keys.

```sh
docker build -f Dockerfile.dev -t fraud-ring-dev .
docker run --rm -w /workspace/build fraud-ring-dev ctest --output-on-failure
docker run --rm fraud-ring-dev ./build/phase1_bench
```

For a library consumer, instantiate `fraud::Tenant<fraud::BruteForce>` from `core/tenant.hpp` and `engines/brute_force.hpp`. Pass the epoch, first available shard LSN, monotonic event time and command to `submit`. Advance the next LSN by the number of returned records. Use `engine().stats(id)`, `engine().connected(u,v)` and `subgraph(id,max_k)` for queries. The reference engine runs BFS on each query.

Engine A builds an immutable offline index with `OfflineEngine::build(tenant, policy, records, end_ms)` from `engines/offline.hpp`. Supply complete ordered tenant records beginning at empty state, including explicit expiry and hub transitions. The end time is exclusive. Records sharing a timestamp produce one observable historical state after the final record at that timestamp.

Use `audit_at(time, vertex, max_nodes)` for component stats, an induced subgraph and the earliest qualifying timestamp for that vertex. Qualification follows the alert thresholds independently of emission cooldown. `connected_at(time, u, v)` checks historical connectivity. Use `audit_many(queries)` for checkpoints across a trace so they share a segment-tree traversal. Each call initializes vertex-sized rollback workspace and applies the interval actions on visited paths.

Engine B is available as `Tenant<LctEngine> graph(tenant_id, policy, LctEngine(vertex_capacity))` with `engines/lct.hpp`. The fixed capacity counts distinct vertices ever admitted. A rejected capacity increase leaves the tenant state intact. Its maximum spanning forest orders edges by expiry and handle. Manual and suppression cuts search the smaller tree for the best replacement. Ordered expiry uses a direct cut. The slab bound of `2V - 1` covers LCT nodes. The live edge index and adjacency lists also use memory proportional to the number of structural edges. LCT queries adjust preferred paths, so callers must serialize access to an instance.

Build the development image and run the two Phase 3 gates from the repository root. The bind mount retains a failure trace if a mismatch occurs.

```sh
docker build -f Dockerfile.dev -t fraud-ring-phase3 .
docker run --rm -v "${PWD}:/traces" -w /traces fraud-ring-phase3 /workspace/build/phase3_fuzz tier1
docker run --rm -v "${PWD}:/traces" -w /traces fraud-ring-phase3 /workspace/build/phase3_fuzz tier2
```

Each tier accepts an optional numeric seed. On a mismatch the runner removes groups of input mutations and then individual mutations while preserving the failing check. The resulting trace is 1-minimal under single-mutation deletion. Its header records the tier and seed. Each line includes the original generator step and decoded command. Run `/workspace/build/phase3_fuzz replay TRACE` in the same image and mounted directory to reproduce it.

Engine C is available as `Tenant<HdtEngine> graph(tenant_id, policy, HdtEngine(vertex_capacity))` with `engines/hdt.hpp`. It maintains one Euler tour forest per HDT level. Each admitted vertex has a permanent self-loop at each level. Level zero owns the fraud weights. Exact-level flags locate tree edges and incident non-tree edges for replacement searches. Deleting any structural edge uses the same HDT path for manual, expiry and suppression events. A graph self-edge remains visible in the tenant subgraph and cannot cross a cut.

Run `phase4_fuzz tier1` and `phase4_fuzz tier2` in the development image to verify Engine C. Tier 1 runs the 100,000-mutation workload plus 8,192 adversarial mutations. Tier 2 compares full probed components and connectivity online after every input batch, with Engine A at sampled checkpoints. Both tiers use the same trace-reduction and replay interface as Phase 3.

### Single-node service

The Linux service uses HDT by default. Select LCT with `--engine lct`. The following commands publish gRPC and Prometheus on the host loopback interface and keep state in a Docker volume.

```sh
docker build -f Dockerfile.dev -t fraud-ring-phase5 .
docker run --rm -p 127.0.0.1:50051:50051 -p 127.0.0.1:9091:9091 -v fraud-ring-data:/data fraud-ring-phase5 ./build/fraud_server --data /data --listen 0.0.0.0:50051 --metrics-address 0.0.0.0
```

Use [the public protobuf contract](proto/fraud_engine.proto) for clients and scrape `http://127.0.0.1:9091/metrics`. The service exposes all nine public RPCs over plaintext gRPC. A successful mutation returns its durable frame boundary. Concurrent requests can share one fsync. A retried producer sequence returns the current durable watermark with `duplicate=true`. Unknown vertices return false for connectivity and `NOT_FOUND` for stats, subgraph and audit. Live subscribers receive committed alerts from subscription onward. A subscriber that falls behind the 4,096-alert buffer receives `RESOURCE_EXHAUSTED`.

The writer assigns monotonic event times and ticks active tenants every 100 ms, including tenants with no new requests. Adjust the interval with `--tick-ms`. The default snapshot interval is 60 seconds and is configurable with `--snapshot-ms`. One process owns each data directory. A storage failure makes the shard unavailable until restart.

`active.wal` contains little-endian FRE1 frames with IEEE CRC32. Recovery checks the 16 MiB payload and 65,536-record limits before allocating. It repairs a torn active tail and refuses interior damage, unsupported versions, and damaged sealed archives. The FRS1 snapshot contains a versioned canonical protobuf payload and CRC32. Snapshot publication syncs the temporary file and its directory before the covered active segment is renamed to an archive. Startup reads retained WAL for historical queries and applies only the suffix after the snapshot to the live graph.

Phase 5 retains the full audit history on disk and in memory. Each historical query builds Engine A over the requested tenant's prefix. Disk usage and audit cost therefore grow with retained history. The operation histograms measure engine operation time. Separate histograms measure fsync and snapshot duration. The engine-memory gauge counts live index container storage and excludes allocator overhead.

```sh
docker run --rm -w /workspace/build fraud-ring-phase5 ctest --output-on-failure
docker run --rm fraud-ring-phase5 ./build/storage_tests --gtest_filter=Wal.*
docker run --rm fraud-ring-phase5 ./build/phase5_determinism
docker run --rm fraud-ring-phase5 ./build/phase5_smoke
docker run --rm fraud-ring-phase5 ./build/phase5_smoke lct
```

Phases 6–9 are unfinished, including replication, routing, Kafka ingestion, and the console. [Progress and measurements](PROGRESS.md) · [Design](PLAN.md)

### Replicated shard

Run one `fraud_replica` process per member with a separate durable directory. Supply the same shard ID, etcd endpoint and three ordered gRPC addresses on each process. The member index is 0, 1 or 2. For example, the first member on a local machine can start with the following command.

```sh
./build/fraud_replica ./data0 0 0 http://127.0.0.1:2379 3 100 127.0.0.1:5100 127.0.0.1:5101 127.0.0.1:5102 9100
```

The two time arguments are lease TTL in seconds and leader tick interval in milliseconds. The last argument sets the optional HTTP metrics port. Start the other two members with their respective directory, member index and metrics port. Public and internal gRPC share each member's address. This configuration assumes a trusted network and fixed membership.

Read the local role, promised epoch, last entry epoch, last LSN and commit LSN with `GetClusterHealth`. Route writes to the member reporting `LEADER`. The default acknowledgment mode is `QUORUM`. Setting `mode=ASYNC` permits local durability during a peer partition. A later election can discard that suffix. Pass `min_lsn` on a read to receive `UNAVAILABLE` while a replica is behind. Successful reads return the applied watermark and role in both the response and gRPC trailing metadata.

Replicas retain FRE1 frames for catch-up and historical audit. Recovery rebuilds the committed graph from those frames. Disk usage, replay time and in-memory log storage grow with history length. The chaos suite starts independent replica processes with disposable directories and gives each peer and lease connection a controllable TCP proxy. Run it in Compose with a dedicated etcd service.

```sh
docker compose -f compose.phase6.yml up --build --abort-on-container-exit --exit-code-from chaos
docker compose -f compose.phase6.yml down
```
