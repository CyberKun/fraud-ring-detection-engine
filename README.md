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

Phases 3–9 are unfinished, including the optimized live forests, durable storage, replication and the console. [Progress and measurements](PROGRESS.md) · [Design](PLAN.md)
