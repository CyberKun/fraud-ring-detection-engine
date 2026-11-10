# Streaming fraud-ring detection engine

Phase 1 provides an in-memory, single-writer tenant graph and a breadth-first reference engine. It accepts typed vertex updates, edge upserts, deletes and ticks. Each accepted command returns its ordered event records and alerts. Queries provide connectivity, component aggregates and bounded induced subgraphs. Hub suppression is a signal-quality policy. Queries report hub-filtered connectivity, while active degree counts all live incident keys.

```sh
docker build -f Dockerfile.dev -t fraud-ring-dev .
docker run --rm -w /workspace/build fraud-ring-dev ctest --output-on-failure
docker run --rm fraud-ring-dev ./build/phase1_bench
```

For a library consumer, instantiate `fraud::Tenant<fraud::BruteForce>` from `core/tenant.hpp` and `engines/brute_force.hpp`. Pass the epoch, first available shard LSN, monotonic event time and command to `submit`. Advance the next LSN by the number of returned records. Use `engine().stats(id)`, `engine().connected(u,v)` and `subgraph(id,max_k)` for queries. The reference engine runs BFS on each query.

Phases 2–9 are unfinished, including offline replay, the optimized forests, durable storage, replication and the console. [Progress and measurements](PROGRESS.md) · [Design](PLAN.md)
