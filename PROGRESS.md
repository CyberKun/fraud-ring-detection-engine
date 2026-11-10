# Milestone status

Phase 3 passed the LCT maximum-spanning-forest gate. Phases 4–9 are unfinished.

## Phase 3 verification

Linux amd64 Debian development image, GCC 12.2.0, CMake Release. Both timed runs used seed 20261111. Elapsed time covers the entire differential run including query comparison and offline indexing.

```text
> docker build -f Dockerfile.dev -t fraud-ring-phase3 .
> docker run --rm -w /workspace/build fraud-ring-phase3 ctest --output-on-failure
100% tests passed, 0 tests failed out of 25
Total Test time (real) =   0.31 sec

> docker run --rm -v "${PWD}:/traces" -w /traces fraud-ring-phase3 /workspace/build/phase3_fuzz tier1
PASS tier=1 seed=20261111 vertices=200 mutations=100000 records=137821 audits=137821 mismatches=0 elapsed_s=3.479
expire=34703 suppress=1559 restore=1559 cycle_swaps=40946 repairs=13160 repair_vertices=45638 repair_promotions=9938 peak_dummies=51 kruskal_checks=197

> docker run --rm -v "${PWD}:/traces" -w /traces fraud-ring-phase3 /workspace/build/phase3_fuzz tier2
PASS tier=2 seed=20261111 vertices=50000 mutations=10000000 records=14348892 audits=4884 mismatches=0 elapsed_s=71.145
expire=4037954 suppress=155469 restore=155469 cycle_swaps=3863413 repairs=1232011 repair_vertices=4244322 repair_promotions=909037 peak_dummies=2085 kruskal_checks=0
```

Tier 1 compares a probed vertex's aggregate and complete induced subgraph, including edge versions, against BFS and Engine A after each WAL record. It also checks pair connectivity and compares B's alerts against BFS. The historical checker uses the LSN as a distinct query coordinate so equal-time intermediate states are testable. A separate descending-key Kruskal reconstruction checks the actual selected tree handles and dummy count periodically.

Tier 2 keeps a continuous 50,000-vertex graph and checks B against A every 2,048 mutations and at the end. Engine A processes consecutive windows with carried policy state. Its checkpoints include connectivity, all component aggregates and complete induced subgraphs. The workload mixes cycle saturation, tied expiries, repeated TTL extensions, manual cuts, hub transitions, cross-group edges and vertex updates. The fixed slab test separately fills all 19 dummy slots for 20 vertices, empties them, and repeats for 20 rounds.

The reducer ran end-to-end in a disposable copy of each harness with a deliberately incorrect comparison. Both eight-mutation inputs produced replayable two-mutation traces. Dropping either remaining mutation removed the mismatch. The production runs above used the unmodified comparison.

```text
tier=1 injected=8 shrunk=2 replay=reproduced all_single_deletions=pass
tier=2 injected=8 shrunk=2 replay=reproduced all_single_deletions=pass
```

## Phase 2 verification

Single runs in the Linux amd64 Debian development image, GCC 12.2.0, CMake Release. The index consumes the same structural transitions as the live writer. Unit tests exercise rollback, disjoint and open-ended lifespans, TTL metadata, overlapping suppressed hubs, duplicate records, bounded subgraphs and first crossings across splits and weight changes.

```text
> docker build -f Dockerfile.dev -t fraud-ring-phase2 .
> docker run --rm -w /workspace/build fraud-ring-phase2 ctest --output-on-failure
100% tests passed, 0 tests failed out of 18
Total Test time (real) =   0.33 sec

> docker run --rm fraud-ring-phase2 ./build/phase2_check differential
PASS mode=differential seed=20261111 vertices=200 timestamps=100000 records=142271 audits=100000 mismatches=0
expire=41363 suppress=454 restore=454
generate_oracle_s=0.440 build_s=0.152 audit_batch_s=0.108 total_s=0.722

> docker run --rm fraud-ring-phase2 ./build/phase2_check scale
PASS mode=scale seed=20261111 vertices=20000 timestamps=777942 records=1000000 audits=1520 mismatches=0
expire=222057 suppress=1 restore=0
generate_oracle_s=2.186 build_s=2.558 audit_batch_s=0.332 total_s=5.080
```

The differential workload issues one query per timestamp and compares component aggregates, pair connectivity, the full induced multigraph with edge handles, versions and TTLs, vertex flags and weights, and the earliest qualifying timestamp against the BFS oracle. Vertex selection alternates between random queries and recently touched endpoints after initialization. The oracle independently tracks first qualification per vertex.

The scale workload builds the index, including the first-crossing pass, over exactly one million records and verifies connectivity, aggregates and subgraphs at 1,520 sampled checkpoints. The elapsed times above separate workload/oracle generation, index construction and batched queries. The scale workload exercised one hub suppression. Repeated suppression and restoration were exercised by the differential workload.

## Phase 1 verification

Measured in the Linux amd64 development image built with `docker build -f Dockerfile.dev -t fraud-ring-dev .`, Debian Bookworm, GCC 12.2.0, CMake Release. The suite includes 11 behavioral GoogleTests and the original toolchain check.

```text
> docker run --rm -w /workspace/build fraud-ring-dev ctest --output-on-failure
100% tests passed, 0 tests failed out of 12
Total Test time (real) =   0.21 sec

> docker run --rm fraud-ring-dev ./build/phase1_bench
commands=200000 records=259904 expiries=59904 alerts=256 elapsed_s=0.377 commands_per_s=530208
```

Single measured run, one in-memory writer with the BFS oracle. The deterministic workload cycles through 64 independent five-account groups and includes upserts, extensions, vertex overwrites, manual deletes and ticks. The timer covers command processing, expiry and alert generation, plus record counting. Container startup is outside the timed region. Large connected components, hub churn and distributed throughput require separate measurements.

## Phase 0 verification

Windows, CMake 4.3.3, MSVC 19.50.35728.0, Debug configuration. Actual command output, with build paths omitted:

```text
> cmake -B build
-- Configuring done (0.1s)
-- Generating done (0.1s)
> cmake --build build
  toolchain_smoke.vcxproj -> D:\Fraud Ring Detection Engine\build\Debug\toolchain_smoke.exe
> ctest --test-dir build -C Debug --output-on-failure
1/1 Test #1: toolchain_smoke ..................   Passed    1.55 sec
100% tests passed, 0 tests failed out of 1
Total Test time (real) =   1.56 sec
```

The Phase 0 test checked compilation and linking. Windows Application Control blocked direct execution of the unsigned smoke executable. Phase 1 subsequently built and ran the behavioral suite in Docker.
