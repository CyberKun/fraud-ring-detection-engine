# Benchmark data

Run `python bench/reproduce.py` from the checkout identified by [run.json](run.json). That manifest records the benchmark source commit, image digest, generator seed, Docker machine configuration and any uncommitted benchmark-source changes.

| File | Observations |
| --- | --- |
| [engines.csv](engines.csv) | One row per engine and workload or sweep point. `operations` includes component queries. The edge count and index/slab byte counts are taken at the end of the trace. Byte counters apply to LCT and HDT. |
| [engine_samples.csv.gz](engine_samples.csv.gz) | Chronological nanoseconds for every timed operation, including queries, keyed by topology, engine and manual deletes per thousand mutations. Plotting uses the unprofiled runs. |
| [scaling.csv](scaling.csv), [scaling_requests.csv](scaling_requests.csv) | Router update throughput, nearest-rank client latency and each successful acknowledgement with its applied LSN. |
| [failover_requests.csv](failover_requests.csv) | Per-attempt start, completion, elapsed time, gRPC status and returned epoch. Times are relative to load start. |
| [failover_lag.csv](failover_lag.csv) | Reachable member count, visible leader epoch and maximum sampled follower lag. Blank fields indicate missing observations. Member samples are sequential. |
| [failover.csv](failover.csv), [failover_summary.csv](failover_summary.csv) | Half-second trace bins and the kill-to-first-success interval. The final partial bin uses its actual duration. |
| [sizes.csv](sizes.csv) | Compiler-reported sizes of engine records in bytes. |
| [LCT functions](LCT-functions.txt), [LCT call tree](LCT-calltree.txt), [HDT functions](HDT-functions.txt), [HDT call tree](HDT-calltree.txt) | GNU gprofng samples across the entire process, including graph construction and benchmark bookkeeping. |
| [ctest.txt](ctest.txt) | Full test output with real broker integration enabled. |

The [crossover](crossover.svg), [scaling](scaling.svg) and [failover](failover.svg) figures come directly from these CSVs. Inclusive CPU percentages overlap between a caller and its callees.
