# CPU and memory profile

The runs use the pinned Debian Release build on the Docker Desktop Linux VM. GNU gprofng sampled the complete executable, including graph construction, clock calls and percentile sorting. Each engine ran 16 fresh 50,000-vertex traces with 500,000 mutations and one component query every 16 mutations. Five percent of mutations manually remove a bridge. Rebuilding the initial graph between traces exercises early HDT promotions as well as subsequent searches. The sweep and throughput tables were collected separately with the profiler disabled.

```sh
python bench/reproduce.py
```

The [source revision, image and seed](bench/results/run.json) identify the run. The collector command inside the container is `gprofng collect app -p hi -o /tmp/LCT.er ./build/phase9_bench LCT`, with the corresponding HDT arguments for Engine C. `gprofng display text -functions` and `-calltree` produce the archived reports. Sampling the optimized binary attributes inlined arithmetic to its enclosing symbol. A virtual aggregate adjustment has no separately measurable symbol here.

## CPU samples

**LCT.** `splay` led exclusive CPU at 29.11%, followed by `pull` at 17.61%. TTL extension accounted for 64.32% inclusive, and `access` for 48.12% inclusive. Access repeatedly splays nodes and transfers child aggregates between preferred and virtual subtrees. Manual repair accounted for 19.01% inclusive. Each profile trace visited 3,125,000 split-side vertices in 25,000 repairs and inspected 1,537,594 candidate incidences. [Function samples](bench/results/LCT-functions.txt) · [Call tree](bench/results/LCT-calltree.txt)

**HDT.** ETT slot allocation led exclusive CPU at 16.67%, followed by the TTL existence lookup at 15.00% and treap `pull` at 13.33%. The split symbol accounted for 11.67% inclusive, the main merge symbol for 3.33% inclusive and HDT replacement search for 7.50% inclusive. Each trace traversed 35,000 search levels and promoted 30,950 edges. The central bridges have no crossing replacement. Repeatedly cutting the same bridges lets HDT reuse earlier promotions. Random chords in the separate windowed benchmark also exercise successful replacements. [Function samples](bench/results/HDT-functions.txt) · [Call tree](bench/results/HDT-calltree.txt)

gprofng attributed 0.425 s of sampled CPU to LCT and 0.120 s to HDT. Treat small differences among individual HDT symbols cautiously. Timing instrumentation itself is visible in the HDT profile.

## Memory at 50,000 vertices

The 5% sweep point ended with 62,099 live structural edges. The LCT slab owns 99,999 nodes of 136 B each. A mapped LCT edge value occupies 32 B before its hash key and node overhead. Each ETT node is 56 B with a separate 48 B weight record at level zero; a mapped HDT edge value is 40 B before its dynamic dart vector storage. [Compiler measurements](bench/results/sizes.csv)

| Engine | Index bytes | Slab bytes | Index B / vertex | Index B / live edge |
| --- | ---: | ---: | ---: | ---: |
| LCT | 34,265,792 | 13,599,864 | 685.3 | 551.8 |
| HDT | 97,986,024 | 90,177,544 | 1959.7 | 1577.9 |

HDT slab bytes include the level forests' node and weight capacities, loop indices and free lists. LCT slab bytes count the real/dummy node array. [Index and scan counters](bench/results/engines.csv)

Engine accounting includes owned slab/vector capacity, mapped values and hash bucket pointers. The estimate excludes allocator headers, implementation-specific hash/tree node links, stack, tenant policy, WAL and networking. The slab column is a subset of the index column. Normalizing the same index by the vertex count or edge count gives alternative views of this graph's footprint. Neither ratio is a marginal allocation cost.

## Optimization history

Phase 9 added byte-size accessors to inspect the existing structures. The measured hot paths retain the approved Phase 4 algorithms. The call trees and scan counters identify where another optimization experiment could be measured; no speedup is attributed to this release.
