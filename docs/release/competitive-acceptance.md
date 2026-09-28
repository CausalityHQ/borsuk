# Minimum production release acceptance

Updated2026-09-28 UTC. Beating **both Amazon S3 Vectors and Turbopuffer** is a
required release gate. This contract supersedes the historical resident-route
contract/checkmarks in the Rust RC checklist. No current result certifies release.

## One matched workload and no failed-axis averaging

Freeze corpus identity, N, dimensions, metric, k, filters, independent query
split, transport, cache state (especially no-cache), region, hardware,
concurrency and lifecycle before measurement. Retain raw per-query samples,
source/config/artifact hashes and exact terminals for each participant.

- Quality must be at least the stronger relevant competitor. Report mean
  recall and lower-tail recall on the same queries. Exact/tied ceilings require
  no regression; do not require recall above100%.
- At that same recall, BORSUK must beat each competitor's end-to-end p90/p95
  and sustainable throughput under the same tail-latency SLO. Report p50/p99
  from the same raw samples. Include retrieval, authentication, scoring and
  transport. Planner CPU is not serving latency.
- Demonstrate100M+ scale, bounded resident RAM including overlapping pinned
  generations, and no full-vector hydration. Arbitrary application IDs,
  incremental insert/update/delete, recovery and self-contained in-process
  compaction/GC must work and survive failure/reload.
- Report total lifecycle dollars: build/train, storage/replication, compute,
  GET/byte/retry charges, incremental updates, compaction/GC and generation
  overlap. Throughput/$ uses this cost definition, not query CPU alone.

Every axis must pass separately against both vendors. Lower recall, warmer
caches, omitted I/O or averages across failed axes cannot establish a win.
Published vendor results may set dated provisional targets with explicit gaps;
they cannot close the matched-measurement gate. Missing authenticated matched
vendor evidence remains an unresolved final release blocker.

## Current gap-to-both-vendors matrix

| Required axis | BORSUK current evidence | S3 Vectors matched gate | Turbopuffer matched gate | Release status |
|---|---|---|---|---|
| Quality at stronger competitor recall, including lower tail | Native raw-input ReLAION100k D768 cosine k100 validation256–999 (744 reused queries): mean98.850806%, p05 97 hits/100; paired flat SQ8 mean99.568548%. Candidate KILL at internal .5pp deficit gate. CoHere native full validation not run after first failure. | Current object-native revision/corpus/split not compared | No authenticated matched run | OPEN; no winning candidate |
| Same-quality end-to-end p90/p95; p50/p99 | No qualifying cold object-native HTTP run from current complete native pipeline | Same workload/transport/cache/SLO required | Same workload/transport/cache/SLO required | OPEN |
| Sustainable QPS at same recall and tail SLO | No qualifying current-route measurement | Direct matched QPS needed | Direct matched QPS needed | OPEN |
|100M+ bounded RAM without hydration | Compressed codes alone project20GB at100M D768; excludes centroid/graph/overlap/runtime. Not measured total RSS or qualified scale. | Record disclosed resource/lifecycle configuration | Record disclosed resource/lifecycle configuration | OPEN |
| Incremental IDs/insert/update/delete/recovery/compaction | Immutable authenticated create/publish/open/search fixture works; current object-native application-ID mutation and compaction path missing | Exercise equivalent workload/lifecycle | Exercise equivalent workload/lifecycle | OPEN |
| Total lifecycle cost and throughput/$ | No complete current-route lifecycle receipt | Direct matched cost ledger needed | Direct matched cost ledger needed | OPEN |
| Install/package/CI/security/failure checks | Local helper-backed crate archive smoke and focused binding/corruption tests pass; registry install and exact release CI unverified | Not a vendor timing substitute | Not a vendor timing substitute | OPEN |

Historical resident-route performance and published vendor claims are not
carried into these cells as current matched numbers. The table's internal
paired flat SQ8 is a quality diagnostic, not either vendor baseline.

## Latest decision and next implementation

The one page-diverse traversal falsifier is KILL (`65096d8b`). On ReLAION100k
D768 cosine k100 development0–63, returned recall fell99.156250%→99.062500%;
fetched coverage fell99.562500%→99.484375%. Removed its experimental APIs and
quarantined the binary. No CoHere/validation/scale run followed.

The subsequent compressed-score page-discovery falsifier is also KILL:
ReLAION development0–63 returned95.656250%, p05 87, discovered/fetched
95.968750%; same caps and paired flat99.515625%. Experimental code removed;
no CoHere or scale run. [Exact terminal evidence](../research/page-score-discovery-20260928/decision.md).

Latest implementation: source-authenticated hierarchical fitting with capped
1024-row semantic extents and explicit versioned CLI receipt. CoHere/ReLAION
first100k development0–63 GT-aware byte-feasible21-extent containment witnesses
reach99.78125%/99.53125%, both p05 99. These are diagnostic bounds, not achieved
recall or vendor wins. Next single gate is source-only extent-centroid
nomination plus returned SQ8 quality against paired flat on development;
kill geometry before root-format/HTTP/scale work if its reference fails.
No matched vendor gate is closed. [Exact decision](../research/hierarchical-source-layout-20260928/decision.md).

Evidence: [native pipeline KILL](../research/native-pipeline-quality-20260928/decision.md),
[discovery diagnostic](../research/nomination-trace-20260928/decision.md),
[centroid diagnostic](../research/centroid-discovery-diagnostic-20260928/decision.md),
[traversal KILL](../research/page-diverse-expansion-20260928/decision.md),
[32-page containment diagnostic](../research/page-containment-stress-20260928/decision.md).
