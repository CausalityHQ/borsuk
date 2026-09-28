# Historical validation loss: discovery is the next causal layer

One ARM Causality Spot worker completed unchanged historical reconstruction and replay: terminal exit0, workeri-0983cf91959a0876c independently verified terminated. Every raw/normalized/order/SQ8/root/truth identity matched, development traces matched byte-for-byte, and both panels matched their original plan hashes and physical coverage counts. No tuning, new layout, new quality score or independent validation occurred.

## Verified closed counts

ReLAION first100k D768 cosine k100. Panels are consumed development0–63 and validation256–999. Means below are exact-GT hits/query, equivalent to recall@100 percent. Flat is the already recorded paired exhaustive nativeSQ8 baseline; returned scoring was reused without rescore.

| Split | Discovery coverage | Nominated coverage | Physical coverage | Returned / paired flat | Discovery missing hits | Nomination losses / gap gains |
|---|---:|---:|---:|---:|---:|---:|
| Development64queries |99.5625|99.5625|99.5625|99.15625 /99.515625|28|0 /0|
| Validation744queries |99.224462|99.224462|99.224462|98.850806 /99.568548|577|0 /0|

All808queries have zero GT nomination loss and zero GT gap bonus. Validation discovery misses GT on121/744queries; physical fetching misses the same577hits. Fetched-but-not-returned278hits remain descriptive scoring losses, not a pure quantization floor or an independently measured counterfactual. The existing KILL remains: returned deficit0.717742percentage points exceeds0.5. No CoHere extension was run.

| Decision | Evidence | Next gate |
|---|---|---|
| Keep the failed-layout KILL | Historical returned/flat scores unchanged | No scale/default/vendor promotion |
| Prioritize candidate discovery |577misses occur before nomination; none added by nomination on this panel | One bounded discovery change; keep source/order/SQ8/plane/159candidate count/fetch accounting fixed |
| Retain the ranking plane for the next falsifier | Zero GT cut loss under the current ranking does not prove a replacement ranking safe | Actual returnedSQ8 quality, not coverage alone |
| Defer layout and graph-cut rewrite | They add a second causal layer and lack scalable build/maintenance evidence | Separate experiment only if discovery change fails or yields an unacceptable cost tradeoff |
| Fresh confirmation remains open | All original1000queries per corpus were consumed | Seal a new authenticated query cohort before confirmation |

## Bounds and process measurements

Development maximum32plannedranges/16,773,120bytes, mean25.234375ranges/16,753,230bytes. Validation maximum32/16,773,120, mean26.272849ranges/16,757,721. These are logical plans; no physical S3 query requests or cold serving latency were measured.

AWS diagnostic process tree:39.05s elapsed, maximum RSS1,630,224KiB (includes reconstruction/truth). Standalone744-query planner phase:6.76s total and50,200KiB maximum RSS, including CLI startup. These are diagnostic process measurements, not production RSS, per-query p90/p95 or HTTP throughput. Historical release compile:299.09s,4,410,536KiB maximum RSS; the4GiB address-space bound applied only to replay, not compilation.

Controller observed461s and estimated compute$0.0262 from$0.2048/hour Spot quote; excludesEBS/S3 and is not an invoice or lifecycle ANN cost.40terminal artifacts were hash/length verified by the controller;29retained gzip artifacts and both source archives were independently verified, with all four current harness inputs matched to the frozen archive. Large historical binary artifacts remain immutable in S3, not checked into Git.

## Ordered next work

1. Design one bounded navigation change using this unchanged generation. Reconcile V139/V146/V149/V150 and the earlier centroid-discovery diagnostic; do not substitute an exhaustive production scan or run a parameter sweep.
2. Before launch, declare100M decoded RAM plus pinned-generation/build transient allowances, bounded route work, build/maintenance cost and cold latency/I/O envelope. Existing200B/row plane and96.125B/row decoded unit summaries are projections, not100M RSS; both and graph structure must be counted. Current production native builder is a separate SquaredEuclidean path; this historical cosine route does not qualify that API.
3. Run the cheapest paired100k returned-quality falsifier on consumed, explicitly disclosed development panels. Preserve0.5pp/98mean/95p05/32GET/16MiB gates; close and kill before expansion on failure.
4. Only a qualified winner plus independently sealed queries proceeds to fresh1McoldHTTP,10M/100M, generation-swap/failure/lifecycle measurements and both matched vendor comparisons. No measured vendor win exists.

Goal stays active. No operator decision or credential blocker is currently needed; no active benchmark worker remains.
