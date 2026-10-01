# Semantic scale admission: engineering boundaries

Source c2e74dfab9f7704c45748a41cb28fb1d400afd79. This is a layout/admission audit, not a new architecture decision or corpus measurement. See semantic-scale-admission.json for exact arithmetic and source SHA. Current FIRST100k D768 development quality/latency survives; fresh1M/10M/100M remain unqualified.

## Existing gates that prevent a valid fresh1M run

- semantic_unit_router.rs independently rejects rows above100000 in preflight, manifest open and seed walk. Generation descriptor and Python packaging/runtime independently enforce the same admission. They must remain fail-closed until a newly frozen scale arm is qualified.
- Original FP16 unit centroids occupy48000032 bytes at1M D768 (ceil(rows/32) units). Existing builder BLOB_CAP is8MiB, and allocation admission is128MiB. Decode/sample/trainer/output accounting must be re-qualified; bypassing the guards does not establish bounded memory.
- The builder requests489 training centers at1M; the split partition can contain at most977 leaves. Their f32 prototypes alone occupy1.50–3.00MB, before root JSON, leaf descriptors, membership, parsed allocations or old-generation pins. The current generation router root cap is1MiB. A1M fit cannot be treated as admitted by simply increasing the row limit.
- The current conservative construction model charges1.656GB at1M,104.131GB at10M and9.830TB at100M. Those are deliberately loose model values, NOT measured RSS or necessary working sets: it charges recursive scratch for requested-centers depth. Any tighter model needs a proof from the actual trainer's maximum live recursion and scratch lifetime.
- Query quality, selected-leaf/source/SQ8 requests and bytes cannot be inferred from these sizes. Frozen nomination first8/1.15-boundary/max16 is unqualified at1M. Do not increase its nomination or source caps without a distinct preregistered intervention.

## Ordered delivery gates

1. Finish the existing offered-a3 unchanged-native throughput campaign and preserve lower-rate valid cells, all-offer outcomes and completion-through-cleanup QPS. An8QPS offered pass is not proof of8QPS sustainable completion throughput.
2. Collect and qualify the existing five-HEAD deletion slice, including original release/test-build/Clippy/affected receipts; preserve current campaign identities. Then integrate once, execute full native assurance once on the final source and run one paired causal cold comparison with unchanged source/scorer/router parameters. The historical45–47ms removable interval estimate is not a measured speedup.
3. The cheapest fresh1M falsifier is ReLAION-first router-coverage then unchanged returned scorer on a newly fixed development panel, before cold serving: authenticate source/query/truth/order and exact original centroid bytes; record unit/page closure truth@10 and@100, returnedR10/R100, selected leaves, source/SQ8 GETs/bytes and route CPU. Require preregistered mean returnedR10>=95%; report per-query/tail andR100 separately. Kill only this arm if the fixed practical gate fails, identifying nomination/source/scoring loss. Complete CoHere on the same frozen mechanism if it survives.
4. Before implementing that scale adapter, choose one explicit compact root representation and prove bounded construction/parse/membership/selected-leaf allocations. Keep native100k admission until source-scale qualification; no historical production reader, silent cap extension or compatibility layer is needed. Freeze scale-specific GET/byte/RAM envelope before corpus execution.
5. Fresh1M coldHTTP targets published TP D768 coldp90<444ms only under declared protocol differences and AWS subsecond directional context. Measure p50/p90/p95/p99 and actual full-span completionQPS. Fresh10M D1024 published-context target is coldp90<1214ms at8QPS; do not transplant either target to100M. Vendorp95/matchedvendorQPS remain UNKNOWN without matched evidence.

## 100M envelope still requires qualification

At100M D768, original unit centroids are4.800GB and membership12.5MB; a flat f32 prototype plane alone is150–300MB. These are layout arithmetic, not servingRSS. The current flat JSON root and conservative trainer admission cannot be claimed100M-ready. A100M product envelope must account for router representation, immutable generation pins during swap, selected buffers, physical requests/retries and maintenance working set; choose memory/quality from the measured scale curve. No100M latency or total-dollar win is asserted here.
