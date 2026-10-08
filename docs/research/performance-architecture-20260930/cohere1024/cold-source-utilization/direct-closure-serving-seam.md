# Direct closure serving seam and recall boundary

This records source inspection, not a selected architecture or an executed experiment. The source-utilization qualification/replay must close before choosing the next serving change.

At immutable source `44e116b9b8754cef0393fad1f96259bd583f3b7b`, `TwoBitGeneration::plan_paged_measured` first normalizes the query and discovers walks locally, then admits walks, derives their 256-row closure, fetches SOURCE ranges, and performs source-dependent nomination. `search_store_measured` subsequently passes the SQ8 ranges to the existing authenticated scorer, which scores every fetched row, including bridges.

A candidate can reuse local discovery, checked closure construction and the authenticated SQ8 scorer while removing the dependent source-fetch/nomination phase. It must preserve normalization, generation/ETag authentication, query admission, exclusion handling, complete result reporting and precise GET/byte/error accounting. This seam is generic; no dataset/model name belongs in the library policy.

## Broader closure is not a recall-preserving superset

The shared smallest-gap cover changes bridges when its required page set changes. For a two-GET allowance:

- Required pages `{0,5,15}` produce covers `[0,6)` and `[15,16)`: gap pages 1–4 are fetched incidentally.
- The larger required set `{0,5,8,11,15}` produces `[0,1)` and `[5,16)`: its three cheaper gaps are merged, and pages 1–4 are omitted.

These are page intervals, not byte intervals. The example follows the unchanged helper's smallest-gap rule. Thus the larger required set can omit previous incidental candidates. The current report already labels direct closure cost as counterfactual and warns that different bridges can change recall. The research review's informal “superset” description must not be used as a quality proof.

If the cost evidence supports a candidate, its tiny native oracle must cover this bridge difference, partial final pages, actual fetched IDs, complete scalar ranking and score bits. Then run the real frozen Cohere request/truth falsifier before a cold performance benchmark. Accept or reject recall from that execution, not from set inclusion of required pages.

## Resources and next decision

At N100,000/D1024, full-population SQ8 payload is bounded by `N*(D+12) = 103,600,000` bytes. That is an arithmetic ceiling, not a measured working set or an admitted query cap. The existing 16,773,120-byte cap cannot silently become the direct strategy's envelope. Use the authenticated report's distribution and measured retained/coexisting buffers to preregister any changed byte/RSS/CPU envelope on the bounded remote host.

No production method, format or default is changed here. No ANN query, corpus/query/truth body or performance result was used for this source inspection. Warm caches, new datasets and 100B remain deferred.
