# Hierarchical-cell 100k diagnostic protocol

Operator approved implementation and continued execution on 2026-10-03. This draft is prospective; it is not a completed experiment or final configuration freeze.

## First falsifier

Use both ReLAION and CoHere 100000-row, 768-dimensional corpora described in input-control-inventory.md, authenticated by complete body hashes. Use cosine k100 and consumed query ordinals 0–63 for a diagnostic only. No held-out or fresh-quality claim. The order/canonical ID binding has passed the separate ID-only admission; requests and truth still require full authentication before use.

Build choices must depend on corpus geometry, never query results or truth. Freeze hierarchy fanout, cell capacity, boundary policy, nomination count, refinement budget and their source hashes before opening diagnostic truth. Execute one candidate configuration per corpus; preserve every failure. A redesign is a distinct prospective arm, not a repair to its result.

Compare against the unchanged control implementation with identical corpus, normalization, logical IDs, query split and k. Historical 100k k10 binaries and current 1M receipts do not qualify that k100 control. Record any separate historical V282 comparison with its original source and protocol; do not merge it into a current-source paired ratio.

The authenticated historical development64 reference already exists at `docs/research/sequential-centroid-diversity-20260928/v282-reference/`. Its original source is `e74d75acb40647f606b498023b11dec26701e4aa`; archive SHA is `3facfaf935a3e4921466281e79afe15f8cc945e30870a97806f814845972998d`. ReLAION graph/flat mean recall@100 is 97.984375%/98.296875%, with p05 hits 94/95; CoHere graph/flat is 96.25%/96.046875%, with p05 hits 92/90. These are verified historical reductions of the identical first64 request/truth populations. Their performance is stale for this architecture. The flat reference is bounded centroid routing, not exhaustive SQ8. Authenticate its original records and retained reduction before comparison; no new reference run is implied.

## Measurements and loss populations

For each query retain ordered IDs and counts at router-selected cells, boundary-expanded cells, locally nominated rows, refinement-admitted rows and final top100. Report exact-GT intersection at each stage and recoveries as well as losses. Final ranking loss is not automatically quantization loss; isolate quantization with a separate exact-score diagnostic over the same admitted population if supported.

Count actual logical object operations, payload bytes, dependency fetch waves and concurrent buffers. Local ObjectStore operations are not physical S3 requests or cold network latency. Measure per-stage CPU, elapsed time, process/cgroup peak memory, build scratch and resident directory separately. Include overlap from retained generations in later serving/maintenance gates.

The initial quality references are mean recall@100 98% and p05 95%; the initial query resource references are 32 GET and 16 MiB. These originate in the prior V282 protocol and remain explicit references, not universal architecture rejection rules. Before executing this arm, freeze a measured feasible candidate envelope and a paired quality/resource decision rule from its source-derived allocation and layout model. No post-result cap relaxation.

## Execution order

1. Finish source-only prototype and exact API/resource contract.
2. Freeze synthetic affected tests and qualify exact source remotely: affected execution, release build, workspace Clippy and real workspace test compilation. Local devbox safety limits remain unchanged.
3. Authenticate actual corpus/request fixtures and run the same admission path cheaply before any paid science. Identity, format, CLI or resource failures are INVALID.
4. Freeze executable configuration, source and prospective envelopes; build both control and candidate, then run the single paired diagnostic with truth evaluated after nomination is sealed.
5. Stop weak arms using their original frozen rule. Advance a Pareto survivor to fresh-panel quality qualification and then cold HTTP infrastructure canary and measured serving.

Production promotion additionally requires incremental writes, authenticated generation pins, recovery and in-process compaction/GC. This diagnostic alone cannot establish these or a vendor win.
