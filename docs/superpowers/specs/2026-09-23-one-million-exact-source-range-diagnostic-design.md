# ReLAION-1M source-vector final-range diagnostic

Status: preregistered design; no result or production quality claim.

## Decision

The closed OPQ8 score-driven 32-range selector retained 91,282/100,000
GT100 positions, below the paired control's 91,541 and the fixed 98,151
gate. Its selected groups contained 98,985 positions, leaving 7,703
in-group positions behind. The truth-aware interval witness shows that
the physical layout and range/byte limits can cover 98,935 positions,
but it used truth. This diagnostic changes only the score representation:
replace OPQ8 row ADC scores with source-vector squared L2 distances.
Keep both sealed group plans, page layout, page-priority rule, range
admission, and I/O caps fixed. A passing exact-source candidate assigns
material loss to OPQ8 score representation; a failure means the present
priority/admission rule cannot meet the gate even with source vectors.
Neither outcome promotes an actual-read system.

## Frozen inputs and arithmetic

Use the authenticated 1,000,000-row ReLAION source Parquet, 1,000 ordered
development queries and truth identities, original base/delta generation,
physical row-to-page and page-to-group mapping, and candidate/control
group plans named in the closed `relaion-1m-dev1000-a0001` data-range
terminal. Authenticate those artifacts against their terminal-listed
lengths and SHA-256 values. Rebuild physical row order from the sealed
generation and reject missing, duplicate or extra IDs. Reject nonfinite
source/query coordinates and nonfinite scores. No truth file may exist
in the planning phase.

For each source row and query, convert their stored float32 coordinates
to float64 and compute squared L2 as `max(0, sum(q*q) + sum(x*x) -
2*dot(q,x))`, with float64 products and accumulation. Use a fixed
single-host implementation and record NumPy/BLAS versions and thread
count. The resulting values are finite float64 scores; this is an
operational source-vector scorer, not a real-arithmetic proof. Stable
ties use physical row ordinal. The plan and its complete page-priority
order are the cross-host closeout authority; on-host validation reruns
the scorer from source bytes.

Within each arm's own frozen selected groups, take the first 100 rows
by `(source score, physical row ordinal)`. Rank their owner pages by
descending top-100 row count, then page minimum source score, then
`(role, page ordinal)`. Append remaining selected-group pages by page
minimum score and the same physical tie. For each priority page,
replay the existing minimum-byte interval admission without truth:
at most 32 role-preserving contiguous ranges and 16,777,216 encoded
bytes. Keep the original generation's page lengths and positions.
The output contains both arms' sealed ordered priorities, targets,
included bridged pages, ranges, GET count and bytes for all queries.

The scorer may stream the source once and retain per-query/per-page
minimum scores and top-100 `(score, ordinal)` rows. A synthetic fixture
must show this streaming reduction yields the same ordered priority as
the full-score reference, including ties, base/delta separation and
different selected groups. The worker must stay within the 3-GiB
process-tree cap with 64-MiB margin and zero swap.

## Evaluation and fixed stop rule

Upload and read back both arm plans and their digest before fetching
truth. Then map every ordered GT100 and GT10 ID to its authenticated
physical owner page. Recompute hit masks, page-priority ranks, hit kind,
GETs and bytes independently from the sealed plans. Report candidate
and page-centroid control GT100, GT10, p05 GT100, sub-90 queries,
paired wins/losses/ties, target/bridge hits and the OPQ8 closed baseline.
The source-score candidate passes this diagnostic only if GT100 is at
least 98,151/100,000, GT10 at least 9,928/10,000, p05 GT100 at least
90, sub-90 queries at most 49, and every query respects the GET/byte
caps. These are the previous gate's fixed thresholds, not tuned values.

If it passes, investigate a compressed row scorer capable of preserving
the source-score page order, then qualify it on a fresh untouched cohort
and actual authenticated data reads. If it fails, investigate the
top-100 page nomination and greedy range admission or change the page
layout; do not build PQ96 under the failed rule. The 1,000-query
development cohort is already used and cannot be an untouched
qualification cohort. Do not tune this diagnostic on its truth.

## Execution, evidence and proof boundary

Push the source before the campaign and seal a create-only source
archive. Run one Causality Spot attempt; preregister interrupted-cell
restart with a new attempt number, retain every instance identity, and
terminate compute immediately on a complete terminal marker. Monitor
incomplete work only through terminal markers and infrastructure health.
Independently authenticate every terminal-listed artifact and resource
receipt, then recount all queries. A missing or mismatched artifact
invalidates the attempt.

Lean can prove interval byte/GET limits and a conditional recall bound
from authenticated truth-owner and included-page certificates. The
source-score result supplies those finite-cohort premises after closeout;
it cannot prove recall on unseen queries or deployment latency. To
formalize the executable method, refine the streaming top-100 reduction
and minimum-byte interval planner to the Lean model, with finite-precision
and tie behavior stated explicitly. The current full scan has linear
work in source rows; no sublinear 100M claim follows from this gate.
