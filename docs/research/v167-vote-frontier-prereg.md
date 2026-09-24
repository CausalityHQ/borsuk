# V167 source-pseudoquery frontier gate (frozen before launch)

## Decision

Test whether the V167 minimum encoded-byte frontier can preserve V165's
above-threshold non-nominee capture while reducing planned SQ8 bytes
without excessive GET growth. This is an internal source-pseudoquery
screen, not a Recall@100 or latency result. Failure kills this stopping
method; success only admits a separately frozen ReLAION-100k D768
development-1000 paired returned-quality gate.

The method and external advancement conditions are specified in
`v167-min-cost-vote-frontier-decision.md`. The implementation in
`scripts/v167_min_cost_vote_frontier.py` minimizes whole 32-row units,
then GETs, for a target of all 100 primary votes weighted 513 each plus
`ceil(f × Smax)` achievable secondary votes. The fixed grid, in order,
is `4/5, 9/10, 19/20, 39/40, 99/100, 1/1`. No dataset name, vector
count, or recall-dependent memory knee chooses a fraction.

## Inputs and split

Use the exact ReLAION-1M D768 source, V70 SQ8, V63 old physical order,
V115 source router, and V164 source-only physical order whose S3 object
keys, byte lengths and SHA-256 hashes are pinned in
`scripts/launch_v167_vote_frontier_spot.py`. The V115 experimental v1
router reader is sealed locally to this campaign; production still
rejects incompatible v1 artifacts. The source archive must come from
the clean pushed `origin/main` commit recorded in the reservation and
terminal.

Sort the one-million stable source IDs by
`SHA256("borsuk-v166-pseudoquery-v1:" || decimal_ID)`, breaking ties by
numeric ID. V166 used human ranks 1–256. V167 uses ranks 257–384 for
fit and 385–512 for one untouched holdout. Artifact `query_ordinal`
fields are zero-based, 256–383 and 384–511 respectively; the prepare
seal states this convention. Both sets are source rows
present in layout and router training; the screen cannot establish
generalization to validation queries.

For each source pseudoquery, reproduce frozen V115 PQ64 512 nominees,
exclude its own old physical row before SQ8 primary selection, use
the exact top 100 SQ8 nominees as primary, and consider only nominee
32-row units plus their adjacent units. In this declared universe,
count non-nominee SQ8 rows at or below the 100th-primary SQ8 threshold,
excluding the source row. This is a proxy for extra coverage, not the
validation nearest-neighbor truth.

## Phase order and acceptance

1. **Prepare:** authenticate every input; write separate GT-blind
   nominee/primary/vote rosters and proxy labels; seal both digests.
2. **Plan:** read only the rosters; write the V165 full-cap plan and all
   six V167 fraction plans for every query; seal the plan digest with
   `proxy_labels_opened=false`. Each plan is bounded by 32 GETs and
   16 MiB of encoded SQ8 bytes.
3. **Evaluate:** open only fit labels to select the smallest fraction
   with at least 99% aggregate V165 capture and empirical nearest-rank
   p95 per-query loss at most one. If none passes, kill without parsing
   or scoring holdout labels; their sealed file may still be streamed
   for SHA-256 integrity. Otherwise score holdout labels only at the selected
   fraction. Holdout must meet both quality conditions, use at most 90%
   of V165 aggregate planned bytes, and use at most 120% of V165 GETs.
   Selection of `1/1` also kills the method.
4. **Check:** independently rebuild roster votes from authenticated
   layouts, deterministically replay every plan and charge, verify
   candidate unit geometry, sealed digests, and the summary. Any detected
   mismatch fails the cell. This replay does not independently recompute
   SQ8 thresholds or per-unit proxy counts from source vectors; those
   labels are sealed during prepare and use V166's closed score path.

The same full-cap baseline is recomputed on this new panel; no old
measurement is substituted. Capture comparisons are paired by query.
The exact external 100k and 1M quality/resource gates, and the separate
10M router/100M scalability work, remain as stated in the method
decision. A Lean theorem proves conditional minimum-cost monotonicity
for ordered targets; it cannot prove these capture or timing conditions.

## Infrastructure and artifacts

Launch one `c7i.12xlarge` Causality Spot instance in eu-central-1 with a
four-hour hard shutdown. The devbox controller holds a process-lifetime
local launch lock and rejects active V167-tagged EC2 workers before launch.
The controller registers a unique attempt,
streams all terminal-listed artifact hashes back from S3, and
terminates the instance immediately after the terminal marker. If Spot
interrupts a cell, discard it and restart the whole cell under a new
attempt; never combine partial measurements. The terminal records the
instance ID, source commit/archive SHA-256, phase, status, and each
artifact's length and digest. Do not inspect incomplete measurement
files. No other V167 instance may overlap.

The remote phases record `/usr/bin/time -v` resources separately for
prepare, plan, evaluate, and check. These are offline resource numbers,
not live serving latency or resident production memory. The status
`advance-to-100k` is only a decision to run the next paired gate; it
does not freeze the production default.
