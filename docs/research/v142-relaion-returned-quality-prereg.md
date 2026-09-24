# V142 ReLAION-1M returned-quality replay preregistration

**Decision:** does the same V140 β=4 page policy that passed the D96
development screen preserve exact-source top-100 quality on the already-used
ReLAION-1M validation-1000 split under its existing V116 layout? Do not
change β, page ranking, source scorer, or caps after seeing this split.
This is the second corpus architecture screen, not a fresh held-out result
or a live-S3 serving latency measurement.

Authenticate V116 requests and sealed replay, the V70 SQ8 object, V115
SQ8 low/step coefficients, V36 original float32 source and validation
GT100, the V63 layout permutation, and V140 ReLAION raw plan against
their frozen SHA-256 values. Download GT only after all top-100 returned
IDs for every arm and query have been sealed. Use one Causality Spot
attempt, terminal-only observation while incomplete, a whole-instance
deadline, interruption discard/restart, artifact authentication and
immediate termination at terminal.

Compare three arms with identical SQ8 arithmetic, 512 router nominees,
top-512 fetched SQ8 rows and original-source float64 cosine ranking:

1. **β=4:** V140 `variants["4"].ranges`.
2. **Broad current candidate:** V116 sealed `ranges`.
3. **Fixed capped control:** V116 sealed `baseline_ranges`.

Use the V116 request query as float32 for SQ8 scoring, and normalize its
float64 coordinates as in V126 only for exact-source cosine.
Validate that every SQ8 physical source ID agrees with V63 layout and
V36 source ID mapping. Score SQ8 rows with V114's original float32
quantized arithmetic and tie rule. Score the union of fetched SQ8 top-512
source IDs and 512 physical nominee source IDs with the V126 exact-source
method. The broad and control arms must reproduce V126 same-route totals:
**99,563/99,432 exact-source hits** and **99,208/98,618 SQ8 hits**, or
the candidate is claim-ineligible. If the V114 Python SQ8 scorer differs
from the V116 Rust top-100 set, stop and diagnose rather than relaxing
this parity gate.

β=4 passes the quality screen only if it returns at least **99,500**
exact-source GT hits out of 100,000, p05 at least **98** hits/query, no
more than **2** queries below 90 hits, and no fewer aggregate hits than
the same-run capped control. Record paired wins/ties/losses against both
baselines, SQ8 hits, fetched-range physical coverage, union size,
planned GET/bytes, process RSS and charged evaluation-cgroup peak.
The source and SQ8 replay is offline. Its stage timings exclude routing,
network, S3 retry, concurrency and startup; do not report them as served
latency. If β=4 fails, diagnose whether page selection, SQ8 ranking or
source-union capture caused the loss, then revise the responsible layer
under a new frozen revision. A pass permits production Rust integration
and a matched-layout live-S3 gate, followed by a fresh held-out quality
split. Neither D96 nor ReLAION development result selects a production
memory cap: the cap must be a generic resource function of requested
recall, N, dimension, compression and charged memory/IO costs, without
a vector-count branch.
