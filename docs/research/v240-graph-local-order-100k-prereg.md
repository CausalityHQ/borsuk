# V240 graph-local physical order falsifier

V239's authenticated ReLAION-100k D768 cosine graph candidate stream
met the 99.6% GT100 containment target at ef=4,096 and prefix 1,024,
but touched 147 existing 256-row SQ8 pages at p95, a 29,352,960-byte
minimum against the frozen 16 MiB budget. The current order is V163's
source-only k-means centroid chain. No further query-driven permutation
or parameter search is allowed for this decision.

Build **one** source-only graph-local order with SciPy's reverse
Cuthill-McKee on the authenticated V218 base-layer adjacency interpreted
as an undirected graph. This standard bandwidth-reduction algorithm
uses no query, candidate, truth or S3 timing data and has no tuned
cluster count. Save the complete permutation and SHA-256 layout seal
before downloading or opening V239's sealed candidate stream. Keep the
same 256-row, 780-byte SQ8 page geometry and the same candidate ID and
physical-ordinal streams; only remap ordinals through the new order.

Independently replay page counts on all 1,000 prior-used queries for
prefixes 512/1,024/2,048/4,096. Require reproduction of V239's old
1,024 p95 page count 147. **Pass** only if the relaid 1,024 prefix has
p95 minimum page bytes ≤16,777,216 (at most 84 distinct pages). Record
p50/p90/p95/p99 page counts and minimum bytes from the same raw query
samples. These are ideal full-page lower bounds; no SQ8 scores, GETs,
latency, changed recall or vendor comparison are measured. A pass
authorizes one frozen 1M I/O-shape replay. A failure ends the sparse
remote SQ8 layout branch and directs work to scalable resident tiers
and graph construction, with V237 as the current serving baseline.

Use one `causality` c7i.4xlarge Spot attempt in eu-central-1c. Pin V218
graph and V239 terminal/raw identities, upload the layout seal before
fetching V239 candidates, sync all terminal artifacts with hashes to
S3, independently replay the closed result, then terminate the instance.
Discard an interrupted cell and restart only under a new attempt prefix.
