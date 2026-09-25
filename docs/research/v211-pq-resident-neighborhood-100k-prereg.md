# V211 query-aware compact-code screen over resident neighborhoods

The V210 ReLAION-100k direct FP16 neighborhood falsifier was too
slow at the 32,768-candidate budget that met quality. Test one
material candidate-generation change on the same reused first 256
development queries: generate the same 32,768 nearest physical
positions from the same frozen 512 seeds, rank those positions by a
source-trained PQ64 query score, and FP16-rank only the best 4,096,
8,192 or 16,384. The PQ64 books/codes are the authenticated V113
source artifact and are mapped to V163 physical order by stable ID.
No GT or query labels fit the codebooks, expansion or selection.

Compute and seal every returned top-100 ID list before downloading
truth or the paired V193 full-rank baseline. The frozen internal gate
for one fixed shortlist is at least the baseline's aggregate hits on
the same 256 requests, p05 at least 98, and p95 at most 10 ms for
PQ screen plus FP16 gather/score/rank on the Spot CPU. Report all
three arms, choose the smallest passing shortlist, and leave the
32,768-neighborhood formation time separate. This is an offline
falsifier on a reused panel, not product latency or a 1M prediction.
No further 1M run is allowed if none pass.

Use one bounded Causality Spot cell, immutable hashes and a pretruth
S3 seal, verify terminal artifact hashes, and terminate immediately.
An interrupted cell is discarded and restarted under a new attempt.
The candidate budget and memory policy remain generic and may scale
with recall and corpus size, without a vector-count knee.
