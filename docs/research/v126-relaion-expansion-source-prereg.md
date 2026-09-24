# V126 ReLAION expansion-union source-score diagnostic preregistration

Status: preregistered development diagnostic before reading the widened
replay. This reuses ReLAION-1M **validation-1000**, which V116 and V124 have
already used; it is not a fresh validation or production qualification.

## One decision

Decide whether the sealed V116 query-blind router and 32-GET/16,777,216-byte
expansion plans contain enough true neighbors for a higher-fidelity source
scorer to clear the 99.0% Recall@100 floor on this different embedding
family. If not, change routing or expansion before investing in final-score
serving integration. Do not use this result to tune the shortlist width,
weights, codebooks or dataset-specific branches.

## Frozen inputs and method

V116 source revision `5e9b35ad40ea023eab4407aa611d759e1893bb34`
and complete terminal SHA-256
`932ca2d0ed378acdbbbe4cd741c3dd451f848184dd19d190c0ee60b10db4655d`
fix 1,000 requests, router nominees, candidate and V109-style capped-control
ranges, SQ8 object, manifest/sidecar, and original SQ8 top-100 replay.
The source Parquet and layout are the same frozen objects used by V124.
Authenticate every input by its recorded byte length and SHA-256 before
calculation; preserve the V116 object and campaign unchanged.
The sealed V116 SQ8 returned-hit reference is 99,208/100,000 for the
candidate and 98,618/100,000 for its capped control. Reproduce both totals
from the original top-100 IDs before interpreting the source-score result.

Run the unchanged Rust `replay-returned` scorer on the sealed requests with
explicit `TOP_K=512` for both arms. Before opening validation GT, require
each widened arm's first 100 IDs and every route/plan field to match the
sealed V116 replay. Any difference invalidates this attempt. Convert the
512 physical router nominees through the frozen physical-to-source-ordinal
layout and original source IDs. Each arm's fixed union is its 512 mapped
nominees plus its own SQ8 top-512 returned IDs. Duplicate IDs are removed;
the union is not trimmed to a memory budget or vector-count threshold.

For each union, normalize the request and original float32 source vectors
in float64, rank by descending cosine then ascending original source ID,
and return top 100. Also score the same union after float16 round-trip and
float64 normalization. No query or GT participates in building a route,
layout, codebook, range, union width or precision choice. Verify the
nominee-only exact-source totals independently reproduce V124's 96,813
returned GT hits and 96,849 captured GT positions before interpreting an
expanded result.

## Fixed outputs and gate

Write one evidence row per query with the original V116 SQ8 top-100,
wide replay top-512, nominee IDs, candidate/control union capture,
exact-source top-100, FP16 top-100, per-arm hits, GETs and planned bytes.
Record aggregate hits/100,000, p05 hits/100, queries below 90, union
sizes, FP16/exact disagreement, and candidate-vs-control paired wins/ties.
The same source scorer and the same K apply to both arms.

The diagnostic advances this fixed expansion path only if the candidate
exact-source arm reaches at least 99,000/100,000 GT hits, is no worse than
the same-run exact-source capped control, has p05 at least 90 and no more
sub-90 queries than that control, and all original range GET/byte caps hold.
If union capture is below 99,000, source scoring alone cannot pass and the
route/expansion must change. If capture is at least 99,000 but exact-source
returned hits fall materially below capture, audit GT metric, source-ID
mapping and cosine precision before changing the route. FP16 must lose at
most 50 total hits versus exact source to remain a candidate precision
plane; otherwise retain exact fallback while investigating the arithmetic.

This is offline batch quality. Record worker RSS and elapsed time but do not
label either live S3 latency or serving RAM. A qualifying diagnostic leads
to a matched 100k serving integration screen and fresh cross-corpus gates;
it does not freeze defaults or predict 10M/100M behavior.
