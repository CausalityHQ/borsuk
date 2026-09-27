# V291 centroid shortlist, two-bit page rank, SQ8 return

Status: frozen before reading V291 outcomes. V289 globally scored all 100k
rotated two-bit rows and fetched 99.953125 GT100 mean/p05 100 on CoHere
first100k D768 cosine k100 development0–63. V290's fixed top-159 of 391
physical pages by existing eight source unit centroids retained 99.8125
GT100 mean/p05 99. This is the sole combined follow-up authorized by those
necessary bounds.

Keep V283 layout, SQ8 payload, authenticated source and query/truth; V289's
200-byte encoder and reconstructed cosine score; V290's 159-page Euclidean
centroid precursor; V284's final 84-page/32-GET/16,777,216-byte scheduler.
For each query score *only* records in the 159 candidate pages, select final
pages by V289 page maximum, and rank all rows in the covered SQ8 pages using
V282's stored norm, `low`/`step`, and squared-L2 formula. No GT affects the
route. Report candidate, fetched and returned GT100, p05, planned GET/bytes,
maximum coded rows scored, offline wall/RSS and paired V283 flat development
control (96.703% returned). The returned rank and planner are diagnostic
NumPy arithmetic; a near-gate result requires bit-exact Rust replay.

KILL if fetched mean <98.9/p05 <96, returned mean <98.0%/p05 <95, or any
plan >32 GET/16 MiB. A pass permits one Rust implementation and source-only
ReLAION+CoHere paired validation, followed by a cold 1M HTTP gate only after
that passes. This 100k route still flat-scores all 3,125 centroids, so a pass
cannot certify 100M work, latency or cost; hierarchical first-stage search
must meet a separate visit and recall gate. One local run <=300 seconds and
2 GiB RSS; no new cloud or full panel.

## Coefficient correction before a valid SQ8 decision

The first local command exited 0 but its returned score is invalid: the script
derived SQ8 `low` and `step` from the authenticated *raw* CoHere input, whose
sample vector norms are 12.53–15.81. V282 normalized that input before
encoding SQ8; sample stored SQ8 norms are 0.999–1.001. The first output SHA
is `ceec9e3a1bb04723444f9787733d800f6bc2df2291f74777b0effcc0afbee80c`;
it reported 99.765625 fetched GT100 but only 91.375 returned, and must not
be used as a quality measurement or KILL decision. The frozen route and
thresholds remain unchanged. The correction loads `low`/`step` from sections
whose hashes are bound by the authenticated V283 router manifest, exactly as
the Rust scorer does, before one replacement local replay. The first command
took 14.43 s/874,832 KiB RSS, exit 0; its flaw is semantic, not a crash.

## Corrected local development decision (2026-09-27 UTC)

**GO to exact Rust SQ8 replay and bounded first-stage work gate, not cloud.**
The single corrected `106e2488` replay completed exit 0 in 14.21 s with
879,576 KiB peak process RSS. Its [result](v291-two-stage-development-result.json)
SHA-256 is
`113a5523c7b382047003758f144ceed10f4a526e2c32b432cceb2d7e4d169c7b`.

| CoHere first100k D768 cosine k100, development0–63 | Mean GT100 hits | p05 hits | Max GET | Max planned bytes |
| --- | ---: | ---: | ---: | ---: |
| V290 159-page centroid candidates | 99.812500 | 99 | — | — |
| V291 two-bit selected SQ8 pages | 99.765625 | 99 | 32 | 16,773,120 |
| V291 SQ8 returned | **98.171875** | **96** | 32 | 16,773,120 |
| Frozen fetched / returned gates | >=98.9 / >=98.0 | >=96 / >=95 | <=32 | <=16,777,216 |
| Paired V283 flat returned control | 96.703125 | 93 | 32 | 16,773,120 |

At most 40,704 two-bit row codes were scored for a query; the first-stage
centroid pass still scanned all 3,125 unit means. The code plane is 200
bytes/row and remains a high-memory candidate, with no measured 100M cost.
The NumPy SQ8 scorer follows the authenticated `low`/`step` and stored norm,
but the 0.171875-hit margin above the mean gate makes bit-exact Rust scoring
parity the next required check. The local 14.21 s is whole-process offline
work, including encoding 100k source rows and 64 queries; it is not serving
latency, QPS or a vendor comparison. No validation or 1M promotion yet.

For the near-gate parity check, the same deterministic source script now
emits the selected physical page sets and per-query hits as a separate JSONL
receipt. Its aggregate output must remain byte-identical to the sealed
`113a5523...` result. Rust will authenticate the generation and SQ8 object,
verify every selected page payload, and use the production `rank_returned_ranges`
arithmetic on precisely those page sets; any per-query mismatch stops promotion.
This replays the scorer, not a new query cohort or route parameter.

The first complete exact Rust replay from `0c0c56f9` passed page-coverage
parity on all 64 queries and returned mean 98.1875/p05 96. Its only returned
count difference was ordinal18 (Rust99, NumPy98). The NumPy scorer used a
BLAS dot product and reduction for shift while Rust accumulates float32
products in coordinate order. The source diagnostic is therefore corrected
to the production sequential-f32 arithmetic before one parity replay; no
candidate/fetched page or threshold changes. The earlier NumPy result stays
immutable as its recorded arithmetic, not an exact production score.

## Exact score closeout

**The development quality/I/O gate passes with the production Rust scorer.**
The authenticated debug Rust replay completed exit 0 in 109.84 s with
102,216 KiB peak process RSS. Its [result](v291-rust-sq8-replay-result.json)
SHA-256 is `6e0e8eecd6c616dbf4fe76b5ae3653f44d51f9fba94855bc8de87b1842a84318`.
It verified all selected page hashes and all 64 fetched-hit counts. The
single returned count difference from BLAS is query18: Rust99 versus BLAS98.

The sequential-f32 Python replay from `a911e39a` completed exit 0 in 35.41 s
with 876,204 KiB peak process RSS. Its [exact-score result](v291-exact-score-result.json)
SHA-256 is `3fed8fb0db99881e83608470cc895be1904b89f340467ad2922637a1e7ac3678`.
[The validator](../../scripts/validate_v291_exact_sq8_parity.py) authenticated
both [original BLAS plans](v291-blas-plans.jsonl) and
[exact-score plans](v291-exact-plans.jsonl), verified that every selected physical
page set was unchanged, and matched all 64 exact-score counts to the recorded
Rust scorer result. Mean returned GT100 is **98.1875**, p05 **96**, fetched
mean **99.765625**, p05 **99**, within 32 GET/16,773,120 planned bytes. The
parity blocker is resolved without tuning the route or using new queries.
Neither debug Rust wall time nor Python offline time is a serving measurement.

The current first stage still flat-scans 3,125 source-unit centroids, and the
200-byte row plane scores at most 40,704 resident codes per query. The next
single gate is a bounded hierarchical first-stage search paired with this
unchanged exact-score route. Only that and the unchanged two-dataset 100k
validation can authorize cold1M HTTP. Code and SQ8 source scales are explicit:
the two-bit plane encodes original raw CoHere rows for cosine nomination;
SQ8 encodes normalized rows, with coefficients bound by its own generation.
No 100M memory/latency projection or vendor win is claimed.
