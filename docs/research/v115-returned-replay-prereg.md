# V115 composed returned replay preregistration

Status: planned before execution. This is an offline ReLAION-1M
**development-1000** gate; no live S3 performance is measured.

Freeze the V114 source-bound SQ8 object, mirror manifest/sidecar, 1,000
requests, per-query reference and paired evidence. Substitute only the
V115 a0001 Rust nominee roster for each query. Use the Rust exact-local
mirror to score all 512 nominees, select the top 100, plan the bounded
physical ranges, and rank every returned SQ8 row using the new Rust
returned-range scorer. The Rust replay runs without GT. After its output is
sealed, read the pinned ReLAION-1M development GT100 and compare all 1,000
queries with V114: score bits mapped by physical row, primary, page votes,
ranges, plan bytes/score, ordered returned IDs and GT hits. Record total
Recall@100, p05 hits, sub-90 query count, and every mismatch count.

The strict inheritance gate is zero mismatch queries for score bits,
primary, page votes, ranges, plan bytes/score, returned IDs and GT hits.
If the returned scorer changes even one ID, report the measured hit effect
and treat V114's 99.234% Recall@100 as historical, not a V115 measurement.
A changed method needs a new paired quality cell before live quality
promotion; no parameter change is authorized to improve this dataset alone.
The same-run V114 V109 capped control is 98.803% Recall@100, both on
ReLAION-1M development-1000.

Run once on Causality `c7i.12xlarge` Spot with a 7,200-second wall cap,
no On-Demand fallback. Pin the committed source archive and each input
SHA-256. Sync output and phase resources under one immutable attempt, write
one terminal, and terminate immediately. On interruption discard the cell
and restart at a new attempt prefix. Monitor incomplete work by terminal
and infrastructure only. Untouched validation, deep-image-96-angular, live
S3 latency/QPS, and 10M/100M remain separate qualification gates.
