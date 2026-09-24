# V121 untouched deep-image paired returned-quality preregistration

Status: method frozen before fetching the deep-image query or GT payloads.
Launch only if V120's immutable construction attempt at
`s3://borsuk-bench-453182569524-euc1/research/v120-deep-image-index/b919685cf1db6c14912c2118d5226c0fb426226b/runs/v120-20260924T004145Z/a0001`
has a complete terminal and the required layout, SQ8, router, mirror and
page-authority artifacts are independently authenticated against it. V120
uses corpus rows only. A V120 failure closes this preregistered V121 cell
without downloading query/GT data.

The untouched inputs are the first 1,000 rows, in source order, of the
publication-v3 **deep-image-96-angular test** split. The query object is
`s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/datasets/deep-image-96/attempts/0001/materialized/test.parquet`,
3,843,448 bytes, SHA-256
`296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`.
The GT object is the adjacent `neighbors.parquet`, 4,003,585 bytes, SHA-256
`d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d`.
These identities come from the authenticated publication staging receipt
SHA-256 `eb10a317cc778b30e2ee88eee8614b760e36f31d2d7af1a62a60f5f32c163230`;
no query or GT values were read to select this method. Require the published
`emb` fixed-list float32 query and `neighbors_id` list-of-int GT schemas.
Normalize each nonzero finite query to unit L2 norm; reject zeros. GT IDs
are original corpus train ordinals and enter only after plans and returned
IDs from both arms are sealed.

The candidate is the unchanged V116 rule with newly constructed D96 planes:
region count `ceil(page_count * 1024 / 3907)` (10,228 for 39,024 pages),
PQ64 top 512 nominees, exact SQ8 primary top 100, 513/1 primary/secondary
page votes, exact weighted interval planner, and the Rust returned SQ8 scorer.
The paired capped control admits pages by minimum PQ64 nominee score using
the **same Rust roster**, then uses the same Rust returned scorer. Both arms
allow at most 32 GETs and 16,777,216 returned bytes per query. Independently
compare Python and Rust nominee **sets** for the first 16 query ordinals;
an ordered near-tie does not fail if the set is identical. A mismatch closes
the cell for diagnosis, not parameter tuning.

The 16-query Rust nomination screen runs before the full 1,000-query cell.
Stop if its measured elapsed time exceeds 32 seconds (2 seconds per query);
even without S3, that is a decisive flat-router runtime failure. Otherwise
complete the paired offline quality cell. Record nomination and replay wall
time, process RSS, per-query returned GT100 hits, p05 hits, sub-90 query
count, paired wins/ties/losses, GETs, physical bytes and cap violations.
The cross-corpus quality gate requires at least 99,000/100,000 returned GT
hits, p05 at least 90, total and p05 no worse than the paired capped
control, no more sub-90 queries than that control, and zero cap violations.
This is the same absolute floor used for untouched ReLAION-1M validation.
Failure changes the generic method by diagnosed layer; do not insert a
deep-image-specific width, vote, region or memory tier. Historical V82
deep-image figures belong to another source and format and are context only.

Run one immutable Causality Spot attempt with a 7,200-second science cap,
interruption detection and immediate termination after terminal. Download
and hash every input before use. Monitor only terminal and infrastructure
while incomplete. Independently recount the sealed replay and GT after
terminal. This offline cell cannot establish live S3 latency, throughput,
charged serving memory or cost; those remain separate release gates.
