Read-only research complete. Nothing in the repo was edited, built, launched, or sent. Sources: public main `0b85a52a` via `git show` only (page authority, two-bit source, range reader, returned/exact SQ8, generation planner, store, compaction, publisher outlines), the two pending reconciliation docs, and both committed Fable verdicts. One note went to my own memory directory outside the repo.

## What the committed layout actually is

- **SOURCE plane** (`plane/records.bin`): 264 B/row at D1024, which is 256 packed two-bit bytes plus f32 scale plus f32 inverse norm. Digests cover 32-row pages in a sidecar, so residency is 1 B/row. The fetch unit is not the 32-row page. The planner nominates 256-row page closures and gap-bridges them under 128 GETs and 64 MiB, so each GET averages 3.8 pages (254 KB). The 32-row digest is already eight times finer than the fetch unit.
- **SQ8 object**: 1036 B/row (i64 id, f32 norm², 1024 u8). Digests cover 256-row pages. The serving loader hard-pins that at two_bit_generation.rs:1541 and :1613. Pages are scored per 256-row page, admitted by `choose_budgeted_pages_sparse` under 30 GETs and 16 MiB, and every returned row is exact-ranked (returned_sq8.rs:114). So the SQ8 shortlist is page-granular today. Row-granular rerank is a shortlist change and a format change, not a scheduling change. This confirms the reconciliation caveat and corrects consultation 895ae749's "no new format" claim for its direction 2.
- **PageAuthority** has no Clone derive, and every fetch and rank seam takes it by reference. Per-range digest clone amplification does not exist in the code. It would only appear if a future offload copies the authority into spawned tasks.
- **Reader**: one range is collected whole into BytesMut, then `verify_payload` hashes page by page synchronously (sq8_s3_range.rs:896 to :912). Hashing page-by-page as chunks arrive needs no format change.
- **Two-bit scoring** is a 512 KiB f64 lookup table gathered one byte at a time with one accumulator per row (rotated_two_bit.rs:301). That is a kernel property on the current AoS bytes, not a layout property.

## Per-query ledger the candidates must move

| Stage | GET/q | MB/q | Rows/q | CPU share |
|---|---|---|---|---|
| SOURCE | 107.9 | 27.4 | ~104k (10.4% of 1M) | SHA ~17 ms, score ~18 ms |
| SQ8 | 29.9 | 16.7 | ~16.1k (63 pages) | SHA ~10 ms, score ~5 ms |

SOURCE bytes are cover and gap bytes. Code width is settled: the G0 result killed narrower codes, and gap bridging is scheduling. No format change shrinks SOURCE bytes without reopening either, so SOURCE belongs to the running cover-curve job. The SQ8 tier is the only place where a layout change buys bytes, GETs, and hash time together.

## Ranked candidates

| # | Format delta | Code seams | Predicted bounds (assumptions stated) | Memory 1M / 100M | Lifecycle consequences | Cheapest distinct native falsifier |
|---|---|---|---|---|---|---|
| **1. SQ8 digest pages 256→32 rows, sub-page shortlist** | SQ8 page manifest v3 with `page_rows=32`; sidecar 8x longer; no change to row bytes or object keys | Publisher and compaction digest loops (two_bit_compaction.rs:284 disk bound, publish bin), drop the two `==256` pins, planner maps top rows to 32-row sub-pages and reuses `cover_pages` gap bridging under the same 30 GET / 16 MiB caps; `query_payload_bytes` constant | Lower bound M×1036 B for M shortlisted rows. If the top 1 to 2k two-bit rows cluster into ≤30 runs, SQ8 drops to 2 to 4 MB (4 to 8x) and hash CPU by ~8 ms. Upper bound is unchanged if rows scatter and bridging re-inflates. Recall is not parity. | Sidecar 1 MB / 100 MB; total digest residency 2 B/row | Only digest emission changes. Mutation deltas, tombstones, epoch-bound GC, and head CAS are untouched. Recovery unchanged (sidecar SHA stays in the page manifest). Old SQ8 generations rejected by schema, allowed pre-release. | Offline, no S3: recompute two-bit scores from local `records.bin` for the 1000 closed queries, simulate 32-row sub-page selection under 30 GET / 16 MiB, exact-score from local `sq8.bin`, compare to truth. Reject if recall@10 < 97.60% at ≤ current GETs, or SQ8 bytes saving < 2x. Minutes of CPU. |
| 2. In-band 32-row digest table per 256-row SOURCE page (two-level proof) | 1 KB table at each page head; resident sidecar holds page-of-table digests only | `two_bit_source.rs` build loop, `PageAuthority::verify_payload` two-level verify | Bytes +1.5% per fetched page, GETs unchanged, hash +1.5%. Resident SOURCE digests 1 B/row → 0.125 B/row. | 0.125 MB / 12.5 MB instead of 1 / 100 | Builder and compaction emit tables; range starts must align to page heads | None worth running now: 100 MB at 100M fits the declared 3 GiB envelope, so this buys nothing at 1M and only a residency saving at 100M. Defer. |
| 3. SoA fast-scan two-bit layout (32-row transposed nibbles) | Row bytes unchanged, order within each 32-row page transposed | Codec encode/score, builder, compaction, every row-addressed test | CPU only: score ~18 ms → ~2 ms. Zero bytes or GETs. | None | Row addressing within a page becomes block-relative everywhere | Not yet: a VNNI or safe-SIMD kernel on the current AoS bytes should capture most of this. Measure that first under `forbid(unsafe_code)`; the failed portable 8-row batch stands. |

Rejected without a candidate row: block compression (two-bit and SQ8 bytes are quantized and near-incompressible, and it breaks range addressing); narrower codes (G0 falsified); BLAKE3 (cuts ~20 ms of SHA per query but changes the trust domain and needs new authentication and lifecycle gates, and candidate 1 already removes the SQ8 share of hashed bytes).

## Selection

Candidate 1 is the one next format candidate, gated by its offline replay before any code. It is distinct from the download-scheduling consultation because it changes digest granularity and the exact-ranked population, and distinct from V139/V146/V149/V150 because no summary ranker, centroid page, or hierarchy is introduced. The cover-curve job stays untouched.

## Data missing, not invented

- Per-row two-bit scores are absent from closed records, so the replay must recompute them locally. Whether the 1M `records.bin` and `sq8.bin` sit in the local assets directory was not verified.
- Within-page concentration of the true top-10 among the 16k reranked rows is unmeasured. It decides whether candidate 1 lands at 4x or at zero.
- No measured AoS SIMD kernel exists, so candidate 3's marginal value over a kernel fix is unknown.
- Hash and score CPU figures above are profile shares scaled to 96.5 ms CPU per query. They are not per-stage timers.
