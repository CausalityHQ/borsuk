# V206 Deep-Image FP16 representation diagnostic preregistration

## One decision

On the **already-used** Deep-Image-96-angular publication test-first-1000
real-query split at 9,990,000 rows, determine whether resident FP16
reranking of the **same frozen V121 physical ranges** repairs V121's SQ8
score crowding. V121 candidate SQ8 returned 98,034/100,000 GT100 hits;
its ranges physically contained 99,580/100,000. This test changes only
the final score representation and keeps the corpus-only V120 layout,
V121 queries, routing, ranges, GETs and planned SQ8 bytes unchanged.
It is a representation-layer diagnostic, not a claim that the ReLAION
V197 layout/planner and V120 are one frozen cross-corpus method.

Authenticate before use:

- V119 9,990,000 × 96 source Parquet: 3,566,768,562 bytes, SHA-256
  `8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee`.
- V120 physical layout: 79,920,128 bytes, SHA-256
  `419f9280d2e85f6fa275c115dd3428e7d714b7124cfc178ca19a42c249ac31ec6af2d19a42f5b96c38ed247c1`.
- V121 closed query JSONL: 2,013,436 bytes, SHA-256
  `331310ae7abc3f0b73ea20010c7ee5a1de5ff04f5ef1d3e45b90dc513a48962d`.
- V121 closed Rust replay JSONL: 14,740,672 bytes, SHA-256
  `ddc9af991bdc6d3ef77d34a156994daa43aeb78f67f18de2cd0dc5ebb93abe91`.
- Publication GT Parquet **only after returned IDs are sealed**:
  4,003,585 bytes, SHA-256
  `d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d`.

For each V121 physical range, map its SQ8 byte offsets through the
authenticated layout to source IDs. Rank every fetched vector twice by
float64 cosine: once after a float32→float16→float64 storage round trip,
and once from original float32→float64 as a same-range precision ceiling.
Use stable train ID as the score tie break. Record both exact top-100 ID
sets and V121's SQ8 list before opening GT. Upload their raw artifact and
seal to S3, then fetch GT and count all three arms on the same 1,000
queries. Validate every physical interval, 100 unique returned IDs,
finiteness, and source train ID geometry.

The diagnostic succeeds if FP16 returns at least 99,000/100,000 GT100,
p05 ≥90, beats same-range SQ8 in aggregate without extra SQ8 GETs/bytes,
and retains zero invalid plans. Report paired query wins/ties/losses,
below-90 count, float32 same-range ceiling, wall time and peak process
RSS. An FP16 miss while float32 passes implicates quantization; if both
miss, physical coverage or plan selection still limits quality. Do not
retune widths, prices or layout on these used queries. A pass licenses a
fresh held-out Deep-Image split and a matched ReLAION build; it does not
qualify live S3 latency, end-to-end serving, mutations or 100M scale.

Run one Causality Spot cell from a pushed source, with interruption
discard/restart, 7,200-second wall cap, closed artifact readback and
immediate instance termination. Monitor only terminal and infrastructure
while incomplete.
