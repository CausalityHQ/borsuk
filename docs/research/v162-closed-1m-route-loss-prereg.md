# V162 closed V161 1M route-loss decomposition

## Single decision

Locate the V161 1M exact-source quality loss before choosing another
architecture. Reuse only terminal-closed, immutable V161 and V155 evidence
and the frozen V116 roster/V36 truth. No new index, route or query score is
produced. This is a postterminal diagnostic on the **used** ReLAION-1M
D768 validation-1000 split, not a quality promotion or fresh measurement.

## Frozen inputs and method

Authenticate the V161 complete terminal SHA-256
`7b370d8e3500ea5be069b5a5d0712d247b5c4c6c9b82f8a2dccc0032132676cf`
and its membership, layout seal, plan seal, plans and raw files; the V63
physical permutation, V116 exact-primary/512-nominee replay, V36 GT100,
and V155 complete terminal and exact/sparse physical evidence. Check every
input's recorded length and SHA-256 before reading it. V161's new SQ8
object is unnecessary: its authenticated membership maps each stable ID
and source ordinal to the relaid 512-row physical order. V63 maps V116
old physical ordinals back to source ordinals.

For each of the 1,000 sealed queries, count distinct exact GT100 IDs at
the following stages: nominated 512 rows; every relaid page touched by
those nominees; exact-SQ8 primary 100 rows; every relaid page touched by
the primary; and V161's final fetched ranges. Recount the terminal V155
sparse physical coverage on the same GT. Independently derive the
GT-blind minimum page-aligned contiguous byte cover of *all primary
pages* under 32 GETs by joining the cheapest gaps between existing
runs, accounting for the 64-row final page. This is a physical lower
bound for the page-aligned schedule, not a new planner or an arbitrary
row-range impossibility proof. Record its p05/median/p95/max bytes and
the number of queries within 16,777,216 bytes. Bind per-query raw,
summary and a separate recount to the source commit and terminal inputs.

Classify the loss with the predeclared 1M transfer floor of 99,400 GT100
hits and p05 97: if primary-page coverage misses either, the relaid
layout plus frozen exact-primary row selection lacks enough page headroom.
If primary-page coverage clears both but final physical coverage misses,
the loss is admission/scheduling. If both stages lose, report both; use
minimum-cover breaches to distinguish an unavoidable page-aligned cap from
an optimizer choice. Do not sweep cluster count, page width, byte cap,
primary weight or dataset-specific controls in this diagnostic.

Run one small Causality Spot cell from a clean pushed source. Seal an
immutable reservation and terminal, sync terminal artifacts even on
failure, rehash all S3 artifacts after completion, and terminate compute
immediately. Interruption discards the entire cell and requires a new
attempt. Local full builds/tests remain paused under devbox swap pressure.
