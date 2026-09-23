# V115 composed returned replay closeout

Status: completed with a **failed strict order-parity gate**. The returned
top-100 sets and GT hits nevertheless matched on every frozen query. This is
an offline development result, not a live S3 serving result or a production
qualification.

## Frozen cell and evidence

- Source commit: `40c1b150448913466f67832a5ff8c2eaeda915a1`.
- Source archive SHA-256: `8e33e9949c0692126ddaaa4b129a5998c6e2bdd9e7144671022ad0d658648ec1`.
- Causality Spot: `c7i.12xlarge`, `i-01ed27bf0bfe7e3c3`, eu-central-1c;
  instance terminated after its terminal marker.
- Immutable attempt: `s3://borsuk-bench-453182569524-euc1/research/v115-returned-replay/40c1b150448913466f67832a5ff8c2eaeda915a1/runs/v115-replay-20260923T234055Z/a0001`.
- Terminal: `failed`, exit 1 at validation after 76 seconds; source and
  instance identity checked. All 14 terminal-listed artifacts were streamed
  back and independently verified against their recorded sizes and SHA-256
  digests (42,050,740 bytes in total). Summary SHA-256:
  `43cf339f6d4fe962f10f937ec6f76b8eb6a5090178717dad460c1c50d100c53f`.

## Result and decision

On **ReLAION-1M development-1000**, the Rust composed replay returned
99,234/100,000 GT hits: **99.234% Recall@100**, 98 hits at p05 and one query
below 90. Frozen V114 produced the same hits on every query. Its same-run
V109 capped control was 98,803/100,000, or 98.803% Recall@100. These are
verified offline measurements on the stated split. They are not an untouched
validation measurement, live S3 latency/QPS, or a cost estimate.

All 1,000 Rust nominee *sets*, exact nominee score bits by physical row,
primary rosters, page votes, physical ranges, plan bytes, and plan scores
matched V114. Ordered returned IDs differed on 23 queries: each discrepancy
was a two-position swap, with **zero changed top-100 sets** and zero changed
GT hit counts. The strict preregistered zero-mismatch order gate therefore
failed; it must not be relabelled a pass. The Python V114 returned scorer uses
a float32 matrix product and the Rust returned scorer uses scalar accumulation.
That arithmetic difference is the likely source of nearby order swaps, but
the exact cause of each swap has not been proved. Retain the Rust ordering as
the intended native serving rule and treat V114 order as historical. Do not
tune tie or score rules using this development cohort.

The Rust offline replay spent 19.69 seconds for 1,000 queries and reached
1,536,588 KiB process RSS. These are a batch computation and process RSS on
the Spot host, **not** per-query serving latency, throughput, cgroup steady
RAM, or S3 GET cost. It reread a local authenticated SQ8 object, not S3.

Next gates use the same generic method and frozen Rust scorer on untouched
ReLAION-1M validation and deep-image-96-angular, with an honest paired
current BORSUK control. Then wire generation-pinned live S3 returned reads,
measure C1/C8/C32 latency and resources, and decide the routing/scaling
architecture from those results. No vector-count knee or universal memory
cap is inferred from this cell; memory admission remains conditional on
`(N, D, R, C, G, L)` and measured host/cgroup budgets.
