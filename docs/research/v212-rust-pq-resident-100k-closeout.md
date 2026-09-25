# V212 Rust PQ-screened resident 100k closeout

The fixed 32,768-neighborhood → source PQ64 top-4,096 → resident
FP16 top-100 Rust kernel **passes paired quality and fails the frozen
whole-kernel p95≤10-ms gate**. Stop this candidate family; do not
promote it or launch another 1M cell. The next material candidate
index is a source-built navigable graph with resident FP16 scoring,
first falsified at 100k.

The one complete Causality `c7i.4xlarge` Spot `a0001` instance
`i-0bf7d560804425168` used commit
`7b58fa467b1cb19cd285467fac402b55697c3d25`, archive SHA-256
`75898a36290d12f14211f0f952e50959f8bec7f6e139daf772e33aaddf1c7ac5`.
The terminal SHA-256 is
`83911b632d3596e49ddc81c2ff22fd0127a5d9bb1e45d8afaad14b8992fbd17d`
at `s3://borsuk-bench-453182569524-euc1/research/v212-rust-resident-100k/7b58fa467b1cb19cd285467fac402b55697c3d25/runs/a0001/terminal.json`.
All 14 terminal artifact sizes and SHA-256 hashes passed readback,
including the 154,400,064-byte authenticated physical FP16 plane;
the instance terminated. Raw returned IDs were sealed to S3 before
truth and V193 baseline download.

Dataset/split: **ReLAION-100k D768, already-used development queries
0–255**. The Rust returned lists contain **25,513/25,600 GT100 hits**,
p05 **99**, and seven queries below 98. Paired V193 full-rank SQ8
has **25,440/25,600** hits on those queries. Rust wins/ties/losses
against V193 are 83/155/18. V211's Python top-4,096 arm also had
25,513 hits and p05 99; actual ordered Rust/Python ID parity was not
required or counted, so the GT intersection is the authority.

| Rust sequential request phase | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Bounded nearest physical expansion | 1.337 ms | 1.576 ms | 1.659 ms |
| Existing source PQ64 score and top-4,096 | 3.876 ms | 4.020 ms | 4.118 ms |
| Resident FP16 score and top-100 | 7.481 ms | 7.653 ms | 7.706 ms |
| Whole kernel | **12.740 ms** | **13.200 ms** | **13.520 ms** |

The 256-query sequential loop completed at **78.46 queries/s** over
3.263 s. Peak Rust process RSS was **170,450,944 bytes**, including
154,400,000 charged FP16 plane-array bytes. Cold map/PQ/plane load
was 0.225 s. Vector-body GETs were zero. These timings exclude
network front end, concurrent admission, mutation, generation swap
and S3 build/hydration cost; they are not external-product latency.
The Python preparation peaked at 1,785,528 KiB RSS, a temporary
research-build cost rather than serving RAM.

The 10-ms threshold is an internal candidate budget. V212 missed it
by 3.200 ms at p95 on only 100k rows, with phase evidence showing
work spread over expansion, PQ and FP16. Further tuning within this
family would restart a local latency optimization series without a
clear 1M or 100M product advantage. A graph candidate index is the
next structural design: bound visits by recall target and score only
a compact FP16 frontier. Prove conditional resource bounds, then
measure its actual quality, p50/p95/p99, QPS, memory and cost on a
frozen 100k panel before another 1M product comparison.
