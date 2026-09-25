# V239 100k graph candidate containment closeout

**Decision: reject the current V163 k-means physical order for sparse
remote SQ8 reranking under the 16 MiB p95 page-read budget.** The graph's
quality was sufficient at a 1,024-row shortlist, but even the minimum
full-page payload was 29,352,960 bytes at p95. A planner cannot remove
that 12,575,744-byte excess by merging reads. This is a physical-layout
failure, not a measured S3 latency failure. Keep the V237 resident FP16
serving line as the current production candidate.

The one `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-0fb02c23fde1e5931` in `eu-central-1c`; it is **terminated**. Frozen
source commit `dfdacbc8fb9fb4bd968291f4a011af05e0912489`, archive
SHA-256 `f11410960be58f2ce5311c2101c4910932d2c48a98df9137409eab3f0660fe02`.
Terminal SHA-256 `e67d8d90ac450fabcccac3fe9c10d65da4baef5de42f04e463cc78d3a85fda14`,
closeout SHA-256 `d039b06f663a1381c6fe826f5228c2692fc2667031e46118537ddc119b1af460`.
The terminal reports `complete`, exit zero. All seven terminal artifact
lengths and SHA-256 hashes passed replay. The sealed raw candidate stream
SHA-256 `61e6e5b6a42931ce76cc496eee864e847593c295dfd0dba26db01c9021ccf7bf`
matched the terminal artifact; the launcher independently rescored that
sealed stream against the pinned truth and compared the complete quality
JSON. Immutable evidence:
`s3://borsuk-bench-453182569524-euc1/research/v239-graph-containment-100k/dfdacbc8fb9fb4bd968291f4a011af05e0912489/runs/a0001/`.

Dataset **ReLAION-100k D768**, cosine, k=100, prior-used development
queries 0–255 and prior-used method-held-out queries 256–999. V218's
same-source FP16 returned baseline was 99,771/100,000 GT100 hits at
ef/shortlist 2,048, p05 99. V239 used ef=4,096, graph and exact flat
PQ64 candidate scores, and measured **candidate containment**, not
returned recall.

| 1,024 candidates | Development / 25,600 | Held out / 74,400 | Combined / 100,000 | Combined p05 | p95 minimum SQ8 page bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Reachable PQ graph | 25,565 | 74,329 | **99,894** | 99 | **29,352,960 B** (147 pages) |
| Flat PQ oracle | 25,574 | 74,351 | 99,925 | 99 | 29,552,640 B (148 pages) |

The graph's 512-candidate prefix contained 99,348/100,000 hits with
p05 96. Thus 1,024 was the smallest tested prefix meeting the fixed
99.6% and p05 98 targets on **both** splits. Its 147 distinct 256-row
SQ8 pages at p95 cost at least 147 × 199,680 = 29,352,960 bytes.
Actual transport may cost more due gap coalescing and request overhead;
no SQ8 scores, actual GETs, S3 query latency or vendor comparison were
measured. The complete Rust candidate pass took 11.59 s and peaked at
203,340 KiB RSS, including the test's resident FP16 ID authority.
The remote narrow graph tie regression passed 1/1. Spot compute was
estimated at **$0.04387** to terminal, excluding S3, EBS and billing tail.

Next: test exactly one query-blind graph-local row permutation on the
already sealed V239 candidate ordinals. Build the permutation only from
the authenticated graph, freeze its hash before opening candidate rows,
and replay the same p95 page bound. If it cannot reach 16 MiB at the
fixed 1,024-row quality-passing prefix, stop the sparse remote SQ8 path
and invest in scalable resident tiers and graph construction. A pass
only authorizes the frozen 1M I/O-shape replay, not a product win.
