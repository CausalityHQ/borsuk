# V141 D96 returned-quality replay closeout

V141 completed one frozen Causality `c7i.8xlarge` Spot attempt in
`eu-central-1` from source commit `f4fb48461e323e148fd2905da43558932d122768`
and source archive SHA-256
`b699523bb325d619ddc69e366e39007836ce582b1d7d30082006393f5841409f`.
The output prefix is
`s3://borsuk-bench-453182569524-euc1/research/v141-deep-returned/f4fb48461e323e148fd2905da43558932d122768/runs/v141-20260924T094142Z/a0001`.
Terminal SHA-256 is
`3608cfa6266978355302ab9ad8d89958514f4ad01dc5ff14ec5dbc19cc60ad34`:
`complete`, exit 0, 70 seconds, instance `i-0a78e9814f7bbe723`, observed
terminated. The launcher authenticated all 12 terminal-listed artifacts
against their recorded sizes and SHA-256 values. The 1,000 replay and
evidence rows were independently recounted against V122 truth and SQ8
source-ID mapping; all per-query returned hits, physical coverage,
planned bytes/GETs, summary aggregates and paired counts match. The
authenticated summary SHA-256 is
`ecfd4d5ed94ce006e5a0526f47f56bb54d4f48afb46badf99aba65e97be6b295`.

This is the **deep-image-96-angular random100k train subset** with 1,000
already-used **publication-test ordinals 9000–9999** and subset GT100.
All three arms used the same V122 source, SQ8, query, 512 physical router
nominees, SQ8 scorer, top-512 fetched expansion and float64 cosine on
original float32 source vectors. Ordered top-100 IDs for every arm were
sealed before GT was downloaded. The broad and capped controls exactly
reproduced the prior V131 source and SQ8 hit totals, validating the
same-method comparison.

| Metric, 1,000 queries | V140 β=4 pages | Broad current candidate | Fixed capped control |
| --- | ---: | ---: | ---: |
| Exact-source Recall@100 | **99.739%** (99,739/100,000) | 99.942% (99,942/100,000) | 98.827% (98,827/100,000) |
| SQ8-only Recall@100 | 98.960% (98,960/100,000) | 99.106% (99,106/100,000) | 98.203% (98,203/100,000) |
| Fetched SQ8 physical GT coverage | 99,738/100,000 | 99,942/100,000 | 98,827/100,000 |
| Exact-source p05 hits/query | 98/100 | 100/100 | 94/100 |
| Exact-source queries below 90 hits | 0 | 0 | 12 |
| Mean planned SQ8 bytes/query | 2,693,744.64 | 9,428,126.976 | 1,535,701.248 |
| p95 planned SQ8 bytes/query | 5,408,640 | 10,772,352 | 2,865,024 |
| Mean planned GETs/query | 28.100 | 1.000 | 24.674 |
| Mean source-union size/query | 549.236 | 548.085 | 540.009 |
| Offline SQ8 scoring p95 ms/query | 10.409 | 23.479 | 5.556 |
| Offline source scoring p95 ms/query | 0.306 | 0.287 | 0.282 |

β=4 won/tied/lost **22/824/154** queries against the broad arm and
**298/700/2** against the fixed control by exact-source hit count. It
passes all preregistered development screen requirements: at least
99,500 aggregate source hits, p05 at least 98, zero sub-90 queries, and
aggregate hits above the same-run capped control. It gives up 203
source GT positions versus the broad arm while planning 71.43% fewer
SQ8 bytes on this used cohort. No per-query plan exceeded 32 GETs or
16,777,216 bytes.

The β=4 exact-source total exceeds fetched-range physical coverage by
one position. This is valid: a router nominee outside fetched SQ8 pages
entered the exact-source union. Fetched physical coverage therefore
bounds SQ8-only returned hits, not the full source-union result. The
prior V139/V140 wording was corrected before this replay.

The replay process took 30.86 seconds for all 1,000 queries and peaked
at 438,688 KiB RSS. Its evaluation cgroup peaked at 522,641,408 bytes;
the post-replay cgroup charge was 142,217,216 bytes. The offline per-query
times exclude router construction, page choice, network GETs, source
fetches, startup, and concurrent serving. They are **not** live-S3
latency or production charged-RAM measurements. This cohort is reused
development data, so the pass is an architecture screen, not a fresh
publication quality result.

**Decision:** retain β=4 as the current selective-page candidate. Next,
run the analogous D768 returned-quality gate with fixed preregistered
thresholds, then implement the planner in production Rust and test the
matched layout with real S3 reads. Freeze publication defaults only after
those gates, including a fresh held-out quality split and measured serving
latency, throughput and memory. Keep the page budget a generic function
of requested recall, dimension, compression and measured costs rather
than introducing a vector-count switch; the current β=4 is a development
screen value, not a universal production default.
