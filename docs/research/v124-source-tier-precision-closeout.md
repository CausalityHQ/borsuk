# V124 cross-corpus source-tier precision closeout

V124 is a **development-only** numeric diagnostic on two already used cohorts:
deep-image-96-angular at 9,990,000 rows, publication test-first-1000; and
ReLAION-1M, validation-1000. Their routers and layout fitters differ. The
source commit was `4134cb8053b3597bd0f8aa866b013c0592327a1a`.
The Causality Spot terminal is
`s3://borsuk-bench-453182569524-euc1/research/v124-source-tier-precision/4134cb8053b3597bd0f8aa866b013c0592327a1a/runs/v124-20260924T0305Z/a0001/terminal.json`,
SHA-256 `fca94e94a625fa0c6d2e70cd088782611bd6b9b7a286430f265d8bbed7ea1d74`.
The worker completed in 77 seconds. Spot `i-0d21f65cc630c6220` was
independently observed terminated. All 18 terminal-listed artifact lengths
and SHA-256 hashes were independently checked from S3. The deep-image
summary SHA-256 is
`7de1227cf0245986e8afd6d7414d59cf33b62db3428e7d714b7124cfc178ca19`;
the ReLAION summary SHA-256 is
`27216d99bea2d3a2784231f5a27a5829641588850ed72deff7983628e7a90cee`.
Six targeted Python tests passed on the frozen worker. An independent recount
of all 2,000 evidence rows matched every summary total, p05, median, p95 and
maximum. Every per-query `certified_reference_ids` list equaled the full
float64 original-source top-100 list within the fixed 512 nominees.

| Metric, GT100 over 1,000 queries | Deep-image-96-angular, test-first-1000, 9.99M | ReLAION-1M, validation-1000 |
| --- | ---: | ---: |
| Nominee capture ceiling | 99,994 / 100,000 (99.994%) | 96,849 / 100,000 (96.849%) |
| Original float32 coordinates, float64 cosine | 99,993 / 100,000 (99.993%) | 96,813 / 100,000 (96.813%) |
| Simulated FP16, float64 cosine | 99,959 / 100,000 (99.959%) | 96,813 / 100,000 (96.813%) |
| Simulated per-row scaled int16, float64 cosine | 99,986 / 100,000 (99.986%) | 96,812 / 100,000 (96.812%) |
| FP16 versus reference top-100 set disagreements | 34 positions total | 0 positions total |
| Int16 versus reference top-100 set disagreements | 7 positions total | 1 position total |
| FP16 certified exact refinements/query | median 102, p95 106, max 289 | median 100, p95 100, max 106 |
| Int16 certified exact refinements/query | median 100, p95 101, max 104 | median 101, p95 103, max 109 |

The deep-image K=0 original and FP16 totals exactly match V123 a0002's K=0
totals, independently validating the cross-corpus evaluator's source-ID map
and cosine semantics on that cohort. Its int16 arm improved the used-cohort
simulated fidelity relative to FP16, but ReLAION did not show the same gain;
no width should be selected from either used split. The ReLAION nominee-only
ceiling of **96.849%** is decisive for the fixed roster: any scorer restricted
to those 512 IDs is below the 99% aggregate goal. Lean's
`AdaptiveRerank.relaion_v124_nominee_only_below_99_percent` formalizes that
conditional implication when the measured capture is authenticated and the
returned IDs are confined to nominees. This is a finite-cohort statement,
not unseen recall. V116's separately served SQ8 candidate returned **99.208%**
on the same ReLAION validation split against its paired capped control
**98.618%**; V116 used a remote expansion wave, so nominee-only V124 must not
be substituted as an equivalent end-to-end method.

For each quantized candidate, V124 calculated a float64 directional error
bound from its original and decoded source vector, plus a fixed `1e-12`
roundoff allowance, and checked every score interval against the float64
reference. It then exactly rescored all candidates whose interval could
cross the top-100 lower-bound threshold. The verified refinement result
demonstrates the conditional interval method on these cohorts. It does not
prove the error bound of a deployed FP32/FP16 kernel, persistence of the
source tier, local random-read latency, S3 request counts, charged memory,
concurrency, cold starts or 100M behavior. The deep-image Python diagnostic
phase took 10.20 seconds and peaked at 11,577,492 KiB RSS; ReLAION took
14.15 seconds and peaked at 9,116,844 KiB. These are batch worker measurements,
not serving p50/p95/p99 or QPS.

**Decision:** retain a corpus-generic expansion path. V123's one-hit
deep-image benefit cannot justify removing it, because V124's ReLAION
nominee capture makes 99% impossible without changing the router or expanding
candidates. Develop an accurate source tier with authenticated S3 persistence
and measured local RAM/SSD or coalesced S3 access; its total bytes and latency
must include exact fallback reads. Pair it with a genuinely sublinear router
and remeasure capture, expansion benefit and final returned quality on matched
method builds. After a development screen, freeze the method for fresh
deep-image ordinals and a matched ReLAION test, then qualify live S3,
generation overlap, concurrency and charged memory as a function of
`(N,D,R,C,G,L)`. No vector-count quality knee is supported by this evidence.
