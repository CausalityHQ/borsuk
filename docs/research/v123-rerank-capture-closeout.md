# V123 a0001 unnormalized scorer closeout

This sealed result used a dot product after FP16 rounding. Rounding changes
vector norms, so the simulated FP16 branch did **not** implement the
preregistered cosine scorer. Its 104-hit gap is a measurement for that
specific dot-product implementation and does not reject FP16 cosine. See
`v123-cosine-correction-amendment.md` for the correction and a0002 rule.

V123 is a development diagnosis on the **already used** V121 deep-image-96-angular
publication test-first-1000 cohort at 9,990,000 rows. It is not held-out
validation or a serving measurement. Its immutable source was
`a9d11bc2ab1ef2674e91ecb2a254832ed78b35e5`, and its Spot terminal is
`s3://borsuk-bench-453182569524-euc1/research/v123-rerank-postmortem/a9d11bc2ab1ef2674e91ecb2a254832ed78b35e5/runs/v123-20260924T0225Z/a0001/terminal.json`,
SHA-256 `0d29ade93b24a3afbd36114b554c6e3151476c741c09b7fd1ccd6d302e368e6c`.
The terminal reports complete/exit 0 after 159 seconds; Causality Spot
`i-0817c55903796a0f7` was independently observed terminated. All 24 listed
artifacts were independently streamed from S3 and verified by byte length and
SHA-256. The capture summary SHA-256 is
`594b7ef9eb675ec500a5f4d1ab6bfc0c3e83b8261c6200c81b8bcf4751282447`;
the rerank summary SHA-256 is
`6b6f608ce6bdac01f8a59436827c6af8c2955fa6931637105781bb2edc5b43c8`.
The diagnostic passed six targeted Python tests and one targeted Rust test on
the remote worker. An independent standard-library recount of all 1,000
capture and rerank evidence rows matched their summaries, including capture
parity, totals, p05 and sub-90 counts. Source and publication GT SHA-256
bindings matched V119 and the frozen dataset.

| Deep-image-96-angular, test-first-1000, GT100 positions | K=0 nominees only | K=512 candidate union | V121 served SQ8 candidate |
| --- | ---: | ---: | ---: |
| Candidate-set capture | 99,994 / 100,000 | 99,995 / 100,000 | 99,580 physical-range coverage |
| Float32 source rerank hits | 99,993 / 100,000 | 99,994 / 100,000 | not run |
| Simulated FP16 source rerank hits | 99,889 / 100,000 | 99,890 / 100,000 | not run |
| Simulated FP16 p05 hits/query | 99 | 99 | not run |
| Simulated FP16 queries below 90 hits | 0 | 0 | not run |
| SQ8 served returned hits | not run | not run | 98,034 / 100,000 |

The K=512 float32 returned Recall@100 was **99.994%** and simulated FP16
was **99.890%**, both on the used cohort. FP16 lost 104 GT positions against
float32 on the same candidate set, exceeding the preregistered 50-position
(0.05 percentage point) tolerance for this scorer. The a0001
`supports_followup` flag is **false**, but the cosine gate is unresolved.
The SQ8 K ladder
showed K=100 union capture 99,995 and K=256..1,600 the same 99,995; the
source rerank result was also unchanged at every K≥100. K=1,600 saturated
at 768 fetched rows for some queries. The paired control is the explicitly
defined **shared-nominee hybrid control**, not V121's served control. Both
union arms had the same returned hit counts for all K because the 512 shared
nominees already contained nearly every truth owner. V121's served paired
control remains 97,791/100,000 SQ8 returned hits under the 32-GET/16-MiB cap.

The score representation is the main current defect: SQ8 final ranking lost
1,546 hits from physically fetched truth owners in V121, while this source
rerank recovered nearly all truth owners in V123. A full remote SQ8 data wave
added just one returned hit beyond nominee-only source rerank on this cohort.
It has no demonstrated quality value commensurate with its 16.75-MB/query
maximum here. The a0001 unnormalized FP16 scorer is an insufficient exact-source
surrogate under the preregistered fidelity gate. Neither the offline Python
rerank nor the Rust replay measured live S3 latency or a production local
FP16 tier. The reranker loaded a complete local source file and randomly
indexed nominee vectors, including nominees outside fetched SQ8 ranges; the
32-GET/16-MiB cap applies only to the old SQ8 wave and does not account for
how a serving system would obtain those source vectors. The remote diagnostic
Python process peaked at 9,016,936 KiB RSS
and took 20.33 seconds for 1,000 queries after source loading; the separate
wide SQ8 replay took 66.50 seconds and peaked at 2,142,812 KiB. These are
batch process observations, not per-query serving latency, QPS or 100M RAM.

**Decision:** do not promote the V121/V123 architecture or tune K on this
cohort. First repeat the diagnostic with the preregistered cosine scoring
semantics under a new immutable source and attempt. The later method test must use a generic score representation with a
certified error policy, test its precision on this development cohort, then
freeze the method for fresh deep-image query ordinals and a matched ReLAION
build. In parallel, replace the O(N) flat nomination scan before a serving
latency claim. A source rerank over locally retained nominees requires a
clear object-storage-native persistence and recovery contract; the current
SQ8 data wave cannot be credited as useful merely because it was fetched.
RAM policy must depend on `(N,D,R,C,G,L)` and measured charged memory, with
no hidden vector-count quality knee. Lean can prove candidate-capture ceilings,
score-error margin implications, payload arithmetic and conditional work/time
bounds after an implementation refinement; it cannot prove unseen recall or
deployed S3 latency from this postmortem alone.
