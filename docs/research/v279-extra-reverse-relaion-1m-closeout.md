# V279 bounded extra reverse edges: ReLAION-1M transfer closeout

Frozen source `8206cbc9a7d0949146b2a8739f4964725871a913`; source archive
SHA-256 `c38a301c5c51ccdc823fc90518160a7a73a0b296ab22a277817f3273b6bc7c9a`.
Attempt `a0001` completed on eu-central-1 c7i.4xlarge Spot
`i-0db9dfa2bedf4dd4b` and was terminated. Terminal SHA-256
`b7c14d3917a596d7d5451a15cd48196332651af7a89a3aa3228d9b9f323c74e7`;
all 38 artifact hashes and lengths were replayed. Closeout SHA-256
`ac7c7f36c78e45c1ade5f9843c1950255e4181f8bf8176cbbdcc554ce669b3da`.
Spot quote $0.3729/hour; estimated compute through terminal $0.47079.

ReLAION-1M, D768 cosine, k100, validation ordinals0–999 (historically used).
The paired arms used the same source Parquet SHA-256
`2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`,
raw FP32 source SHA-256 `a3eac4dedae5006843ea2ad243ec590440fe5b983ca8cd969dbc92b3e3406c33`,
request rows SHA-256 `c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9`,
normalized FP32 query SHA-256 `82fe696c1d765d27b9f8f6c5277a998a4ebbf2bd6d6f89533ad3a5445105525a`,
and exact FAISS cosine GT100 SHA-256
`23180f9b7e7727f478672d919e08e7bc17bdb75a76c9308d69dbc44c41bc1db7`.
Both generations were authenticated. Search was sequential, local resident,
in-process Rust after one warmup, with zero query-time GETs. Timings below
are not HTTP service or vendor timings.

| Width | Measure | Paired baseline | Extra reverse edges |
| --- | --- | ---: | ---: |
| Default PQef4096/shortlist4096/exactef2048 | GT100 hits / 100,000 | 99,989 | 99,992 |
| Default | R@100 | 0.99989 | 0.99992 |
| Default | p05 hits/query | 100 | 100 |
| Default | mean visits/query | 63,233.532 | 64,019.806 |
| Default | p50/p90/p95/p99 ms | 39.396/54.989/59.362/67.409 | 40.027/55.876/60.768/68.317 |
| Default | search RSS KiB | 1,947,548 | 1,948,272 |
| Diagnostic PQef256/shortlist256/exactef128 | GT100 hits / 100,000 | 99,629 | 99,673 |
| Diagnostic | p05 hits/query | 98 | 98 |
| Diagnostic | mean visits/query | 7,012.914 | 7,096.399 |
| Diagnostic | p50/p90/p95/p99 ms | 4.057/5.818/6.306/7.055 | 4.127/5.904/6.431/7.258 |

Baseline and candidate root SHA-256 were respectively
`bdc994e1f58c817ebfd95c7ddaa3aca37fe182f7dc73a569a61ad39a73ec2565`
and `963b4dff10aef94ba7b4497bd1bb1504bd05816b59f11cc1d9631f20732abe59`.
All baseline edges remained. The candidate added 227,225 edges, at most16 per
source; maximum out-degree rose71→83, minimum candidate in-degree was16,
and all1M rows were reachable. Graph bytes rose267,213,494→268,122,394
(+0.34%). Baseline/candidate builds took2,094.118/2,002.518 s and peak RSS
was5,553,860/5,534,980 KiB. Baseline share of nodes at in-degree≤8 was
1.1835% here, versus21.8157% for the CoHere-1M V278 baseline; that
structural contrast explains why this fixed method has fewer eligible targets,
but is not evidence for a validated adaptive policy.

**Frozen decision: `no_material_gain`; retain the baseline graph as the
production default.** Baseline had only11 misses; the candidate recovered3
of them while p95 rose2.37% and p99 rose1.35%. The preregistered rule for
fewer than100 baseline misses required no p95/p99 slowdown. V278's CoHere
gain remains real but does not establish a generic default. The reverse-edge
builder remains an experimental quality/memory option; there is no new
dataset-specific threshold or 100% recall target. The next product gate is
same-revision end-to-end HTTP serving with frozen baseline generation,
recording recall, p50/p90/p95/p99, throughput, RSS, bytes/GETs, cache and
transport under a disclosed workload before any vendor claim.
