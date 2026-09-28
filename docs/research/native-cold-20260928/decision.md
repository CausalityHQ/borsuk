# Native client-cold S3 serving: verified integration PASS

The existing nearest-generation library is now exercised through real S3
publication, authenticated head read, remote open and64 complete search calls.
Added a development-only --live-s3 mode to two_bit_plan_demo; ordinary and
paired modes retain their behavior. No Rust library/default/dependency changed.
The diverse graph remains KILL. This is serving integration and a measured
nearest baseline, not a production or vendor qualification.

## Compact convergence row

Verified ReLAION first100k, D768, cosine, k100, consumed development0–63:

| Baseline and result | Actual quality delta | Complete native call p90/p95 | Throughput/cost | Remaining gap / next decisive test |
| --- | --- | --- | --- | --- |
| Nearest offline control99.15625%; native live99.15625%; exhaustive SQ8 99.515625% | 0pp; exact plans, ordered returned IDs and6346/6400 GT hits; p05 97 | 107.846593 /108.673862ms | Serial observed9.269621QPS; final-cell compute estimate$0.0048, excludes EBS/S3; lifecycle dollars unmeasured | BOTH vendors unmeasured; quality744 still fails; distinct bounded discovery/nomination change must pass both datasets |

Native p50/p99=103.740819/195.836535ms. Query process CPU p50/p90/p95/p99=
106.418029/109.064190/110.547142/124.501012ms, summed across process threads.
Whole64-query loop6.904273373s includes record-writing overhead. Call timings
include routing, nomination, fresh conditional S3 data fetches, authentication
and native SQ8 ranking; exclude request parsing/response serialization. No
incoming service HTTP API is measured. All64 include the first; no warm-ups.
Router/metadata resident, no client SQ8 cache, HTTP connections reusable,
S3 server cache uncontrolled. QPS is one serial observed cell, not sustainable
throughput or a concurrent serving curve. No same-cell competitor exists.

Actual submitted data GETs1615, verified response bytes1,072,206,720, failed
reads0. Each query matches its frozen plan, <=32 GETs/16,773,120B, and exactly
100 ordered returned IDs. Source/metadata/credential requests are outside those
data counters. Existing tested OneAttemptS3 disables SDK data retries; no
failed unverified-byte or network-packet accounting is inferred.

## Authority, startup and resource evidence

The closed topology-a0003 nearest source/order/SQ8/plane/centroid/graph/score
cache/query/truth identities are frozen in config.json. SQ8 conditionally
created at the unique namespace's objects/SHA key; actual ETag/length pinned.
Only three root string bindings changed: SQ8 key, ETag, canonical key. Closed
verification restores them and requires byte equality with original root;
all numeric/query-dependent tokens are preserved. Ordinary64 plans reproduce
SHA789300785215a649a6f16a9ca56da23246444aee33501aba2551ace3d6f3637b.

The public publisher streams/authenticates canonical/source metadata, creates
head last, and the driver verifies the returned/read root/generation/epoch.
Initial prepared root base_epoch0 publishes head-v2 epoch1. open_remote stages
authenticated metadata only. Complete search uses signed ETag-pinned page
GETs and native scoring; query replay never hydrates canonical vectors.

Measured native publication7.200317634s, authenticated head read56.147727ms,
remote metadata open529.455790ms. Native process total15.01s including startup/
queries, maxRSS81,824KiB/zero swap. Whole preparation/native/parity worker28.33s,
maxRSS140,920KiB; whole measurement cgroup peak572,182,528B includes file cache/
kernel/publication/parity, swap peak0B, OOM events0. No isolated steady-serving
or generation-swap RSS claim.64 native searches/scoring calls plus64 separate
ordinary-plan preflight calls. Wrapper only sorts frozen cached scores after
native completion; zero new exhaustive SQ8 kernels. The immutable worker
result's query_scoring_kernel_calls=0 describes the Python wrapper only;
verification.json explicitly records native_search_calls=64.

Final cell Causality c7g.2xlarge Spot, eu-central-1c, same AL2023 ARM image,
4 Tokio threads/affinity0–3,32-way intra-query reads, max active queries1,
1GiB admitted generation,4GiB process address cap/8GiB cgroup/zero swap,
80GiB encrypted disposable gp3. Source/instance/profile/quote and all bounds
are in reservations/preregistration. Native code391-file identity, original
compiler receipts and focused GREEN test reused exactly; no duplicate build
or full workspace assurance after controller-only fixes. Unchanged library/
deps retain prior2684-pass assurance.

## Closed attempts

| Cell | Observed result | Closure / compute estimate |
| --- | --- | --- |
| red/a0001 | Intended live-split stub failure;393 source/four artifact checks | i-07599ed70bae7f1c1, terminated398s; $0.0199 |
| green/a0001 | Compile error: constructor RangeFetchError lacks std::error::Error; mapped only in demo | i-0c12c77f822c221a9, terminated378s; $0.0189 |
| green/a0002 | Release build and focused unit GREEN; controller import missing boto3; replaced by installed AWS CLI | i-015b4ee0b3da2ec43, terminated615s; $0.0307 |
| green/a0003 | Cached bootstrap overescaped hash-record newline; corrected to plain records | i-0ac07aea900a25f83, terminated61s; $0.0031 |
| green/a0004 | Valid publication/open/search and full ordered-ID/count/plan parity; independent verification passed | i-0ab7faed97cf0b8d9, terminated97s; $0.0048 |

All failed GREEN attempts stopped before corpus publication/query; retained
as invalid engineering cells, not performance observations. Cost values are
Spot quote times observed wall, estimates excluding EBS/S3, not invoices or
lifecycle costs. Published308MB canonical/78MB SQ8/metadata remain immutable
S3 evidence. No active compute, consultation or duplicate run remains.

Final archive86018fe4fa2fff505cb06ceeb5af7a05fa8a82fa807294327239828e22bcf0c4;
terminal8aec47d5a88a0816b524a2a2199c43ff85ee9061342aa182611f1acb5f706abd.
396 relevant source files matched; all18 artifacts checked, including reused
driver11,390,680B SHA5a78c8d19fd374616c7649c08f17f55deec603ae0c521f0e17719530b2cd0a2f.
Published head/root/components, canonical/SQ8 HEAD identities, every GT count,
latency/QPS reductions and instance termination independently verified.

## Quality blocker and next causal gate

Historical nearest ReLAION method-validation256–999(744) remains
98.8508064516% returned versus99.5685483871% exhaustive SQ8: deficit0.717742pp
above0.5pp. This live cell does not repair or rerun it. CoHere live/validation
remain unrun. Its latest matched offline development0–63 values are control
99.09375%, failed diverse99.078125%, exhaustive99.234375%. Old vendor/
architecture timings are stale. Fresh quality, maintenance quality/cost,
1M/10M/100M and BOTH matched vendor wins remain open.

Closed trace reduction supplies a cheap limit for a distinct discovery union:
nearest/diverse candidate GT unions6392 ReLAION/6395 CoHere; physical GT unions
6392/6393. These are available GT-set counts, not a new returned-recall result.
Direct physical union violates16,773,120B on57/64 ReLAION and40/64 CoHere
queries; violates32 ranges on14/64 and13/64. Maxima19,169,280B/36 ranges and
18,969,600B/35 ranges. Therefore do not launch simple union fetching or a cap
sweep. A distinct source-derived discovery union would need one bounded,
source-score nomination pass under unchanged physical caps; nomination quality
and doubled discovery cost/residency are unmeasured. Test that mechanism
cheaply before any default/format change. It must pass both frozen development
gates before fresh quality/scale confirmation; use this runnable cold path for
a survivor. No new architecture/reviewer cycle was launched in this slice.

Read-only ListVectorBuckets responds under Causality/eu-central-1, zero listed
buckets; create/query permission or benchmark readiness not inferred. No
Turbopuffer credential variable is visible in this session environment; access
must be established when a qualified matched comparison is ready. No operator
decision is required now. The complete product goal remains active.
