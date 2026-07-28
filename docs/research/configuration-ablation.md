# Configuration and Concurrency Ablations

The production configuration has independent recall, I/O, latency, and memory
controls. Treating them as one “width” obscures the actual tradeoffs.

The historical v7 build-layout ablation compares `ingest-preserving` finalization against
`reclustered` full compaction. Both use the same global product-PQ algorithm and
configuration, but each artifact is trained from its final segment traversal,
so recall is remeasured rather than assumed equal. The comparison reports build
time and peak CPU/RAM/disk alongside recall, serving GETs, bytes, and latency:
reclustering is promotable only if its GET/latency gain justifies its larger
temporary build working set.

## Knob model

| Knob | Changes recall? | Primary cost | Production default/policy |
|---|---|---|---|
| cell rows | indirectly; changes the recall frontier | object count, bytes/decode, build time | dimension-aware |
| `BuildConfig::global_pq_layout` | yes | routing selectivity, resident centroids, code-object fan-out | `Adaptive`; explicit flat/product/hierarchy variants require fresh curves |
| `nprobe` | yes | global coarse-PQ cells, code-chunk GETs, bytes, scan work | persisted coarse-cell rule; tune to recall target |
| global candidates | yes | exact-rerank rows, sidecar ranges, CPU | persisted dimension/corpus rule; tune after `nprobe` |
| prefetch width | no if all probes finish | latency waves, burst I/O, transient state | 16 per query, under the shared cap of 24 |
| query admission cap | no | queueing and total concurrent query state | 4 |
| `BORSUK_CPU_THREADS` | no | build/query scoring CPU saturation vs latency | 4 process-wide compute workers; research sweeps 1/2/4/8 |
| `BORSUK_IO_THREADS` | no | object-store wait overlap, thread stacks, tail latency | 24 process-wide 256 KiB-stack waiters; research sweeps 4/8/16/24/32 |
| global decode cap | no | active Parquet/Arrow/code decode state | 24 |
| resident RAM budget | no | resident routing/codebook/chunk metadata | hard 512 MiB library default |
| exact-sidecar metadata cache | no | range-index hits vs RAM | global 128 MiB LRU |
| global-PQ code wave | no | transient code bytes vs request waves | at most 32 chunks **and 32 MiB/query**; four production queries retain at most 128 MiB of code payload |
| decoded cache budget | no | retained RAM vs repeat decode | disabled unless explicitly sized |
| `BORSUK_BUILD_SCRATCH_DIR` | no | temporary build disk, avoids corpus-sized RAM | `.borsuk-scratch` under process working directory |

Same-cell and same-graph single-flight coalesce overlapping checksum
fetch/decode work without retaining an unbounded decoded cache. Graph traversal
state remains private to each query, while immutable segment and adjacency
arrays are shared by `Arc`. The query admission gate is FIFO so a sustained
caller cannot starve users already queued behind the global production cap.

On the selected Fashion graph point, replacing the unfair gate with FIFO reduced
16-user maxima from 4,358–4,936 ms to 1,229–1,257 ms across three runs. The new
tail matches four waves of the roughly 300 ms disk-cached service time. Worst
RSS did not improve (425 MiB versus 387 MiB), so decoded graph single-flight is
documented as overlap protection rather than a general memory reduction.

Cache-state publication runs set the decoded-cache budget to zero. A nonzero
decoded cache can make a repeated query a process-memory hit and therefore
cannot be labeled `disk_cached`; it also historically forced pq-scan away from
its projected path. The current engine fixes the latter—an empty configured
cache no longer disables projection—while `warm()` remains the explicit
`memory_preloaded` state.

## Current v8 dimension-aware layout

Physical build/decode memory is approximately `rows × dimensions × 4`. V8
targets roughly 16 MiB of float32 rows, clamped to 64–131,072 rows. Global
coarse cells are independent from these physical segments. Query memory stays
bounded because only selected product-code chunks are loaded, in fixed waves of
at most 32 chunks and 32 MiB/query, and only each wave's top candidates survive.

| Representative dimensions | Default rows/segment |
|---:|---:|
| 96 | 43,690 |
| 100 | 41,943 |
| 128 | 32,768 |
| 256 | 16,384 |
| 512 | 8,192 |
| 784 | 5,349 |
| 960 | 4,369 |
| 1,024 | 4,096 |
| 2,048 | 2,048 |
| 4,096 | 1,024 |
| 8,192+ | 512 |

## Historical v6 Fashion-MNIST matched-recall layout sweep

Every row meets or exceeds the directly measured S3 Vectors recall of 0.985.
The source is
[`aws-cell-layout-fashion-2026-07-20.csv`](../web/assets/benchmarks/aws-cell-layout-fashion-2026-07-20.csv).

| Rows/cell | Matched `nprobe / candidates` | recall | uncached p95 | disk-cached p95 | bytes/query | GETs | startup | peak RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4096 legacy | `22 / 12` | 0.986 | 198–229 ms | 79–83 ms | 95.40 MB | 44 | — | 2.63 GiB |
| 1024 | `6 / 11` | 0.988 | 109.0 ms | 20.7 ms | 7.01 MB | 12 | 388 ms | 407.0 MiB |
| 512 | `6 / 11` | 0.989 | 87.6–132 ms | 11.4–12.6 ms | 3.52 MB | 12 | 613–655 ms | 182.6–242 MiB |
| 256 | `9 / 10` | 0.986 | 95.7 ms | 5.5 ms | 2.61 MB | 18 | 913 ms | 244.8 MiB |
| 128 | `15 / 10` | 0.988 | 103.4 ms | 3.6 ms | 2.32 MB | 30 | 1.41 s | 234.0 MiB |

512 was the balanced 784D v6 default. Smaller cells improve disk-cached scan time
but need more probes and requests and did not reduce total process RSS. Fashion
compaction took 49.4 s at 512 rows, 72.4 s at 256, and 111.5 s at 128.

The historical GIST run evaluates the same old byte-target policy at 960 dimensions. Its 512-row
layout is compared with legacy 4096-row cells in
[`aws-gist-cell-layout-2026-07-20.csv`](../web/assets/benchmarks/aws-gist-cell-layout-2026-07-20.csv):
serving peak RSS falls 44%, disk-cached p95 falls 60%, and query bytes fall 40%,
but GETs rise 4.5×, four-worker QPS falls 14%, and the offline build peaks at
31.2 GiB. Cell size is therefore configurable; the dimension-aware value is a
bounded-memory v6 choice, not an unconditional optimum.

Historical GloVe evidence shows why the byte target remains configurable. At
100 dimensions, 4096 rows was the 512-KiB v6 default; a graph-free 2048-row
(roughly 256-KiB code)
index needed `nprobe=128` to match the 4096-row profile's 0.951 recall. Source:
[`aws-glove-cell-layout-2026-07-20.csv`](../web/assets/benchmarks/aws-glove-cell-layout-2026-07-20.csv).

| Layout / bounded I/O profile | uncached p95 | disk-cached p95 | 4-worker QPS | peak serving RSS | GETs / bytes |
|---|---:|---:|---:|---:|---:|
| 4096 rows, legacy graph-enabled, width/cap `32/24` | 382.9 ms | 46.9 ms | 36.6 | 749 MiB | 160 / 55.1 MB |
| 4096 rows, legacy graph-enabled, width/cap `32/32` | 278.9 ms | 39.1 ms | 34.9 | 751 MiB | 160 / 55.1 MB |
| 4096 rows, graph-free, width/cap `32/24` (three-run median; worst RSS) | 392.2 ms | 46.9 ms | 36.4 | 783 MiB | 160 / 55.1 MB |
| **4096 rows, graph-free, width/cap `32/32` (selected; three-run median; worst RSS)** | **302.2 ms** | **38.4 ms** | **36.2** | **727 MiB** | **160 / 55.1 MB** |
| 2048 rows, width/cap `32/24` | 626.1 ms | 39.2 ms | 42.4 | 636 MiB | 256 / 44.7 MB |
| 2048 rows, width/cap `64/32` (three-run median; worst RSS) | 303.2 ms | 33.4 ms | 34.5 | 727 MiB | 256 / 44.7 MB |

The smaller cells cut bytes and cached latency but raise GETs 60%. More bounded
prefetch recovers most of the network latency; it only ties the graph-free
4096-row cap-32 median while requiring 256 rather than 160 GETs. The graph-free
4096-row prefix is also slightly smaller (2.544 versus 2.556 GB) and contains
2,242 rather than 4,422 objects. The 512-KiB layout was therefore the selected
v6 request-efficient layout, while 2048 rows was a measured disk-cached tuning
option. The three 2048-row cap-32 uncached p95s were 299.6–309.5 ms and
disk-cached p95s were 33.3–33.8 ms.

## Probe and candidate frontier

For v8, `nprobe` selects vector-level global coarse-PQ cells. Each cell contains
matching code/location rows from every bounded ingest checkpoint; it is not one
physical segment. The candidate budget selects the whole-query exact-rerank shortlist.
Omitting either uses the persisted build-time production choice.
Recall/latency research must sweep both:
first find `nprobe` values that can meet the target, then find the smallest
candidate budget that retains it. More probes increase code GETs and ADC work;
more candidates increase exact-sidecar ranges, bytes, and CPU.

### GloVe fixed-page I/O scheduling and width ablation (2026-07-21)

The first fixed-page/code64 GloVe index isolated a runtime error: blocking S3
reads were scheduled on the four-worker compute pool, so configured read widths
above four were illusory. Query-only reuse of the identical immutable index
with a separate process-wide 24-waiter I/O pool cut uncached p95 without
changing recall, bytes, or GETs. Raising the public per-query width from 8 to
16 cut another latency wave under the unchanged shared cap of 24.

| I/O profile | probes / candidates | recall@10 | uncached p95 | disk-cached p95 | GETs/query | peak RSS | peak sampled CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| compute-pool I/O (rejected) | `64 / 184` | 0.963 | 1,371.4 ms | 15.5 ms | 217.82 | 274.0 MiB* | 428%* |
| separate I/O pool, width 8 | `64 / 184` | 0.963 | 535.2 ms | 14.2 ms | 217.82 | 130.8 MiB | 460% |
| separate I/O pool, width 16 | `64 / 184` | 0.963 | 345.7 ms | 11.6 ms | 217.82 | 139.0 MiB | 557% |
| separate I/O pool, width 16 | `96 / 184` | 0.984 | 476.0 ms | 16.2 ms | 283.96 | 139.0 MiB | 557% |
| separate I/O pool, width 16 | `128 / 184` | 0.992 | 574.0 ms | 21.4 ms | 349.01 | 139.0 MiB | 557% |
| separate I/O pool, width 16 | `160 / 184` | 0.998 | 670.2 ms | 26.5 ms | 413.69 | 139.0 MiB | 557% |
| separate I/O pool, width 16 | `192 / 184` | 1.000 | 798.3 ms | 30.5 ms | 477.96 | 139.0 MiB | 557% |

`*` The rejected run includes the source build, while the width-8/16 rows are
query-only reuse runs; its RSS/CPU cell is therefore not a serving-only
comparison. The query-only resource rows still show the important cost of
wider overlap: lower wall time does not mean lower instantaneous CPU. The
remaining uncached latency rises almost linearly with 218–478 small GETs, so
the next accepted experiment changes physical row locality to reduce range
GETs rather than raising the global concurrency cap.

Raw evidence:
[`compute-pool I/O`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r4-rejected-cpu-bound-io/),
[`I/O pool / width 8`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r5-io24-width8/), and
[`I/O pool / width 16`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r6-io24-width16/).

A fresh descriptor-v7 rebuild of the selected adaptive angular layout reproduced
the quality curve rather than relying on the reused r6 index:

| probes / candidates | recall@10 | uncached p95 | disk-cached p95 | GETs/query | bytes/query |
|---:|---:|---:|---:|---:|---:|
| `64 / 184` | 0.963 | 588.6 ms | 9.3 ms | 212.47 | 20.87 MB |
| `96 / 184` | 0.985 | 495.1 ms | 13.3 ms | 278.77 | 30.30 MB |
| `128 / 184` | 0.991 | 552.4 ms | 17.4 ms | 343.99 | 40.53 MB |
| `160 / 184` | 0.996 | 683.8 ms | 21.7 ms | 408.54 | 51.15 MB |
| `192 / 184` | 1.000* | 759.1 ms | 25.8 ms | 472.77 | 62.01 MB |

`*` Empirical recall on the 100-query benchmark, not the formal exact-mode
guarantee. The fresh build plus sweep peaked at 350.1 MiB RSS and 523.1 MiB
scratch disk. Whole-run mean sampled CPU was 86.7% of one core with a brief
612.6% 100-ms peak; build and ADC work remained on the four-worker CPU pool,
while the separate I/O pool waited on S3. This run confirms that the remaining
uncached tail is request fan-out: local disk-cached compute is 9–26 ms, while
the S3 path issues 212–473 GETs. Raw evidence and resource trace:
[`glove-100-r12-adaptive-flat256`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r12-adaptive-flat256/).

The full bit-plane Morton recreation preserved the same 0.963/0.984 recall but
also produced exactly the same 217.82/283.96 GETs and byte counts at 64/96
probes. Its p95 did not improve, so row ordering alone is rejected as the
solution: the 184-row shortlist is too sparse across cells for one ordering to
make the ranges contiguous. Raw evidence is under
[`glove-100-r7-rejected-full-morton`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r7-rejected-full-morton/).
The next recreation widened low-dimensional paged codes from 64 to 128 bytes.
At `192 probes / 16 candidates` it retained 1.000 recall, but GETs fell only
477.96→406.70 while bytes rose 63.71→120.26 MB, uncached p95 rose
798.3→991.2 ms, disk-cached p95 rose 30.5→56.0 ms, and live RSS approached
386 MiB. It is rejected: shortlist fidelity was not the limiting end-to-end
resource. Raw evidence is under
[`glove-100-r8-rejected-code128`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r8-rejected-code128/).
The remaining root cause is coarse routing: perfect recall on the 256-cell
layout requires 192 probes, so the next engine experiment must reduce the
fraction of code pages scanned rather than widening each page.

Descriptor v8 also tests a physical request-granularity change independently
of the learned layout. Cell chunks are packed into content-addressed bundles
with at most 1 MiB of contiguous code slices and 32 MiB total. A query still
scores exactly the selected chunks, but selected slices sharing a bundle are
served by one range GET; fixed-width lossless ranges remain separate and are
read only for the rerank shortlist. The caps preserve the 32 MiB/query code-wave
bound and limit build assembly memory. Fresh flat-256 recreations preserve the
descriptor-v7 recall curve while reducing request fan-out:

| Dataset / point | recall@10 | v7 → v8 GETs/query | v7 → v8 uncached p95 | v8 disk-cached p95 | v8 peak RSS |
|---|---:|---:|---:|---:|---:|
| GloVe `96 / 184` | 0.985 | 278.77 → 148.32 | 495.1 → 365.4 ms | 14.5 ms | 359.8 MiB |
| GloVe `192 / 184` | 1.000* | 472.77 → 178.36 | 759.1 → 448.5 ms | 25.0 ms | 359.8 MiB |
| NYTimes `192 / 320` | 0.958 | 435.11 → 75.50 | 701.8 → 312.5 ms | 12.3 ms | 279.5 MiB |
| NYTimes `256 / 320` | 0.976 | 563.46 → 78.46 | 947.0 → 394.8 ms | 16.4 ms | 279.5 MiB |

`*` Empirical on 100 benchmark queries; exact mode remains the only formal 1.0
guarantee. GloVe uses 523.1 MiB peak scratch and 96.8% whole-run mean sampled
CPU; NYTimes uses 289.0 MiB scratch and 62.5% mean CPU. These first-pass rows
qualify the packed mechanism and memory envelope. Production promotion still
requires independent selected-point and bounded-concurrency repetitions. Raw
evidence:
[`glove-100-r14-packed-flat256`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r14-packed-flat256/) and
[`nytimes-256-r4-packed-flat256`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r4-packed-flat256/).

Three independent clean-process GloVe serving repetitions at the empirical
1.000 point (`192 / 184`) reproduce the first pass. Median uncached p95 is
467.3 ms (range 450.6–472.1), median zero-GET disk-cached p95 is 25.0 ms, and
worst serving RSS is 148.8 MiB. With 16 callers and the four-search FIFO cap,
median p95 is 298.0 ms and the worst maximum is 307.3 ms; throughput is
55.6–57.0 QPS. The absence of a multi-second overload tail qualifies the
admission/memory behavior, but the roughly 0.47 s uncached p95 remains a
latency optimization target rather than a claim of completion. Consolidated
evidence:
[`aws-v8-packed-glove-production-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-v8-packed-glove-production-repetitions-2026-07-21.csv).

An all-cell NYTimes candidate sweep then isolates the remaining quality loss.
At 256 probes, widening the lossless rerank shortlist from 320 to 512 raises
recall from 0.976 to 0.990, but 768 and 1,024 both plateau at 0.991. Uncached
p95 rises from 382.7 to 486.9 ms and exact-range GETs from 78.64 to 406.94.
Because every coarse cell is already scanned, the missing neighbors are below
the 64-byte product-PQ ranking cutoff; more routing probes cannot help. Raw
evidence:
[`nytimes-256-r5-packed-candidates`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r5-packed-candidates/).

A fresh 128-byte-code recreation isolates that variable. At the same all-cell
route, recall reaches 0.993 with 288 candidates, 418.9 ms uncached p95,
21.6 ms zero-GET disk-cached p95, 38.9 MB/query, and 83.15 GETs/query. Increasing
the shortlist through 320/384/512/768 never changes recall, while the 768 point
raises uncached p95 to 568.6 ms and GETs to 295.81. Whole build+sweep peak RSS
is 258.0 MiB, sampled scratch is 311.0 MiB, and whole-run mean CPU is 91% of
one core. Thus 128 bytes improves the measured quality ceiling from 0.991 to
0.993 without increasing process memory, but reranking beyond 288 is rejected.
The remaining default-selection work is a probe sweep; 0.993 is empirical, not
a formal perfect-recall claim. Raw evidence:
[`nytimes-256-r6-packed-code128`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r6-packed-code128/) and
[`nytimes-256-r7-code128-candidates`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r7-code128-candidates/).

A ten-query exact control verifies the ground-truth path rather than silently
assuming it. Exact and `256 / 288` code128 ANN both return 1.000 recall on that
subset. Exact uncached/disk-cached p95 is 8,661/5,187 ms and reads 405.6 MB per
query; ANN is 412/21.7 ms and reads 39.2/38.3 MB. This does not upgrade the
100-query ANN result from empirical 0.993, but it confirms that formal 1.0 is
available through lossless exhaustive verification and makes its CPU/I/O cost
explicit. Raw evidence:
[`nytimes-256-r8-exact-control`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r8-exact-control/).

The initial full probe sweep selected 224 rather than all 256 cells. Recall
rises 0.888/0.906/0.935/0.953/0.974/0.993 at
64/96/128/160/192/224 probes; 256 remains 0.993. The selected 224 point is
369.2/19.4 ms uncached/disk-cached p95 and 37.7/37.1 MB, versus 403.9/21.3 ms
and 38.9/38.3 MB at 256. This is the smallest measured point preserving the
code128 ceiling, but scanning seven eighths of the cells also proves that
flat-256 routing—not process memory—is now the dominant optimization target.
Raw evidence:
[`nytimes-256-r9-code128-probes`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r9-code128-probes/).

A subsequent one-cell boundary sweep resolves the production setting: 220
probes reaches 0.985, 221–222 reach 0.989, and 223 is the first point at the
0.993 ceiling. Its uncached/disk-cached p95 is 376.9/19.6 ms with 37.7/37.0 MB
read per query. The persisted default is therefore 223/288; 224 remains a
repetition point rather than the minimum high-recall setting. Raw evidence:
[`nytimes-256-r15-boundary-probes`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r15-boundary-probes/).

A final 256-byte control rejects the tempting assumption that still wider PQ
codes automatically close the gap. At 224/288 it remains 0.993 but costs
418.8/29.3 ms uncached/disk-cached p95 and 71.7/71.1 MB, versus 369.2/19.4 ms
and 37.7/37.1 MB for code128. Even all 256 cells and reranks of
384/512/768/1,024 remain exactly 0.993 while uncached p95 rises from 464.4 to
561.0 ms and GETs from 168.16 to 440.95. Build+sweep peaks at 284.1 MiB RSS,
346.5 MiB scratch, and 139% mean CPU. Code256 is therefore a reproducible
rejected ablation; it is not a default and it does not justify a staged
refinement layer on this evidence. Raw evidence:
[`nytimes-256-r10-packed-code256`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r10-packed-code256/) and
[`nytimes-256-r11-code256-candidates`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r11-code256-candidates/).

The same diagnosis holds at Deep-Image scale. A fresh 9.99M-row fixed-page
build peaked at 348.6 MiB RSS and 4.21 GiB sampled scratch disk; the scratch is
the external partition stream, not process memory. Its bounded-width query
sweep recovered 0.975/0.984/0.987/0.990/0.993 recall at
64/96/128/160/192 probes, but uncached p95 increased from 757 to 983 ms and
GETs from 252 to 513. Disk-cached p95 was 45.7–85.9 ms. The recall regression
is therefore fixed, while the product coarse router remains too weak near the
top of the curve. Query-only peak RSS was 266.5 MiB; neither the build nor
query retained the 9.99M-vector matrix. Raw evidence:
[`Deep build`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/deep-image-96-r2-code64-old-locality-build/)
and
[`Deep query curve`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/deep-image-96-r3-code64-io24-width16/).

The fresh descriptor-v8 hierarchy replaces those independent product cells
with a 64-way full-dimensional parent and 64 full-dimensional local leaves per
parent. On the same 9.99M Deep corpus it reaches
0.978/0.987/0.990/0.994/0.995/0.998/0.999 recall at
64/96/128/192/256/384/512 probes. The production-comparison point is 96/200:
403.2/21.1 ms uncached/disk-cached p95, 30.3/28.4 MB, and 145.32 GETs. The
high-recall 384 point is 889.0/57.0 ms and 98.4/96.5 MB; 512 crosses 1.07 s and
126.4 MB for the final 0.001 empirical recall gain. Relative to the rejected
product router, the hierarchy improves recall and cuts the 0.99x frontier's
latency/request amplification, so it is now the adaptive large-angular layout.

A separate candidate-width control holds routing at all 512 probes. Increasing
the exact shortlist from 256 through 320, 384, and 512 leaves recall fixed at
0.999. The endpoints move from 1,098.3/71.0 ms to 1,236.6/73.9 ms
uncached/disk-cached p95 and from 344.39 to 392.75 GETs/query. The final 0.001
is therefore not recoverable by retaining more exact-rerank rows from the same
ADC ordering; widening candidates is rejected. Raw evidence:
[`Deep candidate-width control`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/deep-image-96-r5-hierarchical-candidates/) and
[`isolated 512-candidate point`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/deep-image-96-r5b-candidate512/).

Fresh construction took 896 s ingest plus 653 s final publication. The entire
build+sweep peaked at 461.4 MiB RSS, 4.20 GiB sampled scratch, and 96.4% mean
CPU (brief 524% sampler peak). Scratch returns to zero after immutable
publication. At 50M+ rows the child fan-out increases under the same 32 MiB
centroid-table cap so 100M does not simply put ten times as many rows in each
Deep-scale cell. Raw evidence and resource trace:
[`deep-image-96-r4-packed-hierarchical64`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/deep-image-96-r4-packed-hierarchical64/).

The first hierarchical GloVe ablation intentionally tried 4,096 leaves at
ordinary million scale. It cut disk-cached p95 to 1.1–2.8 ms and held uncached
p95 to 121–326 ms across 4–64 probes after the first S3 pass, but recall rose
only from 0.474 to 0.839. Peak build-plus-query RSS was 323.8 MiB and sampled
scratch was 528.7 MiB; mean whole-run CPU was 34.8% with brief 100-ms sampler
spikes. The layout is rejected because tiny cells did not qualify recall. It
directly motivates the 1,024-leaf ordinary-million default now being recreated.
Raw evidence:
[`glove-100-r9-rejected-hierarchical-4096`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r9-rejected-hierarchical-4096/).

Reducing the hierarchy to 1,024 leaves improved GloVe recall at the same probe
count, but the first implementation hard-assigned each corpus vector through
only its nearest parent. It reached 0.923/0.936/0.960 recall at 96/128/192
probes with 461/576/776 ms uncached p95; that remains worse than the original
flat 256-cell router's recall. The build was materially cheaper—157 s final
publication versus 351 s for 4,096 leaves—and peaked at 310 MiB RSS, but it is
rejected on quality. Descriptor v7 now evaluates the four nearest parents
during construction and selects the best full-dimensional child across them,
removing the hard parent-boundary error without changing query-time memory or
fan-out. Raw evidence:
[`glove-100-r10-rejected-hard-parent-1024`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r10-rejected-hard-parent-1024/).

Checking four parents did not rescue the GloVe hierarchy: recall moved only
0.843→0.851 at 32 probes, 0.896→0.899 at 64, and 0.936→0.940 at 128. This
falsifies hard parent boundaries as the main GloVe loss. The layout policy
cannot be universally hierarchical: normalized corpora retain the measured
flat-256 router at ordinary million scale, while Euclidean corpora use the
hierarchy. A fresh NYTimes hierarchy independently confirmed the angular
failure, reaching only 0.696 at 32 probes. A subsequent 2x64 product-routing
control above 5M was also rejected; the separately measured full-dimensional
hierarchy is the qualified large-angular layout described below. Raw evidence:
[`glove-100-r11-rejected-parent4-1024`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r11-rejected-parent4-1024/) and
[`nytimes-256-r1-rejected-parent4-1024`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r1-rejected-parent4-1024/).

The fresh adaptive NYTimes rebuild restores flat-256 and recovers 0.958 recall
at `192 probes / 320 candidates`, but needs 435.11 GETs and 701.8 ms uncached
p95; its zero-GET disk-cached p95 is only 9.3 ms. Scanning all 256 cells reaches
0.976 at 947.0/11.8 ms uncached/disk-cached p95 and 563.46 GETs. Peak
build-plus-sweep RSS is 262.0 MiB, scratch 298.4 MiB, and whole-run mean sampled
CPU 53.1% of one core (brief 756.9% 100-ms peak). Thus flat routing fixes the
hierarchy's quality regression but not object fan-out. Raw evidence:
[`nytimes-256-r2-adaptive-flat256`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r2-adaptive-flat256/).

The completed fresh `Product2x64` ablation is rejected on both angular
corpora. It creates 4,096 Cartesian cells, but matching the flat layout's
recall requires far more probes and requests:

| Dataset | product probes / candidates | recall@10 | uncached p95 | disk-cached p95 | GETs/query | whole-run peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| GloVe | `128 / 184` | 0.817 | 1,047.3 ms | 6.8 ms | 330.86 | 335.0 MiB |
| GloVe | `1024 / 184` | 0.976 | 3,158.8 ms | 28.4 ms | 2,147.18 | 335.0 MiB |
| NYTimes | `128 / 320` | 0.659 | 985.2 ms | 3.0 ms | 343.39 | 256.6 MiB |
| NYTimes | `1024 / 320` | 0.889 | 3,090.6 ms | 15.9 ms | 2,157.34 | 256.6 MiB |

This is not a RAM failure: both builds remain below the 512 MiB envelope and
use 528.4/302.2 MiB peak scratch. It is a routing-quality and request-fan-out
failure. Consequently `Product2x64` stays available only as a named research
ablation; it must not be selected merely because a corpus crosses a row-count
threshold. The scalable normalized-corpus successor must preserve
full-dimensional neighborhoods, bound centroid memory, and be qualified on
Deep-Image before it can become a default. Raw evidence and resource traces:
[`glove-100-r13-product2x64`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r13-product2x64/) and
[`nytimes-256-r3-product2x64`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r3-product2x64/).

The production candidate rule is evidence-based rather than one multiplier for
every dimension. The first production-shaped vector-level IVF Fashion-MNIST
run makes the code64 default `8 probes / 320 candidates`, measured at 0.989
recall versus the direct S3 Vectors target of 0.985. The smaller `8/256` point
reached 0.983; widening probes to 12 recovered only 0.002 recall while
increasing uncached p95 from 183.8 to 225.1 ms.

GIST demonstrates why code width, probes, and exact candidates must be tuned
independently. With code128, 32 probes needs 608 candidates merely to reach
0.985 recall. With code256, 24 probes and 96 candidates reaches 0.995 while
adding only 1.5% to the complete index and leaving build RSS effectively
unchanged. At fixed 24 probes, the measured candidate curve is:

| candidates | recall@10 | uncached p95 | disk-cached p95 | GETs/query |
|---:|---:|---:|---:|---:|
| 64 | 0.990 | 446.1 ms | 29.3 ms | 138.48 |
| **96 (default)** | **0.995** | **388.0 ms** | **29.3 ms** | **163.70** |
| 128 | 0.996 | 409.3 ms | 29.4 ms | 190.27 |
| 192 | 0.996 | 400.5 ms | 29.5 ms | 240.63 |
| 384 (max-recall research) | 0.997 | 467.3 ms | 30.0 ms | 376.70 |

The non-monotonic uncached samples are normal S3 variance; recall, local-cache
latency, bytes, and request count provide the controlled curve. The 96-row
default is the knee: 128 adds only 0.001, while 384 more than doubles exact
request fan-out for +0.002. A fast `16 probes / 96 candidates` profile remains
documented at 0.985 recall and 303.8/22.0 ms uncached/disk-cached p95. Raw
evidence is in
[`gist-960-r10` through `gist-960-r13`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/).

GloVe, SIFT, and Deep-Image retain their measured 184/184/200-candidate rules.

### Historical v7 resident scan

For the v7 resident global path, `nprobe` no longer controls the primary
shortlist. `max_candidates_per_segment` is interpreted as the whole-index ADC
shortlist/rerank budget. The persisted default is used when it is unset. Each
curve therefore sweeps candidates, then compares only points meeting the recall
target; cell `nprobe` remains relevant to the filtered/WAL/exact fallback.

On NYTimes-256, 256 candidates reached only 0.888 recall; 320 was the first
tested qualified point at 0.975 and 64.3 ms uncached p95. Increasing the budget
to 384/512/1024 raised recall to 0.983/0.990/0.992 but also raised bytes and
latency, so 320 is selected. On the first 64-subspace GIST artifact, 512 reached
0.935 and 768 reached 0.953. Those GIST latency samples overlapped a Deep-Image
build and are retained only as recall/configuration evidence; they are not
production latency claims. The 768-row requirement motivated the fresh
128-subspace GIST recreation. See
[`aws-global-pq-v7-candidate-sweeps-2026-07-21.csv`](../web/assets/benchmarks/aws-global-pq-v7-candidate-sweeps-2026-07-21.csv).

On the legacy Fashion layout, fixing `nprobe=22` and sweeping 10–32 candidates
found 0.981 recall at 11 candidates and 0.986 at 12. The 12-candidate row is the
lowest point meeting S3 Vectors' 0.985. The raw fine sweeps are under
[`s3-vectors-fashion/`](../web/assets/benchmarks/raw/2026-07-20/s3-vectors-fashion/).

The 128/256/512/1024 layout directories contain broad and fine `nprobe ×
candidates` grids. Layout selection always follows two stages: find the lowest
recall-qualified point, then compare latency, bytes, GETs, memory, and build
cost. Comparing layouts at unmatched recall is rejected.

## Historical v6 decode-cap ablation

At the selected 512-row, recall-0.989 profile:

| Global decode cap | uncached p95 | disk-cached p95 | 4-worker QPS | 4-worker p95 | peak RSS |
|---:|---:|---:|---:|---:|---:|
| 8 | 89.9 ms | 11.4 ms | 207.7 | 25.1 ms | 195.6 MiB |
| **12** | **87.6 ms** | 11.8 ms | **274.9** | **19.7 ms** | **182.6 MiB** |
| 16 | 132.1 ms | 11.6 ms | 314.0 | 17.2 ms | 241.5 MiB |
| 24 (earlier graph-enabled artifact) | 120.5 ms | 11.7 ms | 310.6 | 17.0 ms | 229.6 MiB |
| **24 (graph-free v6 engine, three-run median / worst RSS)** | **88.7 ms** | **11.9 ms** | **310.7** | **17.4 ms** | **193.2 MiB** |

Fashion alone slightly favors cap 12 for uncached tail latency, but it is not a
representative global-cap workload because six probes fit in one wave. The
cross-corpus final default is 24: the graph-free repetition set preserves
sub-194 MiB Fashion RSS while
removing avoidable decode waves on the 16/32-width profiles.

## Historical v6 GloVe decode-cap ablation

The earlier cap-12/16 rows come from
[`aws-decode-cap-glove-2026-07-20.csv`](../web/assets/benchmarks/aws-decode-cap-glove-2026-07-20.csv);
the final graph-free cap-24/32 rows come from
[`aws-glove-cell-layout-2026-07-20.csv`](../web/assets/benchmarks/aws-glove-cell-layout-2026-07-20.csv).
Recall, bytes, and requests are identical in every row.

| Global decode cap | uncached p95 | disk-cached p95 | 4-worker QPS | 4-worker p95 | peak RSS |
|---:|---:|---:|---:|---:|---:|
| 12 | 693 ms | 70.0 ms | 19.5 | 260 ms | 793 MiB |
| 16 | 426 ms | 53.1 ms | 25.8 | 190 ms | 847 MiB |
| 24, graph-free three-run median / worst RSS | 392 ms | 46.9 ms | 36.4 | 128 ms | 783 MiB |
| **32, graph-free three-run median / worst RSS** | **302 ms** | **38.4 ms** | **36.2** | **106 ms** | **727 MiB** |

On the final graph-free 4096-row index, cap 32 cuts both single-query and
four-worker p95 without increasing RSS; its QPS is within 1% of cap 24. It is
therefore the selected GloVe-specific profile. Cap 24 remains the cross-corpus
default because it bounds aggregate decode work more conservatively; tuning to
32 is evidence-backed for this width-32, 100-dimensional workload rather than a
silent global-default change.

## Historical v6 uncapped overload ceiling

The uncapped study sets width equal to `nprobe` and disables global admission.
It characterizes saturation and is not a production default. Source:
[`aws-uncapped-research-2026-07-20.csv`](../web/assets/benchmarks/aws-uncapped-research-2026-07-20.csv).

| Dataset | Users tested | Best QPS | p95 at highest users | experiment peak RSS |
|---|---:|---:|---:|---:|
| Fashion-MNIST | 1–64 | 96.9 at 8 | 2.46 s | 6.27 GiB |
| GloVe | 1–16 | 32.0 at 2 | 836 ms | 1.47 GiB |
| SIFT | 1–32 | 132.7 at 2 | 997 ms | 2.01 GiB |
| NYTimes | 1–16 | 26.5 at 1 | 2.03 s | 3.63 GiB |
| GIST | 1–8 | 17.1 at 1 | 1.76 s | 6.32 GiB |
| Deep-Image | 1–8 | 34.6 at 1 | 342 ms | 984 MiB |

Past saturation, more callers mostly multiply queueing, RSS, and tail latency.
This is the empirical basis for bounded query and decode gates.

## Resource evidence

Every layout, recall, width, production, repeat, cap, uncapped, and S3-comparison
experiment has a `resources.csv` with CPU, RSS/VMS, disk reads/writes, and cache
footprint. Rendered timelines are in
[`resources/`](../web/assets/benchmarks/resources/); raw samples are in
[`raw/2026-07-20/`](../web/assets/benchmarks/raw/2026-07-20/).

Three older local-harness artifacts remain available for regression history:
[`production_cold_warm.csv`](../web/assets/benchmarks/production_cold_warm.csv),
[`production_concurrency.csv`](../web/assets/benchmarks/production_concurrency.csv),
and [`production_recall.csv`](../web/assets/benchmarks/production_recall.csv).
Their “cold/warm” labels predate the explicit startup, uncached,
disk-cached, and memory-preloaded definitions, so they must not be mixed with
the dated AWS curves without relabeling the cache state.
