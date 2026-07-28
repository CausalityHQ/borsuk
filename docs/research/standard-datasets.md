# Historical standard-dataset evidence (invalidated)

> **Not current production evidence.** The July 20–22 rows below predate the
> standard Arrow IPC exact-vector sidecar and declared typed-vector storage.
> The format and read granularity changed, so their latency, memory, CPU, disk,
> request, and cost measurements cannot be carried forward. They remain only
> as historical/rejected research notes. Fresh empty-prefix AWS runs must
> satisfy the [market benchmark matrix](market-benchmark-matrix.md) before any
> result is promoted back to the website or default documentation.

This page preserves the old full-corpus SRHT-PQ evidence so design regressions
and rejected layouts remain auditable. It is not a current product claim.

## Environment and protocol

- Client: AWS `c7g.8xlarge`, 32 Arm vCPUs and 61 GiB RAM.
- Region: `eu-central-1`; index objects in Amazon S3 in the same region.
- Queries: 100 shipped public-dataset queries per selected profile.
- Recall: strict overlap recall@10 against shipped full-corpus neighbors.
- Quantizer: persisted SRHT/random-sign rotated learned product codes,
  asymmetric shortlist scoring, and exact float32 rerank. This is
  TurboQuant-inspired rotation, not the paper's unmodified scalar codec.
- Resource sampling: process CPU, RSS/VMS, physical process disk reads/writes,
  and local-cache footprint throughout every experiment.

The consolidated artifacts are
[`aws-production-profiles.csv`](../web/assets/benchmarks/aws-production-profiles.csv),
[`aws-production-repetitions-2026-07-20.csv`](../web/assets/benchmarks/aws-production-repetitions-2026-07-20.csv),
and
[`aws-recall-latency-2026-07-20.csv`](../web/assets/benchmarks/aws-recall-latency-2026-07-20.csv).

## Rejected v8 checkpoint-centroid qualification

The first fresh two-level coarse/product-PQ source rebuild used physical-segment
centroids as its global routing units. On Fashion-MNIST it used six cells and
the persisted `nprobe=6, candidates=184` point reached 0.976 recall@10. The
selected cache-state run measured 133.9 ms uncached p95 and 3.58 ms disk-cached
p95 with zero backing GETs in the latter phase. Source ingest took 23.6 s,
bounded global-PQ finalization took 17.0 s, and the whole build+sweep+serving
process peaked at 472.5 MiB RSS.

This is rejected evidence, not a production table row. The same layout later
reached only 0.623 recall on GloVe and 0.576 on Deep-Image at its defaults;
Deep-Image required nearly all 256 cells for 0.97 recall. That curve identified
a routing-design failure, not a default that should be widened. V8 now assigns
every vector to a globally trained cell through bounded disk-backed external
partitioning, and all datasets are being recreated from empty prefixes. The
first broad Fashion frontier files
also reused cache contents between successive configurations, so their recall
values are valid but their latency values are diagnostic only. The harness now
emits explicit `uncached` and `disk_cached` rows for every frontier point,
resetting the data cache before every uncached query and validating zero backing
GETs for disk-cached rows. Raw evidence is under
[`v8-global-coarse/fashion-mnist-784-r1`](../web/assets/benchmarks/raw/2026-07-21/v8-global-coarse/fashion-mnist-784-r1/)
and
[`v8-global-coarse/fashion-mnist-784-frontier-r2`](../web/assets/benchmarks/raw/2026-07-21/v8-global-coarse/fashion-mnist-784-frontier-r2/).

## Superseded pre-fixed-page v8 qualification

The first vector-level global-cell rebuild fixes the checkpoint-centroid recall
failure. On Fashion-MNIST, `8 probes / 256 candidates` reached 0.990 recall@10,
124.3 ms uncached p95, and 2.24 ms disk-cached p95 with zero backing GETs.
Increasing probes from 8 through 128 changed recall by only 0.001 while raising
uncached p95 as high as 754 ms, so the smaller point is the qualified frontier.

This r1 run is still diagnostic rather than promoted: it used the preceding
16,384-row high-dimensional physical segment and eight CPU workers, peaking at
473 MiB RSS and 819% sampled CPU. The production recreation uses 5,349-row
physical segments, 32 MiB ingest checkpoints, four CPU workers, and persisted
`8/320` defaults. Raw r1 evidence is under
[`v8-vector-ivf/fashion-mnist-784-r1`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/fashion-mnist-784-r1/).

The production-shaped r2 curve selected `8 probes / 320 candidates`: 0.989
recall, 202.0 ms uncached p95, and 2.43 ms disk-cached p95 with zero backing
GETs. Three subsequent fresh-prefix production repetitions, r4-r6, reproduced
0.989 recall. Their selected cache-state medians are 430 ms startup, 153.97 /
191.11 / 211.53 ms uncached p50/p95/p99, and 2.22 / 2.45 / 2.52 ms
disk-cached p50/p95/p99. Uncached work is 47.86 GETs and 4.48 MB per query.

The median whole build+sweep+serving peak is 229 MiB RSS and 427% CPU; the
worst RSS is 231 MiB and build scratch remains below 4 MiB. At 16 cached callers
the three runs sustain 590-616 QPS, with p95 28.5-31.4 ms and worst maximum
34.4 ms. The FIFO four-search admission cap has no multi-second starvation
tail. The consolidated evidence is
[`aws-v8-vector-ivf-production-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-v8-vector-ivf-production-repetitions-2026-07-21.csv);
raw r2-r6 runs are under
[`v8-vector-ivf`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/).

## Descriptor-v6 hierarchical fixed-page diagnostic

The first source-recreated Fashion-MNIST hierarchy retains the selected
`8 probes / 320 candidates` point at 0.988 recall, above the directly measured
Amazon S3 Vectors 0.985 target. Its uncached p95 is 178.2 ms with 48.9 GETs and
3.28 MB/query; the identical zero-backing-GET disk-cached pass is 1.40 ms p95.
The complete sweep reaches 0.991 at 12 probes, but 16–32 probes do not improve
recall, so they are dominated. Build plus the complete sweep peaked at 276 MiB
RSS and 177.5 MiB sampled scratch. This row remains diagnostic until fresh
reuse repetitions and bounded-concurrency measurements pass. Raw evidence:
[`fashion-mnist-784-r7-hierarchical`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/fashion-mnist-784-r7-hierarchical/).

The fresh descriptor-v8 packed recreation selects the same `8/320` default at
0.989 recall, again above the directly measured S3 Vectors 0.985 target. It
measures 186.4/1.68 ms uncached/disk-cached p95, 3.52/0.52 MB per query, and
33.38 uncached GETs. Recall reaches 0.991 at 12 probes and remains there through
32, while latency and bytes rise, so the wider points are rejected. Source
ingest took 26.1 s and bounded index finalization 19.9 s; the complete build and
curve peaked at 258.2 MiB RSS and 163.3 MiB scratch. Raw evidence:
[`fashion-mnist-784-r8-packed-adaptive`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/fashion-mnist-784-r8-packed-adaptive/).

Three clean-process Fashion production repetitions all return 0.989 recall.
Median uncached/disk-cached p95 is 197.9/1.667 ms, worst serving RSS is
75.0 MiB, and mean whole-run CPU is 22.5–25.1% of one core. With 16 cached
callers, the bounded profile sustains 2,427–2,462 QPS with p95 7.04–7.28 ms and
a 7.83 ms worst maximum.

The fresh descriptor-v7 adaptive GloVe rebuild restores the non-hierarchical
angular layout and reproduces the high-recall frontier. At `96 probes / 184
candidates` it reaches 0.985 recall with 495.1 ms uncached and 13.3 ms
zero-GET disk-cached p95; the sampled 100-query sweep reaches empirical 1.000
at `192 / 184`, with 759.1/25.8 ms p95 and 472.77 GETs/query. Build plus sweep
peaked at 350.1 MiB RSS and 523.1 MiB scratch. This qualifies recall and the RAM
envelope, but not uncached latency: packed code-slab experiments must reduce
the hundreds of small object requests before promotion. Raw evidence:
[`glove-100-r12-adaptive-flat256`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r12-adaptive-flat256/).

The corresponding fresh NYTimes adaptive rebuild reaches 0.958 at `192 / 320`
and 0.976 at `256 / 320`. Their uncached p95s are 701.8 and 947.0 ms, while
disk-cached p95s are 9.3 and 11.8 ms. Peak build-plus-sweep RSS is 262.0 MiB;
the row is a recall/layout diagnostic, not a latency-qualified production row.
Raw evidence:
[`nytimes-256-r2-adaptive-flat256`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r2-adaptive-flat256/).

Descriptor-v8 packed recreations keep those recall values and materially reduce
S3 request amplification. GloVe at 0.985 falls from 278.77 to 148.32 GETs/query
and from 495.1 to 365.4 ms uncached p95; its empirical-1.000 point falls from
472.77 to 178.36 GETs and from 759.1 to 448.5 ms. NYTimes at 0.958 falls from
435.11 to 75.50 GETs and from 701.8 to 312.5 ms; at 0.976 it falls from 563.46
to 78.46 GETs and from 947.0 to 394.8 ms. Whole build+sweep RSS is 359.8 MiB
for GloVe and 279.5 MiB for NYTimes. These are first-pass diagnostics pending
independent production-profile repetitions, not silently promoted defaults.

GloVe now also has three independent selected-point serving repetitions. All
report empirical 1.000 recall; uncached p95 is 450.6–472.1 ms, disk-cached p95
24.9–25.7 ms, and worst serving RSS 148.8 MiB. Under 16 cached callers, the
bounded four-search profile sustains 55.6–57.0 QPS with 293.4–301.9 ms p95 and
307.3 ms worst maximum. This validates stable bounded load behavior, but the
uncached row remains above the desired production latency and is not presented
as a final optimum.

For NYTimes, scanning all 256 cells proves that the 320-candidate ceiling is
not a routing miss: the 64-byte code rises to 0.990 at 512 candidates but
plateaus at 0.991 for 768 and 1,024. The 1,024 point costs 406.94 GETs and
486.9 ms uncached p95. A fresh 128-byte-code index reaches 0.993 at only 288
candidates, with 418.9/21.6 ms uncached/disk-cached p95, 83.15 GETs, and
258.0 MiB whole-run peak RSS. Candidate counts through 768 do not improve
recall and only increase p95/GETs, so the narrower 288-row rerank is the
measured frontier. The subsequent full probe sweep selects the production
point below; simply widening the exact shortlist is rejected.

The ten-query exact control returns 1.000 for both exact and ANN on the sampled
subset, confirming the shipped ground truth is compatible with BORSUK's metric.
It also exposes the guarantee cost: exact p95 is 8.66 s uncached and 5.19 s
disk-cached after reading/scoring 405.6 MB/query, versus 412/21.7 ms for ANN.
Only exact mode is a formal 1.0 guarantee; the complete ANN set remains 0.993.

The broad code128 probe curve reached 0.993 at 224 cells; probing all 256 did
not improve recall. A one-cell boundary sweep then found 223 as the true first
0.993 point: 220 reaches 0.985 and 221–222 reach 0.989. At 223, uncached and
disk-cached p95 are 376.9 and 19.6 ms. Lower points remain explicit tuning
choices, not the high-recall production default. Raw boundary evidence is in
[`nytimes-256-r15-boundary-probes`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/nytimes-256-r15-boundary-probes/).

Three clean-process production repetitions qualify the selected NYTimes point.
The first two used the adjacent 224-probe point and the third pins the final
223-probe default; all return 0.993 recall. Median uncached/disk-cached p95 is
368.9/19.7 ms, worst serving RSS is 118.9 MiB, and 16 cached callers sustain
95.9–96.7 QPS with p95 175.8–181.0 ms and a 187.2 ms worst maximum. The final
223-probe repetition alone measures 359.2/19.7 ms and 115.3 MiB peak RSS.

The explicit code256 control is dominated. It stays at 0.993 from 288 through
1,024 candidates even with all cells scanned, while its measured 224/288 point
doubles scan bytes to 71.7 MB and raises disk-cached p95 from 19.4 to 29.3 ms.
This rules out both a code256 default and an unsubstantiated two-stage
refinement claim for NYTimes; the persisted code-width knob remains available
so other datasets can publish the same controlled curve.

SIFT now has a fresh descriptor-v8 source recreation using the adaptive
full-dimensional hierarchy and packed code/exact ranges. The persisted default
`64 probes / 184 candidates` reaches 0.997 recall with 205.0/4.72 ms
uncached/disk-cached p95, 9.58/7.64 MB per query, and 74.51 GETs in the uncached
state. The 96-probe research point reaches 0.999 at 241.3/6.63 ms; 128 probes
stays at 0.999 while rising to 263.5/8.53 ms and is dominated. Source ingest
took 83.5 s and bounded index finalization 66.4 s. The complete build and
nine-point sweep peaked at 305.7 MiB RSS and 537.6 MiB scratch. Raw evidence:
[`sift-128-r3-packed-adaptive`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/sift-128-r3-packed-adaptive/).

Three clean-process SIFT production repetitions confirm the `64/184` default.
All return 0.997 recall; median uncached/disk-cached p95 is 213.8/4.58 ms,
worst serving RSS is 104.9 MiB, and mean whole-run CPU is 64.2–69.8% of one
core. Under 16 cached callers the bounded profile sustains 516.7–524.6 QPS,
with p95 34.6–35.8 ms and a 38.3 ms worst maximum.

Deep-Image now has a fresh descriptor-v8 9.99M-row hierarchical recreation.
The full-dimensional 64-by-64 router reaches 0.987 recall at `96 / 200` with
403.2/21.1 ms uncached/disk-cached p95 and 30.3/28.4 MB/query. The curve rises
to 0.994 at 192 probes, 0.998 at 384, and 0.999 at 512; their uncached p95s are
567.9, 889.0, and 1,073.8 ms. Build plus sweep peaks at 461.4 MiB RSS and
4.20 GiB scratch, with scratch released after publication. This dominates the
old product router at matched high recall and qualifies the hierarchy as the
large-angular adaptive layout, while making the final 0.001 recall cost
explicit. Raw evidence:
[`deep-image-96-r4-packed-hierarchical64`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/deep-image-96-r4-packed-hierarchical64/).

Three clean-process Deep production repetitions confirm the persisted
`128/200` default. All return 0.990 recall on the full 100-query set; median
uncached/disk-cached p95 is 508.2/26.0 ms, worst serving RSS is 271.9 MiB, and
mean whole-run CPU is 112.6–115.6% of one core. At 16 cached callers the
four-search global cap sustains 103.7–104.9 QPS, with p95 176.5–185.7 ms and a
194.3 ms worst maximum. The former multi-second overload tail does not recur.
Consolidated evidence:
[`Fashion, SIFT, and Deep repetitions`](../web/assets/benchmarks/aws-v8-packed-sift-deep-production-repetitions-2026-07-21.csv).

Holding all 512 cells fixed and widening exact candidates from 256 to 512 does
not change the 0.999 result. Uncached p95 instead rises from 1.10 to 1.24 s and
GETs rise from 344 to 393 per query. This rejects candidate widening as a way
to obtain the missing 0.001 and keeps the bounded `128/200` production default
separate from the high-recall research ceiling.

GIST now has a fresh descriptor-v8, one-million-row, 960D source recreation.
The original 128-byte product code exposed a fidelity problem: at 32 probes it
needed 608 exact candidates to reach only 0.985 recall, and widening routing to
128 probes plateaued at 0.986. The controlled 256-byte rebuild changes only the
approximate code fidelity. It adds 128.0 MB to an 8.37 GB index (1.5%), keeps
the whole build-and-sweep peak at 369.9 MiB RSS, and reaches 0.995 recall with
`24 probes / 96 candidates`. That selected point measures 388.0/29.3 ms
uncached/disk-cached p95, 65.8 MB/query, and 163.70 GETs/query in the isolated
candidate sweep. This is the GIST default; it is not extrapolated to narrower
corpora where wider codes were measured and rejected.

The full code256 routing curve at 768 candidates reaches
0.934/0.987/0.997/0.998/0.998/0.999 recall at 8/16/24/32/48/64 probes.
Disk-cached p95 rises from 15.8 ms at 8 probes to 31.5 ms at 24 and 60.4 ms at
64; uncached p95 rises from 588.6 to 895.1 ms between the endpoints. Holding
24 probes fixed, 64/96/128/192/384 candidates reaches
0.990/0.995/0.996/0.996/0.997. The 384-row research point costs 467.3 ms
uncached p95 and 376.70 GETs/query, so the final +0.002 empirical recall is not
free and is not the production default. The lower-latency `16/96` tuning point
is 0.985 recall, 303.8 ms uncached p95, 22.0 ms disk-cached p95, and 142.60
GETs/query.

Three clean-process selected-point repetitions reproduce 0.995 recall on all
100 queries. Median uncached/disk-cached p95 is 427.3/29.2 ms, worst serving
RSS is 304.3 MiB, and whole-run mean CPU is 247.7–260.7% of one core. With 16
cached callers behind the four-search FIFO cap, throughput is 59.2–59.6 QPS,
median p95 is 306.1 ms, and the worst maximum is 343.1 ms. There is no
multi-second overload tail. Offline source ingest takes 635.9 s and final
publication 322.0 s; sampled external scratch peaks at 3.80 GiB and returns to
zero. Raw curves and repetitions are under
[`gist-960-r9-code256` through `gist-960-r16-production-rep3`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/),
with consolidated selected-point rows in
[`aws-v8-packed-sift-deep-production-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-v8-packed-sift-deep-production-repetitions-2026-07-21.csv).

A final empty-prefix recreation verifies that these are real defaults rather
than benchmark flags. The synced release binary built r17 with
`global_pq_code_bytes=adaptive`; serving then left both `nprobe` and candidates
unset. The resulting 1,659-object, 8,499,228,140-byte artifact matches the
code256 layout, and its ten-query smoke pass returns 1.000 recall with
831.3/27.6 ms uncached/disk-cached p95. That small smoke set is only a persisted
configuration check—the publication recall and latency remain the three full
100-query repetitions above. R17 build RSS peaks at 328.1 MiB. Raw evidence:
[`gist-960-r17-production-default-rebuild`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/gist-960-r17-production-default-rebuild/).

As everywhere else, 0.995 is empirical recall on the complete public query
set—not a mathematical guarantee. Exact mode remains the formal 1.0 path and
uses the lossless pages; it may need to read and score the complete eligible
corpus.

The independently recreated product-layout controls rule out 2x64 Cartesian
routing as the fix. At 1,024 probes, GloVe reaches only 0.976 recall with
3,158.8 ms uncached p95 and 2,147 GETs/query; NYTimes reaches only 0.889 with
3,090.6 ms and 2,157 GETs/query. Peak RSS remains bounded at 335.0 and
256.6 MiB, respectively, so this is a quality/I/O rejection rather than a
memory rejection. The product layout is not a production row and is not a
defensible automatic large-corpus default without new evidence.

These Fashion-MNIST rows remain valid historical evidence for the earlier v8
physical-sidecar rerank layout. They are no longer a current production claim:
the fixed-width cell-aligned exact-page format must be recreated and repeated
on all six corpora before promotion. No latency, RAM, or GET result is carried
across that format boundary.

The first vector-level GloVe recreation is retained as a rejected layout
ablation. Applying the 2x64 / 4,096-cell scale topology at only 1.18M rows
over-fragmented the corpus: `16/88` reached 0.543 recall with 574 ms uncached
p95 and 115 GETs/query; `64/88` reached only 0.709 while rising to 1,030 ms and
211 GETs/query. The same points were 2.29 and 3.73 ms disk-cached, isolating
object fan-out rather than PQ compute as the problem. The sweep was stopped
after that falsification instead of spending more queries on a configuration
that could not meet the gate. Standard million-row corpora now use one
full-dimensional 256-centroid coarse quantizer; large normalized corpora use
the separately qualified full-dimensional hierarchy rather than product cells.
Raw partial evidence is under
[`v8-vector-ivf/glove-100-r1-rejected-4096-cells`](../web/assets/benchmarks/raw/2026-07-21/v8-vector-ivf/glove-100-r1-rejected-4096-cells/).

## Historical v7 global-PQ evidence

Version 7 replaced the old cell-routed serving path with a fully resident global
product-PQ scan followed by the then-current lossless row-range reranking. These rows are
retained as regression evidence only. They are not current production claims:
v8 pages routed global-PQ code cells, uses 32 MiB ingest checkpoints, caps build
workers at four, and caps resident exact-sidecar metadata at 128 MiB. A v8 row
is promoted only after a fresh source-data build and independent repetitions.

| Dataset | Global PQ (`subspaces / candidates / rerank-read cap / query cap`) | recall@10 | startup | uncached p50 / p95 / p99 | disk-cached p95 | GETs / bytes | clean-process peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| Fashion-MNIST | `64 / 184 / 64 / 4` | 0.972 | 0.59 s | 34.2 / 64.9 / 71.1 ms | 2.49 ms | 26.79 / 2.00 MB | 99.2 MiB worst |
| GloVe | `32 / 88 / 64 / 4` | 0.956 | 1.32 s | 48.6 / 93.5 / 101.3 ms | 7.08 ms | 54.71 / 6.82 MB | 262 MiB median; 303 MiB worst |
| SIFT | `32 / 88 / 64 / 4` | 0.976 | 1.07 s | 37.5 / 66.1 / 89.3 ms | 5.41 ms | 28.95 / 3.37 MB | 240 MiB worst |

These are medians of three independent 100-query serving processes. Fashion
uncached p95 was 67.8/64.8/64.9 ms, disk-cached p95 was 2.46/2.49/2.53 ms, and
all runs reported 0.972 recall. GloVe uncached p95 was 87.2/94.1/93.5 ms,
disk-cached p95 was 7.08/7.11/7.07 ms, and all runs reported 0.956 recall. At 16
callers with the production four-search admission cap, median p95 was 19.5 ms
for Fashion and 44.0 ms for GloVe; the worst maxima were 22.2 and 53.4 ms.
SIFT uncached p95 was 70.0/66.1/62.0 ms, disk-cached p95 was
5.43/5.41/5.41 ms, and every run reported 0.976 recall; its 16-caller median p95
was 31.7 ms with a 34.8 ms worst maximum.

The selected locality-sorted build used 670 MiB peak RSS, 124.8 s ingest, and
160.6 s global-PQ finalization; its global artifact is 40.3 MiB. The matched
full-reclustering ablation reached 71.6 ms uncached p95 and 23.86 GETs/query but
used 2.05 GiB build RSS and 433.8 s reclustering. The unsorted ingest-preserving
ablation used 563 MiB but regressed to 148.8 ms p95 and 77.23 GETs/query.
Locality-sorted ingest was therefore the v7 selected layout; full reclustering
remains an explicit historical research/layout comparison.

The source tables are
[`aws-global-pq-v7-glove-layouts-2026-07-21.csv`](../web/assets/benchmarks/aws-global-pq-v7-glove-layouts-2026-07-21.csv)
and
[`aws-global-pq-v7-glove-production-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-global-pq-v7-glove-production-repetitions-2026-07-21.csv).
The shared serving-repetition table is
[`aws-global-pq-v7-production-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-global-pq-v7-production-repetitions-2026-07-21.csv).
Every v7 row comes from a newly created empty object-store prefix populated from
the original dataset files. No old index is rebuilt or migrated in place.

## Historical v6 cell-routed selected profiles

These are historical three-run medians, with the worst serving RSS across those
runs. All profiles use query cap 4. The cross-dataset decode-cap
default is 24; GloVe uses its measured bounded cap-32 tuning point. Fashion and
GloVe use graph-free `pq-scan-only` indexes. Fashion and GIST use the
dimension-aware 512-row layout; GloVe, SIFT, and Deep-Image use 4096 rows.

| Dataset | Profile (`nprobe / candidates / width / decode cap / query cap`) | recall@10 | startup | uncached p95 | disk-cached p95 | uncached GETs / bytes | experiment peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| Fashion-MNIST | `6 / 11 / 6 / 24 / 4` | 0.989 | 510 ms | 88.7 ms | 11.9 ms | 12 / 3.52 MB | 193 MiB |
| GloVe | `80 / 16 / 32 / 32 / 4` | 0.951 | 819 ms | 302 ms | 38.4 ms | 160 / 55.1 MB | 727 MiB |
| SIFT | `16 / 32 / 16 / 24 / 4` | 0.969 | 801 ms | 104 ms | 13.5 ms | 32 / 12.2 MB | 438 MiB |
| NYTimes | `72 / 16 / 16 / 24 / 4` | 0.959 | 626 ms | 477 ms | 78.3 ms | 144 / 68.8 MB | 759 MiB |
| GIST | `144 / 16 / 24 / 24 / 4` | 0.967 | 5.97 s | 544 ms | 75.2 ms | 288 / 79.8 MB | 724 MiB |
| Deep-Image | `64 / 32 / 16 / 24 / 4` | 0.956 | 5.04 s | 368 ms | 45.7 ms | 128 / 45.8 MB | 712 MiB |

Every disk-cached row reported zero backing GETs/network bytes. Logical bytes
served from local disk may still be nonzero; object-cache miss counters were not
used to establish cache state.

## Independent repetitions

Every selected profile was repeated three times with a fresh local data cache
for each uncached phase. Uncached p95 ranges were Fashion 87.1–117.9 ms, GloVe
285.6–315.8 ms, SIFT 91.7–150.3 ms, NYTimes 465.2–524.8 ms, GIST 521.8–831.4
ms, and Deep-Image 359.5–584.8 ms. Disk-cached ranges were 11.6–12.5, 38.2–38.5,
13.5–14.6, 76.0–79.2, 69.8–76.9, and 45.6–46.1 ms respectively. No measured
query-phase p95 reached one second, much less four seconds.

The multi-second numbers are startup/open measurements, not query latency.
Median startup was 5.97 s for GIST and 5.04 s for Deep-Image; their slowest
individual opens were 8.81 and 8.55 s. Startup resolves and retains serving
metadata before the cache-state query phases begin.

The earlier cap-12/16 repetition campaign is retained rather than overwritten.
At those historical profiles, uncached p95 ranges were Fashion 168–182 ms, GloVe
286–475 ms, SIFT 144–145 ms, NYTimes 496–568 ms, GIST 849–904 ms, and
Deep-Image 581–621 ms. Disk-cached ranges were 63–66, 38–39, 14–15, 78–80,
183–189, and 46–47 ms respectively. Bytes and requests were identical between
passes. The GloVe spread is retained and rules out a narrow confidence claim.

The earlier GloVe and NYTimes angular rows were rejected because their legacy
indexes used raw shortlist geometry. The accepted indexes persist normalized
angular coarse geometry. The rebuild resource histories are retained as
`normalized-build-*` charts.

## Recall/latency frontiers

The recall sweep used memory-preloaded cells to isolate shortlist/rerank compute
from network variance. It is not the uncached production latency table.

| Dataset | Low point (`nprobe`, recall, p95) | Qualified point | High point |
|---|---|---|---|
| Fashion-MNIST | `1`, 0.862, 6.1 ms | `4`, 0.961, 19.9 ms | `32`, 1.000, 91.7 ms |
| GloVe | `16`, 0.834, 10.9 ms | `80`, 0.951, 51.0 ms | `128`, 0.978, 80.6 ms |
| SIFT | `1`, 0.446, 0.9 ms | `16`, 0.969, 11.7 ms | `64`, 1.000, 44.1 ms |
| NYTimes | `16`, 0.783, 18.4 ms | `72`, 0.959, 73.1 ms | `128`, 0.989, 117.6 ms |
| GIST, legacy 4096 rows | `8`, 0.793, 41.2 ms | `32`, 0.964, 153.2 ms | `128`, 0.999, 582.0 ms |
| Deep-Image | `32`, 0.921, 22.8 ms | `64`, 0.956, 44.8 ms | `512`, 0.997, 344.8 ms |

The curves demonstrate the expected recall/latency exchange: probing more cells
improves recall but adds scan, rerank, and I/O work. Per-dataset SVGs are in
[`recall-latency/`](../web/assets/benchmarks/recall-latency/).

## Historical v6 Deep-Image cache interpretation

The old 41.9/44.6/46.4 ms result had decoded vectors already pinned in memory
and was incorrectly called cold. The historical three-run median disk-cached
p50/p95/p99 is 44.6/45.7/46.1 ms, uncached p95 is 368 ms, and startup/metadata
preparation is 5.04 s. `memory_preloaded`, `disk_cached`, and `uncached` remain
separate.

## Historical v6 Fashion layout comparison

The v6 784D default used 512 rows/cell, `nprobe=6`, 11 candidates, width 6,
query cap 4, and global decode cap 24. The final graph-free index retained 0.989
recall; that engine's exact small-coarse-layer routing independently
reconfirmed 0.989 at the selected point. Across three independent v6-engine
read-only repetitions it measured median 88.7/11.9 ms uncached/disk-cached p95,
median 310.7 four-worker QPS, and at most 193.2 MiB serving RSS. The graph-free
S3 prefix is 240.4 MB and contains no
graph objects. See the complete
[configuration ablation](configuration-ablation.md).

## Historical v6 GIST layout comparison

The historically selected 512-row layout reaches 0.967 recall at `nprobe=144`, 16
candidates, and width/decode cap 24. Relative to the 4096-row tuning option it
cuts disk-cached p95 from 185.8 to 75.2 ms, peak serving RSS from 1,247 to 724
MiB, and bytes/query from 133.1 to 79.8 MB. Uncached p95 is unchanged at about
network scale because request count rises from 64 to 288; the historical three-run
median is 544 ms with a 522–831 ms range. Four-worker throughput falls from
19.5 to 17.3 QPS. The source is
[`aws-gist-cell-layout-2026-07-20.csv`](../web/assets/benchmarks/aws-gist-cell-layout-2026-07-20.csv).

This layout has a substantial offline cost: 19.2 minutes ingest, 25.6 minutes
compaction, and 31.2 GiB experiment peak RSS. The 4096-row layout therefore
remains a documented lower-request/higher-throughput v6 option. Neither is the
v8 production default.

## Resource graphs

Every standard dataset has CPU/RAM/disk/cache timelines under
[`resources/`](../web/assets/benchmarks/resources/):

- `current-engine-production-repetitions/*`: three current-engine runs for
  every cap-24 selected profile except separately tuned GloVe;
- `glove4096-production-repetitions/cap-32`: selected graph-free GloVe runs;
- `production-final-*`: earlier final-engine single-run profiles;
- `production-*`: earlier common-layout and repetition profiles;
- `production-repeat-*`: independent repetitions;
- `recall-*`: recall sweeps;
- `uncapped-*`: overload ceilings;
- `normalized-build-*`: corrected angular-index builds.
