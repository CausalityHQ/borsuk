# TurboQuant, mixed-tier, and object-locality design

**Date:** 2026-07-22
**Status:** approved by direct user instruction; breaking changes are intentional

## Objective

Reduce the 100M remote p95 from the current 910 ms baseline without weakening
recall, evaluate the complete TurboQuant inner-product construction, and make
`auto` execute a true per-cell mixture: graph traversal for locally covered
cells and the configured packed blob scan for uncovered cells.

The publication matrix must compare the methods at matched recall and record
latency, throughput, CPU, RSS, process I/O, cache occupancy, scratch space,
object-store requests, transferred bytes, index footprint, and build time.

## Fresh-build public model

The implementation removes ambiguous names without adding a compatibility or
migration layer. Every changed layout or codec is rebuilt from the raw dataset
with `reuse_index=false`. Historical indexes are evidence artifacts only and
are never opened by the changed engine.

- `pq-scan`: learned classical product quantization with no rotation.
- `srht-pq-scan`: learned PQ after a seeded structured Hadamard rotation.
- `fast-turboquant-mse-scan`: data-oblivious Fast-TurboQuant MSE scalar codes,
  stored vector norm, and no residual stage.
- `fast-turboquant-scan`: full two-stage TurboQuant inner-product estimator.
  Stage one uses `b-1` scalar bits; stage two stores one residual-sign bit per
  padded coordinate and the residual norm. The scalable implementation uses an
  independent seeded structured projection and is explicitly reported as the
  Fast-TurboQuant structured implementation.
The structured implementation is validated for signed bias, variance, recall,
and latency before full-corpus or 100M promotion. Results do not describe its
structured residual projection as an i.i.d. Gaussian matrix.

## Object-store locality

The rejected hierarchical encoding placed `parent` in the low byte and `child`
in the high byte. Numeric spool order consequently alternated parents for each
child. Nearest children of a semantic parent landed in unrelated bundles,
turning 64 probes into approximately 64 remote range reads.

The current layout stores `parent` in the high byte and local child in the low byte. Numeric
order becomes parent-contiguous. Bundles may not cross a parent boundary unless
an individual parent exceeds the bundle byte cap. Experiments always recreate
the descriptor and bundle set together.

The read planner uses a byte-cost model rather than only a fixed gap:

```text
cost = request_weight * physical_GETs + transferred_bytes
```

For each parent-local bundle it chooses between selected code slices and one
contiguous parent span. The decision, predicted GETs, predicted bytes, actual
GETs, and actual bytes are emitted in the query report. Read and decode waves
remain bounded by process-wide gates.

Exact reranking remains lossless. A later experiment may add a contiguous
intermediate rerank code, but it cannot replace final lossless scoring and is
not promoted without an ablation proving its benefit.

## Full TurboQuant_prod

For normalized database vector `x`, stage one produces the MSE reconstruction
`x_hat` using `b-1` scalar bits. Let `r = x - x_hat`, `gamma = ||r||`, and let
`S` be the independent residual projection. The persisted code contains:

```text
scalar_indices | sign(S r) | original_norm | residual_norm
```

For query `q`, the score estimates:

```text
<q, x_hat> + gamma * sqrt(pi/2) / m * sum_i (S_i q) * sign(S_i r)
```

where `m` is the number of residual projections. The full structured profile
uses one projection per padded coordinate. Smaller `m` values are separately
labeled ablations, never “full TurboQuant”. Cosine, dot-product, and Euclidean
distance conversions have independent deterministic tests. Zero vectors,
padding, odd dimensions, corrupt payloads, and descriptor-version mismatch are
explicit error cases.

## Mixed cache execution

Execution is selected per routed global cell, not once for the whole index.

1. Route the query using resident global coarse metadata.
2. Partition selected cells by the active manifest/checksum coverage map.
3. Traverse a shared immutable graph for covered decoded cells.
4. Scan packed `srht-pq-scan` blobs for uncovered cells.
5. Merge both candidate streams with deterministic global ordering.
6. Perform one bounded lossless rerank and late ID materialization.

`auto` defaults its remote codec to `srht-pq-scan` until another codec passes
all promotion gates. Explicit `graph` is also mixed and falls back per cell; it
never fails merely because coverage is incomplete. A query pins the manifest
version and coverage snapshot so eviction or publication cannot change engines
mid-query.

Global-cell graph bundles are independently checksummed and keyed by the
global-cell identity. Decoded graphs use a byte-accounted process-wide LRU.
Same-checksum reads and decodes are single-flight across callers. The request,
decode, graph-byte, and search admission limits are global, not per query.

The report includes selected cells and rows by engine, decoded/disk/backing
fractions, graph and scan candidate counts, shared-read waits, fallback reason,
and the ordinary resource/I/O counters.

## Experimental sequence

1. Preserve completed earlier results only as explicitly labeled historical
   baselines; never reuse their indexes after a layout or codec change.
2. Prove parent-contiguous packing locally and measure GET/byte predictions on
   the existing 100M descriptor shape.
3. Validate TurboQuant-MSE and structured TurboQuant on deterministic vectors
   for bias, MSE, and top-k recall.
4. Run Fashion, GloVe, GIST, and Deep-Image qualification sweeps for classical
   PQ, SRHT-PQ, TurboQuant-MSE, and TurboQuant_prod.
5. Run cache mixes of 0/25/50/75/100% observed local cell coverage, direct
   NVMe controls, bounded multi-user loads, and an uncapped research ceiling.
6. Repeat qualified points three times on all six public corpora.
7. Run clustered, uniform, and adversarial synthetic controls and finally a
   qualified 100M candidate.
8. Compare equivalent configurations with FAISS/DiskANN/TurboVec and published
   managed-service measurements where identical execution is unavailable.

## Promotion gates

A production default must retain recall within 0.001 of its matched control,
improve or tie p95 and p99 in every selected repetition, remain within the RAM
and CPU envelopes, issue zero backing GETs for fully covered cells, avoid
multi-second bounded-concurrency tails, and have complete build/footprint/cost
evidence. The 100M remote target is p95 <= 500 ms at recall@10 >= 0.99; the
stretch target is <= 300 ms. Until all gates pass, the default remains
`srht-pq-scan + scan`.
