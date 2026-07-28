# SIMD query-kernel evaluation

Status: implemented and scalar-equivalence tested for production dense,
low-precision decode, packed-binary storage, named-sparse, BM25,
late-interaction MaxSim, routing, and Fast-TurboQuant scan arithmetic.
End-to-end AWS measurements remain behind the fresh-index benchmark gate.

## Coverage

| Query path | Vectorized work | Necessarily scalar work |
|---|---|---|
| Dense Euclidean / squared Euclidean | 8-lane `f32` differences, squares, and reduction | final square root |
| Dense inner product / cosine / angular | 8-lane `f32` dot products and norms | final division, clamp, `acos` |
| Float16 exact vectors | architecture-dispatched bulk conversion from `f16` to `f32`, followed by the 8-lane dense kernels | non-vector-width tail on targets without a native half conversion |
| BFloat16 / Int8 exact vectors | 32-value bounded conversion blocks followed by the 8-lane dense kernels | conversion and non-vector-width tail |
| E4M3FN / E5M2 exact vectors | 32-value decode blocks through exhaustive 256-entry format tables, followed by the 8-lane dense kernels | table address lookup and non-vector-width tail |
| Packed binary exact vectors | one bit per dimension in Arrow `FixedSizeBinary`; decoded bulk scoring uses 8-lane masks and population counts | selected-row unpack and non-vector-width tail |
| Dense Manhattan / Gower / Chebyshev / Bray-Curtis / correlation | 8-lane `f32` bulk reductions | scalar tail and final transform |
| Optional histogram/distribution metrics | 8-lane `f32` Canberra, Minkowski, Hellinger, chi-square, Bhattacharyya, Ruzicka, squared-chord, wave-hedges, Jensen-Shannon/KL, Lorentzian, and Clark arithmetic | input validation, scalar tail, and final transform |
| Named sparse inverted scan | 8 posting values multiplied by a broadcast query weight | row-id scatter and touched-row bookkeeping |
| Sparse–dense and sparse–sparse primitives | 8 gathered products per batch | index merge/gather and scalar tail |
| Late-interaction MaxSim | each document/query token dot product uses the shared 8-lane `f32` kernel | maximum selection across document tokens and query-token sum |
| BM25 persisted Parquet runs | 4-lane `f64` TF/document-length normalization and IDF scaling | document-length gather and row-score scatter |
| BM25 WAL/live-corpus fallback | same 4-lane `f64` scoring kernel | row gather/scatter and MVCC visibility |
| SRHT/FWHT rotation | 8-lane sign multiplication and every butterfly stage whose half-span is at least 8 | first three small-span butterfly stages |
| PQ build/query | 8-lane centroid distances, centroid accumulation/bounds, code distance, and ADC reduction | centroid/code lookup |
| Fast-TurboQuant scan | 8-lane normalization, SRHT, residual/norm, QJL-sign, and query/centroid arithmetic | packed-bit decode and centroid lookup |
| Build/routing geometry | 8-lane weighted centroid accumulation, vector bounds, signed-log PQ bounds/codes, locality projections, and routing lower bounds | code conversion, hashing, sorting, heaps, and graph adjacency |

SIMD is applied only to contiguous arithmetic. Posting row ids and quantizer
codes require gathers, while result rows require scatters. The safe portable
implementation gathers into fixed-size lane arrays, computes in SIMD, and
performs bounded scalar scatters. Claiming those irregular operations as SIMD
would be misleading.

PQ ADC and packed TurboQuant still decode or gather each code-dependent table
address scalarly, then reduce the gathered values with SIMD. Portable safe SIMD
does not provide a generally efficient scatter, and source-level gather support
varies by target.

Wasserstein remains scalar because it is a prefix sum; dynamic time warping
remains scalar because every DP cell depends on the previous row and adjacent
cell. SIMD versions are not promoted unless measurements beat those scalar
dependency chains on both ARM64 and x86-64.

## Correctness gate

Every SIMD kernel has a deterministic scalar-reference test covering:

- a bulk length larger than one vector;
- a non-multiple tail;
- multiple packed bit widths where applicable;
- multiple BM25 terms and repeated row accumulation;
- positive and negative sparse weights.

Floating-point reductions may use a different addition order. Tests therefore
use a tight relative tolerance; exact result ordering is independently covered
by dense, sparse, BM25, hybrid, and TurboQuant search tests.

The 2026-07-26 Apple Silicon release micro-gate measured the new decode paths
against their scalar references over 1,536-value buffers and 20,000
iterations. Float16 bulk conversion took 24.92 ms versus 30.54 ms for the
scalar control (1.23x); blocked E4M3FN lookup took 18.13 ms versus 94.45 ms
(5.21x). These are local kernel measurements, not end-to-end publication
results. The command was:

```bash
cargo test --release -p borsuk --lib microbenchmark -j2 \
  -- --ignored --nocapture
```

## Benchmark protocol

The publication run must compare SIMD enabled versus an explicitly compiled
scalar control, not versus historical results. For every modality and dataset,
record:

- p50, p90, p95, p99, maximum, mean, and standard deviation;
- queries per second at 1, 2, 4, 8, and 16 clients;
- recall and exact-result agreement;
- process CPU time, utilization, peak and timeline RSS;
- backing-store bytes/requests and disk-cache bytes/requests;
- cached, uncached, and mixed-cache traffic at 0%, 10%, 25%, 50%, 75%, 90%,
  and 100% coverage.

Report the machine architecture and enabled target features. A source-level SIMD
implementation is not evidence of a speedup: promotion requires lower
end-to-end CPU/query or latency without recall regression on both Graviton and
x86 AWS controls. All results must use indexes recreated from the exact source
revision under test.
