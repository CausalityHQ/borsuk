# V8 bounded-memory global PQ design

## Goal

Make the default TurboQuant `pq-scan` path production-safe on the 9.99M-row
Deep-Image corpus without trading away the measured recall target. Both index
construction and serving must have bounded transient memory, and the manifest's
RAM budget must account for the structures that are actually made resident.

## Root causes

The v7 global-PQ object is one Parquet binary value. Building it materializes
the resident arrays, a second contiguous payload, an Arrow copy, and Parquet
compression buffers at the same time. Loading performs the inverse copies. The
resident value also retains record IDs, ID offsets, and generations even though
the ADC scan only needs product codes and row locations. Deep-Image therefore
peaks around 4.4--4.5 GiB while building and around 2.7 GiB while opening.

The v7 candidate default depends on dimensions and subspaces but not corpus
size. Deep-Image therefore receives the same 88-row shortlist as GloVe despite
having 8.4 times as many vectors. Diagnostic sweeps show that 100 candidates
restore the former recall and 104 provides reproducible headroom.

## Selected format

Format v8 replaces the monolithic Parquet value with a small descriptor and
content-addressed raw chunks. Each chunk contains a fixed header, contiguous PQ
codes, and packed row locations. The implemented chunk cap is 32 MiB and is written as
soon as it fills, so build memory no longer grows with the corpus. Startup reads
only the descriptor/codebook. Product-code chunks remain paged; each query reads
only IVF-selected cells and releases their buffers afterward.

Locations use a descriptor-wide row-bit count and the smallest safe width. The
normal layout packs `(segment ordinal, row ordinal)` into `u32`; `u64` remains a
correctness fallback for unusually large explicit layouts. The global artifact
does not contain IDs or generations.

Vector sidecar rows become self-contained exact records containing generation,
record-ID length and bytes, and the lossless dense vector. Their tail table uses
32-bit offset/length pairs when the sidecar fits below 4 GiB. Global-PQ reranking
therefore retrieves ID, generation, and vector in the same bounded range read;
normal full-segment reconstruction simply ignores the added metadata.

## Defaults and accounting

The persisted shortlist is corpus-size aware. Large angular corpora use at least
`3 * subspaces + 8` candidates (104 for Deep-Image's 32 subspaces); higher-
dimensional angular corpora retain the wider `5 * subspaces` rule.

`GlobalPqRef` records exact descriptor/codebook bytes and the 128 MiB bounded
exact-sidecar-index cache envelope. Manifest RAM enforcement includes both.
Superseded implementation note: physical segments now target 16 MiB of decoded
float32 vectors (512–16,384 rows), while global routing cells are independent. The automatic
probe count grows with `2 × sqrt(cells)` but caps at 256, and build worker pools
cap at eight. A 100M-row index therefore retains metadata rather than all codes
and scans only a bounded routed subset. Query and request concurrency remain
globally bounded, so transient memory cannot multiply without limit.

## Compatibility and validation

There is intentionally no v7 reader or migration. Existing indexes must be
recreated. Tests cover chunk flushing, corruption checks, packed-location
boundaries, exact sidecar record round trips, compact tables, budget rejection,
and size-aware candidates. Publication numbers require fresh v8 AWS builds and
must include build/open/query CPU, RSS, disk/cache, latency, recall, bytes, and
request counts.
