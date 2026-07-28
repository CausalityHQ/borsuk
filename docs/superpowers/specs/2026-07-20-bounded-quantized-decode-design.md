# Bounded quantized decode design

## Problem

The Fashion-MNIST recall-matched profile (`nprobe=22`, candidates=12,
recall@10=0.986) reads 95.4 MB/query from an index built with
`segment_max_vectors=4096`. The index has roughly 23 searchable cells, so the
query scans almost the complete corpus. `width=22` and the process query cap of
four allow 88 full cell decodes at once. The four-user experiment therefore
peaked at 2.63 GiB RSS even though its disk cache was only 112 MiB.

## Acceptance gates

- Use the same 60k Fashion-MNIST corpus, 100 queries, and shipped ground truth
  as the direct S3 Vectors experiment.
- Strict recall@10 must be at least 0.985.
- Startup, uncached, disk-cached, and memory-preloaded remain separate states.
- Every disk-cached row must report zero backing GETs and zero backing bytes.
- Report p50/p95/p99, bytes, GETs, QPS, CPU, peak RSS, disk I/O, and cache size.
- Target no more than 768 MiB peak RSS for one admitted query and 1.5 GiB for
  four callers on the c7g.8xlarge experiment host.
- Do not accept a memory improvement that silently preloads decoded vectors or
  lowers recall below the S3 Vectors result.

## Approach

### 1. Layout ablation

Build otherwise identical TurboQuant-4b indexes with 512- and 1024-row target
cells. Sweep `nprobe` and candidate budget to locate the lowest profile meeting
recall 0.985. Smaller cells should reduce bytes and transient decode state per
unit of useful recall. Select by the recall/uncached-latency/RSS envelope, not
cell count alone.

### 2. Compatible process-wide decode bound

Keep per-query width as an I/O-wave latency knob, but add a process-wide bound
around active projected cell decodes. Query admission alone is insufficient
because it multiplies by per-query width. The process-wide gate must be shared
by cloned index handles and released as soon as a cell has been scored and
compacted. Reuse workers rather than creating fresh scoped OS threads for every
query wave.

The initial compatible implementation uses a global cell permit count because
segment sizes are already bounded by the selected layout. If the 512/1024-row
ablation still produces materially unequal cell memory, replace the count with
a byte-weighted permit derived from `SegmentSummary::size_bytes`.

### 3. Format optimization only if required

TurboQuant's logical 4-bit levels are currently stored in an Arrow `UInt8`
code column. A versioned packed-nibble representation can halve persistent code
bytes, while direct scoring over Arrow buffers can remove `Vec<Vec<u8>>`
materialization. This changes the storage format and requires rebuild and
backward-compatible decoding, so it is not part of the first compatible fix.

## Correctness and failure behavior

Decode permits are RAII guards: success, storage error, checksum error, and
worker panic all release capacity. The gate must never change candidate order,
recall, bytes selected, or exact rerank results. A configured zero limit is
rejected. Extra callers wait at the gate rather than allocating decode state.

## Publication treatment

Retain the 2.63 GiB result as the diagnosed baseline. Report the layout ablation
and engine bound separately so the paper shows which gain comes from cell size
and which comes from multi-user admission. Do not label a width-22 result a
production default until it clears the RSS gates above.
