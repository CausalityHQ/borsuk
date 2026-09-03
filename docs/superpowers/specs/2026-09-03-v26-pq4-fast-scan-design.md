# V26 PQ4 Fast-Scan Design

## Decision

Replace the rejected scalar global PQ16 serving candidate scan with a
128-bit fast-scan representation: 32 three-dimensional subquantizers, 16
centroids per subquantizer, and two four-bit codes per byte. Persist codes in
32-row transposed blocks so an AArch64 NEON query scores 32 rows with register
table lookups rather than random 256-entry scalar loads.

This is a new pre-release artifact family with no compatibility reader or
conversion path. Existing PQ16 artifacts remain immutable evidence only. The
new path must pass unchanged quality, latency, memory, page-budget, authority,
and no-page-read gates before it can replace anything else.

## Evidence and failure being addressed

The authenticated native PQ16 screen recovered 316 of 320 truth neighbors:
987,500 ppm aggregate recall, 900,000 ppm minimum-query recall, and 987,500 ppm
oracle attainment. It therefore needs three additional hits to reach the
995,000-ppm oracle gate. Its stage-timed rerun measured global ADC at
15.84/16.64/17.41 ms p50/p95/maximum and exact rerank at
3.13/3.23/3.24 ms. The scalar ADC scan alone exceeds the 15-ms total budget.
Increasing its exact-rerank depth cannot fix latency.

Page centroids, random-projection trees, and 2/4/8/16/32/64 per-page modes have
already failed the at-most-256-page concentration boundary. Fast scan instead
keeps a global row-code path but changes both fidelity allocation and CPU
execution: twice as many subspaces reduce cross-dimension quantization error,
while four-bit tables fit SIMD register lookup instructions.

## Representation and exact memory bound

Each code block represents 32 source-order rows. For each of 32 subquantizers,
16 bytes store row `2*i` in the low nibble and row `2*i+1` in the high nibble.
One block is therefore `32 * 16 = 512` bytes, exactly 16 bytes per row. The
last block is zero-padded, and `row_count` is authoritative; padded rows are
never ranked.

The codebook is `32 * 16 * 3 * 4 = 6,144` bytes. The 100-million-row serving
projection charges:

- packed transposed codes: 1,600,000,000 bytes;
- reusable `u16` query-score buffer: 200,000,000 bytes;
- bounded cold-vector cache: 536,870,912 bytes;
- codebook: 6,144 bytes;
- 8,192-entry `u32` histogram: 32,768 bytes;
- top-4,096 `(score, ordinal)` scratch: 65,536 bytes;
- fixed query/table/accounting scratch: 384 bytes.

The exact projection is 2,336,975,744 bytes, 884,249,728 bytes below 3 GiB.
No postings or corpus-sized row-ordinal array is resident; source ordinal is
implicit in block order.

Persist two strict Arrow IPC files: `pq4-fast-codebook.arrow` and
`pq4-fast-codes.arrow`. Fields are concrete and non-nullable. The manifest
binds source construction and assignment Parquets, layout terminal, both
outputs, row count, dimension 96, 32 subquantizers, 16 centroids, block size
32, code bytes 16, byte order, nibble order, padding count, and the exact
projection by SHA-256, URI, length, role, and generation.

## Training and encoding

Training is query-independent. Use the existing deterministic stratified
8,192-row construction sample and four Lloyd iterations, independently for
each three-dimensional subspace and 16 centroids. Ties choose the lower
centroid. Encoding streams every authenticated construction row once, validates
source order and finite nonzero vectors, emits its 32 nibbles into the
transposed block, and never opens query, truth, or page-body data.

Scalar reference tests cover training, encoding, padding, nibble orientation,
ties, and reversed block traversal. The scientific builder must reproduce the
same SHA-256 outputs from the same inputs.

## Query algorithm

For each query, compute 32 floating-point 16-entry distance tables. Subtract
each table's minimum, then use one query-global scale mapping the largest
residual to 255. Quantize round-to-nearest with deterministic saturation. The
per-table minima sum is row-constant and does not affect ordering. Record the
scale, saturation count, and maximum scalar-versus-quantized distance error.

On AArch64, load each 16-byte code plane, split low and high nibbles, use
`vqtbl1q_u8` against the corresponding 16-byte lookup table, widen, and add
into separate 16-lane `u16` accumulators. Thirty-two tables have a maximum
score of 8,160, so accumulation cannot overflow. Scientific execution accepts
only the explicit `aarch64-neon-table` backend. A scalar implementation is a
test oracle, not a scientific fallback.

Write every row score into the reusable `u16` buffer while incrementing the
8,192-bin histogram. Determine the top-K score threshold by cumulative count,
then scan the score buffer in source order to emit all lower scores and the
needed threshold ties. Sort only the bounded result by `(score,
source_ordinal)`. This eliminates per-block and reduction heaps.

Read and exactly rerank the selected source rows from authenticated Arrow cold
vectors, using `(squared_l2, source_ordinal)` ordering. Fetch their two page
assignments and apply the existing deterministic ten-page reducer. Page bodies
are never opened by the falsifier.

## Fail-fast evaluation and leakage control

The already-burned first 32 external queries form a development-only frontier.
One scan retains top 4,096 candidates; exact rerank evaluates fixed prefix
depths 512, 1,024, 2,048, and 4,096. The diagnostic reports quality for every
depth but makes no serving-latency claim because the shared scan/read is not a
single-arm serving execution. The smallest passing depth is selected only when
it reaches aggregate recall 975,000 ppm, minimum-query recall 800,000 ppm, and
oracle attainment 995,000 ppm with exactly ten pages.

Only that fixed depth advances to a fresh-process 32-query latency screen with
a 15,000,000-ns maximum gate. If it passes, queries 32 through 511 are opened
once as a sealed holdout. No parameter changes are allowed after holdout
results. Holdout failure rejects the architecture. Repeated development-query
measurements are never described as release quality.

## Verification and execution policy

Every edit first runs a seconds-long exact selector, then the default
`python3 scripts/check_v26_fast.py` smoke gate. The broader
`--affected` gate runs once per stable slice. Strict workspace Clippy and the
locked full workspace suite run only after both development quality and
latency pass; the sealed holdout and final release suite run once.

Scientific work uses one `causality` Spot instance in any available
eu-central-1 zone, SSM, exact immutable S3 identities, zero swap growth, memory
PSI full avg10 at most 1%, and immediate termination after the original
terminal. Typed samples are Parquet and summaries are canonical newline JSON.
Every result is claim-ineligible until the sealed holdout and release gates
pass. D3 and competitor claims remain fenced throughout this falsifier.
