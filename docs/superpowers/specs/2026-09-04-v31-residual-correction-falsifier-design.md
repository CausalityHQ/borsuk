# V31 Residual-Correction Falsifier Design

## Decision

Before changing the persistent index, V31 runs one six-arm evaluator-only
falsifier over the authenticated 100K V27 page artifacts. It tests whether the
remaining variable-rate PQ miss can be repaired by corpus-only per-row
reconstruction information. The devbox downloads no corpus; one disposable
`causality` Spot worker streams the registered 46,761,076 bytes of immutable
Arrow pages, evaluates the frozen arms in memory, writes canonical JSON plus
Parquet evidence to S3, and terminates.

## Causal premise

The reproduced 24/48-byte PQ8 route reached 319/320 hits at ten pages. Exact
distances over identical bounded candidates reached 320/320, and the missing
row was already at candidate rank 82. Containment is therefore sufficient and
compressed row ordering is causal. Page means are not a substitute: the later
16-page centroid route regressed to 273/320.

For query residual `q`, PQ reconstruction `x_hat`, and residual error
`e = x - x_hat`, let `v = q - x_hat`. Then:

`d_true = d_ADC - 2 * dot(v, e) + squared_norm(e)`.

The untested information is the residual norm and direction. V31 evaluates it
at row granularity before reducing rows to pages.

## Frozen ladder

All arms use the same query-independent base-code page layout, leaf beam 64,
12,288-candidate bound, ten-page limit, exact page rerank, queries, and truth.

1. `none`: the reproduction control; it must equal 319/320.
2. `u8-error`: subtract a per-leaf nearest-u8 estimate of squared error.
3. `sign8`: use the u8 error estimate plus eight residual-direction sign bits.
4. `sign16`: use the u8 estimate plus sixteen sign bits.
5. `exact-error`: subtract exact f32 squared error; diagnostic ceiling for the
   norm-only family.
6. `exact-cross-term`: use the exact residual cross-term; it must equal the
   320/320 exact-distance control.

For sign arms, fixed Gaussian projections are generated once from
`SHA-256(pages_manifest_sha256 || "borsuk-v31-residual-sketch-v1")`. The
diagnostic records the seed. A future production writer stores the projection
matrix as authenticated Arrow rather than depending on cross-language RNG
reproduction. Scan admission uses the u8 norm correction; the sign correction
is applied only to at most 12,288 retained candidates. Ties are
`(corrected_distance, source_ordinal)` before unique-page reduction.

## Authority, formats, and leakage

The worker reuses the existing strict page-manifest JSON, leaf-posting
Parquet, leaf-centroid Arrow, query Parquet, and authenticated Arrow page
loaders. Every byte length and SHA-256 is registered explicitly. Raw per-query
arm evidence is non-null typed Parquet; the final result is sorted compact JSON
plus LF and `claim_eligible=false`.

Construction/evaluation code receives no D3 or prior-result capability. The
projection seed, six arms, correction formulas, arm order, and smallest-pass
rule are committed before metrics are observed. A burned-fixture pass chooses
a mechanism only; it cannot support a product or competitor claim.

## Memory and compute projection

The frozen variable-rate baseline projects 2,630,588,896 resident bytes at
100 million rows. A production u8 norm plane adds 100,000,000 bytes. A
sixteen-bit sign plane adds 200,000,000 bytes; projection tables and framing
bring the projected `sign16` total to at most 2,933,872,864 bytes, leaving
287,352,608 bytes below 3 GiB. The sign8 arm needs one byte of sign data per
row and has larger headroom. Runtime peak RSS remains authoritative and
requires mapped/zero-copy buffers at scale.

Norm correction adds one byte load, lookup, and subtraction for at most one
million scanned rows. Direction correction evaluates only the retained 12,288
rows. It does not change S3 GET count or page bytes.

## Gates and interpretation

The run is void if `none` differs from 319/320 or `exact-cross-term` differs
from 320/320. A shippable arm passes only at 320/320 hits, 32/32 perfect
queries, minimum recall 1,000,000 ppm, exactly ten pages, no more than
4,587,520 page bytes, no more than 1,000,000 scanned codes, and no authority or
resource failure. The smallest passing arm freezes.

If u8 fails but exact-error passes, only a preregistered u16 norm diagnostic may
follow. If sign16 fails while exact-cross-term passes, one preregistered sign32
diagnostic may follow; failure closes this resident-correction family. No
post-result seed, shrinkage, page-count, or sketch-width tuning is allowed.

Only a passing mechanism proceeds to persistent Arrow TDD and the disjoint
9.99M confirmation. Physical coalescing remains a later latency-only change.
D3, 100M, competitor claims, local corpus download, and paid scale work remain
fenced.
