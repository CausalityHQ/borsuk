# Page-Centered Group Codes: 100k Falsifier Design

## Decision to answer

The sealed residual PQ48+PQ24 cell passed mean GT100 containment (97.554%) but
failed p05 (87% versus 90%). Exact source-vector scores over the same retained
leaves reached 98.556% mean and 91% p05. Thirty-five of the 68 residual
queries below 90 hits can be rescued by exact scoring; at least 19 need rescue
to clear the frozen p05 gate. The responsible 100k layer is row-score
approximation, while the 1M risk is scattered code reads. The next cell asks
whether **page-centered source residual coding**, read through fixed four-page
groups, closes enough of that gap without adding resident page centroids.

This is a single pre-release falsifier, not a selected production format.
The user authorized iterative measured redesign; no compatibility with the
failed code plane is required.

## Alternatives considered

1. Add another global residual PQ stage. The two-stage result recovered mean
   but left p05 unchanged, so another width is an unmotivated tuning step.
2. Rotate and scalar-quantize every coordinate with exact source norms. This
   changes representation family, but V103's prior exact-source-norm correction
   made PQ48 ranking decisively worse on a different cohort. That result does
   not falsify scalar codes, but it weakens this as the first bet.
3. **Chosen:** subtract each source-only page centroid before fitting one
   shared PQ48x8 book. This changes the distribution being quantized while
   preserving a compact 48-byte row code. Fetch the page centroids with the
   corresponding code groups, so they need not be resident. The quality gain
   is unproven and will be measured once without a width or seed sweep.

## Frozen source and access boundary

Use the authenticated ReLAION 100k source, the frozen two-means 480-KiB
membership and geometric tree, the same 1,000 development queries and GT100,
the same 32-page/16-MiB final data limit, and the same seed 20260921. Build
page means from source vectors and fixed membership. Form source residuals
`x - mean(page(x))`, train exactly one global PQ48x8 book on all 100,000
residual rows with ten iterations and the existing deterministic PQ trainer,
then encode each residual. Construction must finish and seal its artifacts
before the worker can download queries or truth. No query, truth, or prior
per-query outcome can choose the centroids, books, groups, or row codes.

## Code format and bounded read

The physical code object has one immutable range per consecutive four-page
group. Each range contains the float32 centroids and row counts of its one to
four pages, followed by the page-ordered 48-byte codes for those pages. A
separate canonical manifest binds the source/membership/tree identities,
PQ books, group offsets and lengths, per-group SHA-256, centroid bytes,
physical source order, format version, and complete code-object SHA-256.
The reader uses one real S3 Range GET for every selected group, validates the
returned length and SHA-256, and records successful GET/byte counters. Disable
SDK retries in the measurement reader; a failed GET fails the attempt.

The existing geometric tree still returns its deterministic 128-leaf order.
Map each leaf to `page_ordinal // 4`; take the first 32 distinct groups in
that order. Read *all* pages and rows in those groups, with no silent partial
group or fallback. Keep the same top-100-row nomination rule and stable source
ordinal tie break; select at most 32 final pages and 16,777,216 encoded data
bytes. For 632 rows/page, 32 four-page groups contain at most
`32 * 4 * 632 * 48 = 3,883,008` row-code bytes plus at most 393,216 centroid
bytes. Headers and the manifest are charged separately and the actual code
wave must remain below 32 GETs and 16,777,216 bytes. Fail construction if a
page or group violates its declared maximum.

Score each fetched row against its reconstructed vector
`mean(page) + PQ48(residual_code)` using squared L2. Accumulate in float64,
cast once to float32 before sorting, and independently replay by direct
reconstruction. A distinct exact-distance arm scores source vectors over
*precisely the same selected groups*. That arm isolates a grouping or tree
loss from quantization loss. The old PQ48 and residual PQ48+PQ24 results remain
paired controls on the same frozen queries, not retrained competitors.

## Decision and scale path

The preregistered quality gates are mean GT100 at least 97.5%, p05 GT100 at
least 90%, and GT10 page containment at least 96%, on all 1,000 frozen queries.
The code wave must use at most 32 actual GETs and 16 MiB; the planned final
data wave at most 32 pages and 16 MiB. Record every query, independent replay,
source and artifact digests, successful read counters, and process resource
peaks. If the exact grouped arm fails quality, reject the group-locality rule.
If exact passes but page-centered codes fail, reject this representation.
If both pass, classify only `quality-advance-memory-pending`: run native SQ8
and cold-S3 serving at 100k, a separate 1M grouped-code quality/locality
cell, and a measured two-generation resident-memory qualification before
format selection. No holdout or publication claim follows from this cell.

The initial memory path removes resident page centroids from row-score routing:
the tree's 192-float split normals suffice to obtain the 128-leaf order; page
centroids arrive in the authenticated code range. Under a provisional maximum
of 200,000 pages at 100M, two copies of the historical V102 reserve set
(2,539,009,404 bytes) plus two float32 normal arrays (307,198,464 bytes)
total 2,846,207,868 bytes before topology, group directory, authentication,
allocator and overlap costs. This is a design envelope, not a passing memory
worksheet. The production loader must account for those remaining terms,
concurrent queries, and both live generations below 3 GiB. The 4.8-GB row
codes at 100M remain in object storage.

## Exploratory ceiling and limitations

A read-only diagnostic over the already-closed residual artifacts mapped each
query's frozen 128-leaf order to its first 32 distinct four-page groups. It
authenticated the 762,442-byte membership and 512,093-byte truth files by
their recorded SHA-256 values, then counted GT100 owner pages. The restricted
truth-aware best-32-page *ceiling* within those groups was 98.516% mean and
92% p05, with 34 queries below 90; the original restricted oracle was
98.557%/91% with 33 below 90. This uses truth for diagnosis and is neither a
query-blind exact scorer nor a promotion result. The exact grouped arm in the
preregistered cell must still pass.
