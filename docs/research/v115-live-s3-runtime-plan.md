# V115 runtime route and live S3 gate plan

Status: design after the [V114 paired 1M development pass](v114-1m-paired-closeout.md).
The next claim is a measured live query path, not a projection from V114's
offline preparation or Rust batch process times.

## Decision and frozen comparators

Keep the source-only V114 SQ8 bytes and 256-row physical page geometry. The
V114 1M development terminal at source commit `87881c71e048d557c5c1285ffa0ac1d8c381f764`
is immutable historical evidence: 99,234/100,000 returned hits against a
same-run V109 baseline of 98,803, with exact RAM/file score and route parity.
V115 must first establish runtime nomination and route parity to that frozen
cohort. A changed roster or plan requires a new paired offline returned-quality
cell before a live latency claim; it cannot inherit V114's 99.234% result.

The method must have no dataset-name branch, GT-trained threshold or
vector-count memory knee. Source-only PQ64 training may adapt to each corpus.
The vote factor is `max_nominees + 1`, which Lean proves dominates any bounded
secondary roster; V114's factor 513 is the 512-nominee specialization. The
memory tier is selected from a measured `(N,D,R,C,G,L)` frontier, and a
request outside qualified bounds fails visibly.

## Task 1: authenticate the actual S3 data wave

- Build a page-digest sidecar from the authenticated SQ8 object before
  queries or GT are available. It records SHA-256 for every 256-row page,
  including the short final page, and binds row count, dimension, object
  SHA-256, page geometry and sidecar SHA-256 to one generation. Keep the
  existing 4-KiB sidecar for local file reads. Mark the new transport
  sidecar with its own format version; never reinterpret old artifacts.
- Each S3 range begins and ends on those physical page boundaries, so a
  response can be checked page by page without fetching extra bytes. Require
  the exact 206 status, Content-Range and Content-Length, frozen object ETag
  on conditional GETs, and page digests. A wrong length, digest or object
  generation fails before returning IDs. Test a short final page, a changed
  byte, an incorrect range and an object changed after hydration.
  The current interval DP charges byte budget in 32-row units, but its
  returned intervals always begin/end on **256-row page** boundaries (except
  the short final page). The SHA sidecar therefore follows pages, not the
  DP accounting units; this is checked against both Python and Rust range
  construction before using the sidecar on S3.
- Verify the V70 S3 object identity once by full streaming SHA-256 at
  hydration. Its current S3 HEAD exposes a multipart ETag but no SHA-256
  checksum or version ID, so ETag alone is insufficient as a cryptographic
  authority. The page sidecar supplies authenticated range bytes afterward.

## Task 2: move nomination into the measured runtime

- Export a source-only router artifact: dimensions, rows, page geometry,
  summaries, PQ64 books and codes, source/object hashes and format marker.
  Keep development queries and GT in separate files. Verify router bytes
  against the frozen V77 manifest's source-derived fields.
- Implement geometry-driven PQ64 region selection and top-nominee ordering
  in production-facing Rust. Match the frozen V111/V114 roster for all
  1,000 development queries or record every difference and rerun the paired
  returned-quality gate under the new source revision. Test ties, non-multiple
  dimensions if supported, short final pages, and finite input admission.
- Keep the Rust mirror scorer alive across requests. Measure routing,
  authenticated local nominee reads, physical planning, S3 range wave,
  returned scoring and total latency separately. Implement the S3 range
  transport and returned SQ8 scorer in Rust as part of that serving process;
  Python may validate artifacts after the run but is outside the timed query
  path. This must be the same query path in both RAM and NVMe arms; only local
  SQ8 placement differs. Test one-wave inclusive HTTP byte ranges, 206 and
  412 handling, exact returned IDs, retries and logical versus actual GETs.
- The current file scorer reads 4-KiB blocks serially and may be limited by
  NVMe queue depth. Before the paid gate, add deduplicated parallel block
  reads with bounded depth and the same digest checks, or preregister a
  separate direct-I/O implementation. Retain per-block read timing so the
  disk delta is measured, not hidden in total S3 time.

## Task 3: one preregistered live Spot campaign

- Use one `eu-central-1` Spot instance with verified local NVMe, tentatively
  `i4i.2xlarge` (8 vCPU, 64 GiB, 1.875 TB Nitro SSD). Its observed Spot
  price was $0.256900/hour in `eu-central-1c` at 2026-09-23 20:00 UTC;
  refresh the zone price, capacity and instance identity before launch.
  Download/hydrate the SQ8 object onto NVMe and RAM, then verify both against
  the same source-only authority. Do not treat EBS as the NVMe placement.
- Freeze 1,000 ReLAION-1M development queries and the identical runtime
  method before timing. Run paired RAM/file arms at concurrency 1, 8 and 32,
  with balanced order, distinct cold and warm conditions, and a bounded
  connection preflight excluded from timed queries. Use one S3 data wave
  per query. Record per-query returned IDs/hits, GETs/bytes, p50/p95/p99,
  QPS, local read counts/bytes, NVMe occupancy/IOPS, S3 transport times,
  cgroup steady/peak memory including file cache, and hydration cost.
- A buffered file read may be satisfied by page cache. Charge that cache
  to the file tier and observe actual NVMe read deltas; do not call a warm
  cached result a disk latency measurement. An isolated cold-cache or direct
  I/O arm needs its own explicit protocol and cannot be mixed with warmed
  results. Use isolated cache state and record `memory.current`, `memory.peak`,
  file-cache bytes and NVMe read counters for each arm. Check two pinned
  generations and rollover only after the
  generation manager supports it; until then this remains an open product
  gate, not an inferred property of the one-generation scorer.
- Preserve identical returned IDs and zero physical cap/authentication
  failures in both placements. The preregistered provisional disk
  promotion gates remain local-read p95 delta at most 15 ms, p99 delta
  at most 30 ms, and end-to-end p95 delta at most 15 ms versus paired RAM.
  Apply an absolute research stop rule of end-to-end p95 at most 200 ms and
  sustained throughput at least 20 queries/s at concurrency 8; these are
  feasibility screens, not matched commercial comparisons or release SLOs.
  Report the absolute latency/QPS and market baseline separately; a disk
  pass alone does not establish competitive product performance.
- Sync terminal repetitions and all raw evidence to immutable S3 keys.
  On Spot interruption, discard and restart the affected measurement cell
  under a fresh attempt ID. Monitor only terminal/infrastructure while
  incomplete and terminate compute immediately after terminal closure.

## Promotion after live S3

Run untouched ReLAION-1M validation and deep-image-96-angular from
independently built source-only indexes under the same method and acceptance
rules. Then profile nomination growth, local I/O and `G ≥ 2` resource use at
10M before attempting 100M. A different architecture starts a new frozen
comparison rather than importing V114's development numbers.
