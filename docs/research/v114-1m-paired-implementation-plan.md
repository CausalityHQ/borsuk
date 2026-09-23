# V114 Paired 1M Development Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task by task.
> Follow the source-frozen, focused-test, Spot-first workflow. This is
> an authorized continuation of the production goal; implement and
> review each slice before launching its paid cell.

**Goal:** Decide whether the exact local SQ8 mirror preserves at least
99.0% returned Recall@100 and its required tail quality under the 32-GET,
16,777,216-byte one-wave S3 data plan on ReLAION-1M development.

**Architecture:** Reuse the V112 frozen ReLAION-1M source, development
queries, GT100, V63 layout and V70 SQ8 object. Build and seal the V114
mirror from source-derived low/step and the already source-only SQ8 object
before query and GT download. Rebuild the deterministic V77 PQ64/router
manifest once, then run a paired V109 capped baseline, same-run exact SQ8
oracle using V114 physical planning, and both Rust local placements over
the same 512 nominated rows. Final returned IDs come from the same SQ8
page bytes for every arm.

**Spec:** [V114 exact local gates](v114-exact-local-nominee-gate.md), with
[the passed 100k correctness closeout](v114-exact-local-100k-closeout.md).

**Data authority:** `scripts/launch_v112_precise_nominee_spot.py::INPUTS`
pins the five immutable URIs, SHA-256 values and sizes. The 1M cell must
copy these identities verbatim. Its baseline must reproduce V109's
98,803/100,000 returned GT hits on all 1,000 development queries (or stop
as a harness mismatch). V112's historical 99,234/100,000 is context; the
same-run exact oracle with the new planner is the paired comparator.

## File boundaries and contracts

- `scripts/v114_1m_mirror.py`: `seal_existing_sq8(source, sq8, mirror,
  source_sha256, sq8_sha256)` returns a source record and writes the 4-KiB
  sidecar and mirror manifest. It scans only corpus rows and the existing
  SQ8 object, computes V70 low/step, checks full object identity, then
  seals. No query or GT argument exists.
- `scripts/v114_1m_paired.py`: `prepare(manifest, mirror, sq8, requests,
  reference, count)` fixes one source-only PQ64 top-512 roster per query,
  prepares exact local Rust requests and a Python reference with V114
  513/1 votes and tie policy. `reduce(manifest, sq8, reference, rust,
  evidence, summary)` compares all score bits/primary sets/routes, scores
  fetched pages for V109, oracle and production arms, then intersects
  immutable GT100 only after each arm's returned IDs are fixed.
- `scripts/validate_v114_1m_paired.py`: independently reloads input
  identities, rechecks all source/SQ8/mirror hashes and block digests,
  recomputes every query's nomination, plans, returned IDs and GT hits,
  and checks the exact evidence/reduction hashes.
- `scripts/launch_v114_1m_paired_spot.py` and
  `scripts/run_v114_1m_paired_remote.sh`: one Causality Spot instance,
  one frozen pushed archive, an immutable reservation and terminal,
  interruption detection, and immediate termination. Observe incomplete
  work only by terminal and infrastructure health.
- `docs/research/v114-1m-paired-closeout.md`: publish the terminal-bound
  paired result, source revision, baseline reproduction, resource use,
  decision and exact next live S3 gate.

## Task 1: source-only 1M mirror

- [ ] Write a tiny Parquet fixture with `feature_row_id, embedding`, a
  two-block SQ8 file and a reordered source layout. Assert the mirror's
  row bytes and sidecar hashes match the source-only inputs and that
  changed source/SQ8 hashes fail. Run the test and observe the expected
  failure before implementing.
- [ ] Implement `seal_existing_sq8`. Use the V70 source-derived quantizer
  rule `low=min(source)`, `span=max(max(source)-low,1e-12)`,
  `step=span/255` in f32. Verify that rule against the regenerated V77
  manifest in Task 2. Retain the existing SQ8 object path; do not copy
  780,000,000 bytes into a second file. Bind a generation marker and
  `max_nominees=512` without switching placement at a row threshold.
- [ ] Run the narrow fixture test and mirror Rust tests. Commit and push
  only after byte geometry and hashes pass.

## Task 2: paired offline returned-recall pipeline

- [ ] Write a tiny deterministic 256-row, 64-D fixture for the generic
  roster, exact score, page-vote and returned-hit functions called by
  `prepare`/`reduce`. Keep the frozen 1M input wrapper separate from
  these geometry-driven functions. Assert V109 and exact arms see identical nominated
  rows, that 513/1 weights include all 512 nominees, that GT is touched
  only after routes and returned IDs are fixed, and that a Rust primary
  or range mismatch fails before reporting quality. Observe failing
  tests first.
- [ ] Implement query preparation with V77's 1,024-region PQ64
  nomination on the frozen 1M corpus. Keep row count, dimension, page
  geometry and budget explicit so the method can be tested on another
  corpus. Feed the Rust CLI one ordered JSONL request per query and
  record an independent Python exact-SQ8 reference. Reuse the proven
  V114 tie/byte planner for the oracle, and the V109 capped admission
  function for the paired baseline. Do not use historical V112 plans as
  the new oracle.
- [ ] Implement returned scoring over the same fetched SQ8 page ranges
  and `(score, ID)` final top 100 for all arms. Record per-query ordered
  returned IDs, GT hits, primary/page/range equality, GETs/bytes and
  score-bit parity. Reject any query with a primary, route or cap mismatch.
- [ ] Run the fixture and focused Rust checks. Commit/push the pipeline.

## Task 3: one frozen Spot cell and decision

- [ ] Add launcher/runner tests for source archive hash, immutable
  reservation, `c7i.12xlarge` Spot launch, terminal and termination.
  Run `bash -n` and `shellcheck -S warning`. Register current regional
  Spot price observations, 100-GiB-plus input storage need, interruption
  handling and instance identity before launch.
- [ ] Archive the exact pushed revision; upload it with an S3
  `If-None-Match: *` condition. On Spot, download and check frozen source,
  layout and SQ8 first. Seal the mirror with query/GT paths unavailable.
  Download and check query/GT only afterward, regenerate the V77
  manifest and verify its low/step exactly matches the sealed mirror.
- [ ] Run a 200-query paired prefix. Require V109's historical 19,739
  GT hits and zero cap violations, exact RAM/file/reference score and
  route parity, and no source/query/GT identity drift. A failed prefix
  closes the attempt without promoting to 1,000. A passing prefix runs
  the same frozen implementation for all 1,000.
- [ ] Independently validate terminal-listed artifacts by streaming
  SHA-256 and byte counts after the terminal exists. Confirm EC2
  termination. Never read incomplete measurement files.
- [ ] Promote to live S3 only if the all-1,000 production arm returns
  at least 99,000/100,000 GT hits, p05 is at least both 90 and the
  stronger of V109 p05 and paired exact-oracle p05 minus one, sub-90
  queries do not exceed paired V109, and all physical caps and exact
  placement parity hold. Otherwise name the responsible layer and
  redesign before another paid cell.

## Next live gate after offline promotion

Use one available `eu-central-1` Spot instance class with real local
NVMe, the same frozen 1M cohort and two explicit placements. Pair cold
and warmed RAM/file arms at the same concurrency and page plan; query
the actual S3 SQ8 object with one range wave. Measure p50/p95/p99,
QPS, local read delta, cgroup peak/steady RAM, NVMe occupancy and IOPS,
S3 GET/bytes, hydration and two-generation rollover. Require the
preregistered disk versus RAM latency deltas and identical returned
hits/caps. Then run untouched ReLAION-1M validation and
deep-image-96-angular before 10M/100M.

No fixed vector-count memory knee is used. `M(N,D,R,C,G)` remains an
empirical frontier over dataset size, dimension, requested recall,
concurrency and generations; Lean proves only explicitly stated
conditional score and capacity properties.
