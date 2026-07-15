# Benchmark Evidence Dashboard Design

## Goal

Improve the docs and webpage so benchmark evidence is more complete, more legible, and honest about billion-vector scale. The page should persuade performance-minded OSS developers and platform/search engineers with measured data, artifact provenance, and runnable commands.

## Evidence Policy

Benchmark claims must be measured or clearly identified as a target/attempt. The 1B-vector story is a local benchmark attempt, not a published result until the run completes and writes an artifact. If the run stops at the practical limit, the docs and webpage report the largest completed measured scale and the stop reason.

## Benchmark Scope

- Keep the existing million-vector release gate as the checked-in measured high-scale result.
- Add a separate local billion-vector attempt path with target `1_000_000_000` records, 16 dimensions, 4 hour max elapsed time, and 250 GB max temporary storage.
- Keep 64D benchmark data as the representative smaller comparison family.
- Preserve existing `large-scale.csv` semantics for the million-vector gate.
- Add a separate `billion-attempt.csv` artifact so partial attempts cannot be confused with release-gate measurements.

## Webpage Design

Use an industrial systems dashboard direction: dense, technical, tactile, and evidence-first. The benchmark area should show:

- measured evidence cards for 1M recall, latency, bytes/query, routing pages, RSS delta, ingest, compaction, and GC;
- interactive charts/tables for existing sequential, scale, routing-overfetch, lifecycle, parallel, and large-scale CSVs;
- a 1B local attempt panel showing requested records, completed records, dimensions, stop policy, stop reason, elapsed time, temp bytes observed, and artifact status;
- a reproducibility panel with exact commands and artifact-copy rules;
- routing hierarchy context for 1B, using the existing hierarchy calculator instead of unsupported projections.

Measured and attempted data must be visually separate. The 1B target panel should never read like a completed benchmark unless `completed_records` equals `requested_records`.

## Docs Design

`docs/benchmarks.md` and `README.md` should explain:

- which artifacts are measured and checked in;
- how to run the normal report and million-vector release gate;
- how to run the local 1B attempt with stop limits;
- how to interpret partial attempt artifacts;
- how to copy completed artifacts into `docs/web/assets/benchmarks/`.

## Testing

- Unit-test the new billion-attempt CSV formatting and stop-reason behavior.
- Extend the docs webpage Node test so it requires loading and rendering `billion-attempt.csv`.
- Add doc/web assertions for the separate measured-vs-attempted wording and metric selectors.
- Verify Rust tests, docs webpage tests, and formatting after implementation.

