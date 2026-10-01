# Read-only fresh-panel audit

Consultation 43de4f051edf439e, claude-sonnet-5-5/xhigh, exit 0, elapsed 380s. This is advisory analysis, not measured resource/quality evidence. Root protocol decisions are in semantic-1m-fresh-panel-preregister.md. Local execution suggestions and old consumer changes are not adopted; use bounded remote preparation and the new production scorer seam.

# Cheapest new 64-query development panel: ReLAION D768 (CoHere after survival)

No unconsumed authenticated query bodies exist for either dataset. Nothing here was edited, run, downloaded or sent.

## What is consumed
- **ReLAION rank16:** all ordinals 0–999 were opened. dev64 `a0002` read 0–63 and confirm936 `a0001` read 64–999 (`fresh-rank16-seal/a0001/verification.json`).
- **Old sealed panels:** `fresh/a0002` (source rows 100000–100999) was rejected because ordinal 903 was V189 pseudoquery 2578. The V36/V114/V198 roles all sit in the original ranks 0–15 bank, which the rank16 rule excludes by ID.
- **CoHere:**
  - registered test rows 0–999
  - train rows 100,000–104,999
  - train rows 1,000,000–1,001,999
  - train rows 1,005,000–1,005,999 (dev64 and confirm936)
- **Reusable material:** the only unconsumed material is IDs and rows never decoded, with no sealed bodies. For ReLAION that is the same hash ranking beyond ordinal 999 (about 3.57M eligible IDs). For CoHere it is the rest of shard 46, rows up to 1,026,714.

## Recommended path: continue the frozen ranking (ReLAION ordinals 1000–1063)
**Source and exclusion authority (all existing pins):**
- Registry `b9a19e2f…` and population authority `be6abb86…`.
- ID-overlap report `888748ef…`, HF revision `bfc7465d…`.
- Objects ranked 16–31 by sha256 of `b"borsuk-v36-screen-object-v1"` + path + le64 bytes.
- All original rank 0–15 IDs are excluded (3,579,759 IDs).
- Seed `b"borsuk-v36-rank16-fresh-v1"`; score is sha256(seed + le64 ID), ascending with ties by ID.
- Duplicates use the first occurrence by rank, then row offset.

**Panel and lineage:**
- **New panel:** reservoir ordinals 1000–1063 become `query_ordinal` 0–63, with a `reservoir_ordinal` field. Optionally select 2000 at no extra cost, so 1064–1999 are fixed as an undecoded confirmation reservoir before dev results.
- **Lineage:** the 16 objects keep the existing object-level evidence (701 terminals, 35+33 archives and 149 trees with zero references). No new history scan is needed because no new objects are touched.

**Duplicate and audit scope:**
- IDs are disjoint from the original bank and from the consumed 1000 by construction.
- No vector-level duplicate audit exists for ReLAION rank16, only ID and lineage checks. Add raw and unit sha256 checks against:
  - the indexed source `a3eac4de…` (inside the `source_raw` loop)
  - consumed `queries.raw` `a1a2d8d0…`
  - within-panel duplicates
- Any hit rejects all 64; never replace one row.
- `complete_prior_query_audit` stays false.

**GT oracle:**
- `scripts/run_native_source_frontier_1m.py::oracle`, pinned `fe91b3af…`.
- f64 cosine over source parquet `2796b579…`, GT100 as uint32 LE, ties by ascending source ordinal.
- Outputs are `queries.raw` 196,608 B, `truth.u32` 25,600 B and `requests.jsonl` (about 0.94 MB, ESTIMATE).
- They go to a new S3 prefix with `--if-none-match "*"` and sha256 metadata.
- The layout matches `range_object()` in the runner, so consumers read them with only a config change.

**Output pins to record:**
- panel SHA, config SHA, code SHAs
- the three sealed-object SHAs
- `decision.json`
- S3 HEAD identities

## Resources (all ESTIMATE)
- **Selection:** all 32 shards are already in `~/.cache/borsuk-v36-id-shards` (11 GB, every size matches the registry; the selector re-hashes them on use). Expect about 3–8 min on one core, at most 1 GB RSS, no network and no cost. Re-downloading would take about 6 min, going by the cached files' mtimes.
- **Seal:** about 3–6 min locally. The CoHere precedent ran 6:11 with 764 MB RSS for 1000 queries, and the 64-query oracle is about 16× cheaper than the 1000-query one. A Spot cell would cost about $0.02, against $0.0264 observed for 249 s of construction at 1000 queries.
- **Local venv:** `~/.cache/borsuk-cohere-1m-v4/analysis-venv` already has the pinned numpy 2.3.3 and pyarrow 24.0.0 that the scripts assert.

## Smallest correctness checks
1. Run the selector with `--keep 1064` and assert its first 1000 entries are identical to `fresh-rank16-provisional-ids.json` (`a8bd97d6…`): feature ID, rank, offset and selector hash. A failure means the path is not the frozen ranking.
2. Seal-time checks: re-read each feature ID at its locator, check vectors are finite and nonzero, and run the oracle `self_check()`. Optionally add the consumed ordinals 0–63 as control rows and require their GT to equal the sealed `truth.u32` prefix.
3. Run the raw and unit duplicate audit above.

## Files
**ReLAION:**
1. New `docs/research/native-union-20260928/fresh-rank16-next64-preregister.md`: an addendum committed and pushed before selection.
2. Edit `scripts/select_v36_rank16_fresh_ids.py`:
   - Add `--keep` and `--previous`.
   - Touch the hardcoded 1000 at lines 62, 70, 75 and 81.
   - A `--keep 1000` run should reproduce the old file byte for byte.
3. New `scripts/seal_v36_rank16_next64_1m.py`, a copy of `seal_v36_rank16_fresh_1m.py`:
   - Replace the hardcoded 1000 at lines 35, 61, 119–123 and 136, and the strings at lines 147–150, with the panel length.
   - Add the duplicate audit.
   - A copy is safer because the old verifier reads the old config by fixed name.
4. New `fresh-rank16-next64-seal-config.json`, plus a local verifier modelled on `scripts/verify_fresh_cohere_seal.py`. The AWS `verify-fresh-rank16-seal.py` hardcodes the old config names, 1000 rows, 3,072,000 B and c7g.4xlarge.
5. Consumer, the root's own: `scripts/run_native_fresh_rank16_dev64.py:38–49` needs a new schema in the whitelist. Line 49 forces `prospective_ordinals_sealed == [64, 999]` when `first == 0`, so a 64-only seal needs `[]` for the new schema.

**CoHere (after survival):**
- **Rows:** train rows 1,010,000–1,010,063, a gap of at least 4,000 from the consumed block so no neighbouring Wikipedia passages. Contiguous 1,006,000–1,006,063 is the alternative. Pin the choice before decoding.
- **Local run:** `terminal.json`, `source.raw` and the publication receipt are all cached, so no AWS compute is needed. ESTIMATE 3–5 min, from the 6:11 precedent for 1000 queries.
- **`scripts/check_fresh_cohere_source.py`:**
  - Change `FIRST` and `COUNT`.
  - Add `[1005000, 1005999]` to the excluded list.
  - Update the regex.
  - Hazard: it unconditionally overwrites `fresh-cohere-source-candidate.json`, the consumed pin, so give it a new output path.
- **`scripts/audit_fresh_cohere_vectors.py`:**
  - Replace the hardcoded 1005000/1000 slices, shapes and prefix.
  - Add the consumed `fresh-identity/queries.raw` (`28af674a…`) to the prior-hash set.
  - Use a new work subdirectory.
- **`scripts/verify_fresh_cohere_seal.py`:** same hardcodes as above.

## Risks and blockers
- **Preregistration wording:** it says "1,000 smallest", so continuing the ranking needs a root-authorised addendum committed before the selector runs. The root decides this.
- **Duplicate rate:** the ReLAION vector-duplicate rate is unmeasured. CoHere had 0 of 1000. A hit costs one rerun, and any successor panel is the root's call.
- **Scratch hazard:** `seal_v36_rank16_fresh_1m.py` calls `path.unlink()` on every shard it reads (line 60). Never pass the cache directory as scratch; symlink the cached shards into a fresh `out` directory instead.
- **Router build:** the semantic 1M ReLAION generation is not built, so the dev falsifier cannot run yet. Panel and GT sealing do not depend on it.
- **Resolution:** 64 queries give 640 top-10 trials, so the result is coarse. Expect about ±1 pp near 98%.
- **Local contention:** seal compute is a few GB of RAM plus BLAS. Use `taskset`/`nice`, or use Spot, if the Rust child is compiling. The paid campaign is on AWS and is untouched.

