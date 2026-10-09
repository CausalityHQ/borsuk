I reviewed commit `5098c6aa`, which adds `--derive` to `build_sq8_source`. I read the committed source statically with `git show` and did not build, run or edit anything.

**Verdict:** the composition is correct. `normalize_source` → flat `fit_source_order` → `build_sq8_source` matches the library APIs, and the receipt-v2 schema matches what `validate_receipts` reads. The calibration bits are bound to the served generation's `manifest.json` low/step. The same receipt validates unchanged for Q32 and Q1000.

I found no compile errors and no way for the code to produce a false success. There is one likely production run blocker (finding 1) and one confirmed failure-path defect (finding 2). The rest are gaps in what the code claims versus what it proves.

## Ranked findings

**1. HIGH, likely but unconfirmed — the release binary may refuse its own executable hash.** `build_sq8_source.rs:263-266` (used by `executable_sha()` at :338 and :353)
- `stamp()` requires `nlink() == 1`, and that rule is also applied to `/proc/self/exe`.
- Cargo normally places a built example as `target/release/examples/build_sq8_source-<hash>` and hard-links it to `…/build_sq8_source`. Both names share one inode, so the link count is 2 whichever name you run.
- Result: the production run would stop with "single-link regular source/output" before writing anything. This fails safe, but the derivation would never run.
- The tests don't catch it: they run under the test-harness binary, which has a single link.
- I couldn't confirm this here because the local target directory has no uplifted example binaries. On the gate host, check `stat -c %h target/release/examples/build_sq8_source`.
- Minimal fix: don't apply the link-count rule to the executable (compare dev/ino/len/mtime only), or copy the binary to a single-link frozen path before pinning `executable_sha256`.

**2. HIGH, confirmed — the revocation path hides the original error, and its durability is unproven.** `build_sq8_source.rs:711-715`
- If `fs::remove_file` fails, its error replaces the original fsync error. The COMPLETE receipt then stays on disk while the process exits non-zero.
- If the second `dir.sync()` fails, which is likely after a first EIO, the original error is also lost. On Linux, a retried fsync can report success after an earlier failure, so a successful second sync does not prove the revocation is durable.
- If `Directory::check` fails because the output directory was swapped, `remove_file` deletes `derivation.json` in whatever directory now sits at that path. The real receipt stays in the moved directory.
- None of this is tested: `boundary()` has no phase after the receipt is moved into place.
- Minimal fix: always return the original error, with any cleanup failure appended as context. Add a `"published"` boundary phase and one test that injects an error after publication and asserts the receipt is gone and the original message is returned.

**3. MEDIUM — exit status 0 is the only real success signal, and no consumer requires it.**
- A COMPLETE receipt exists with a non-zero exit in three cases:
  - the process is killed or loses power between the receipt move (:708) and the directory sync (:711);
  - the revocation in finding 2 fails;
  - the directory is swapped.
- Neither the baseline nor the reducer can tell these apart from success. They accept any receipt whose SHA appears in the frozen config.
- Minimal fix: the root should freeze `derivation_receipt.sha256` only from a run with a recorded exit 0, and should record the producer's command and exit receipt next to it. This is a procedural requirement, not something the code enforces.

**4. MEDIUM — several receipt fields are echoed from config, not verified.**
- `corpus_intervals` (:193, :394-405) is only summed to `rows`; it never selects bytes. `source_commit` is echoed back unverified (:546). `admission.*` is a modeled number.
- The baseline does bind the corpus SHA to the cohort receipt, so query/truth exclusion rests on the cohort receipt's provenance, not on this producer.
- The baseline's admission check (`check_cohere_native_baseline.rs:342-352`) is close to empty. For example, scratch ≥ original bytes is always true.
- Minimal fix: drop `corpus_intervals` from the receipt, or label it as declared in config. Don't count the baseline admission check as qualification evidence.

**5. LOW–MEDIUM — the pinned source files are not the full source closure.** `build_sq8_source.rs:358-366`
- The flat fit's results also depend on `two_bit_source.rs`, `Cargo.lock`, and the nalgebra/matrixmultiply and rand_chacha versions.
- `executable_sha256` covers the effective binary, but the field names suggest more source authority than the receipt actually carries.
- Minimal fix: either pin a `Cargo.lock` SHA, or document that `executable_sha256` is the authority and the three source SHAs are partial.

**6. LOW — failed runs leave partial files under their final names.** After a failure, `normalized.f32`, `order.u64` and a possibly partial `sq8.bin` remain (`sq8_source.rs:253` uses `create_new` directly, with no temporary file).
- This is safe only if every consumer, including the generation publisher, requires a receipt with matching seals before reading `sq8.bin`. That rule is external to this delta.

**7. LOW — the double-counting is only conservative, not a bug.**
- The SQ8 stage charges `order` twice: once at :441 and again inside the API's own model. The fit stage charges 40 bytes per row against an API figure of 32.
- `include_bytes!` lengths (read-only binary data) are counted as heap.
- No underflow is possible: `budget - rows*8` is safe because peak ≤ max payload.

**8. LOW — test fragility.**
- `Snapshot::open` requires a canonical path, so the tests fail if `TMPDIR` passes through a symlink.
- No test runs the real `--derive` binary through `main`, which is the only path that would catch finding 1.

## Requirements outside this code (not claimed as verified)

- **Disk space:** the scratch figure is modeled and compared with a config value, with no free-space check. At 1M × 1024 it is roughly 13.4 GB.
- **Memory:** no actual memory limit is set (no RLIMIT/cgroup). The 8 GiB ceiling only bounds the modeled payload.
- **Fit runtime:** the flat fit is O(12·S·C·D + N·C·D), roughly 10¹³ floating-point operations at 1M rows. There is no timeout, and its own ponytail comment requires scale qualification first.
- **Binary-to-source binding:** linking the executable to reviewed source, and linking the corpus SHA to the intervals, both stay with the root and the cohort receipt.
- **Pending work:** native qualification, the actual-input parity check and the 1M cold benchmark are still pending. Nothing in this delta claims performance.

## Checked and sound

- **Seals:** original, normalized, order and SQ8 contents are fully re-hashed before publishing. Stamps check dev, inode, ctime and link count, which also covers mutation during the SQ8 second pass.
- **Config:** pinned by SHA and parsed twice (`deny_unknown_fields`). There is no query or truth input (`queries`/`truth` are refused as unknown fields).
- **Output creation:** `create_dir` is exclusive. The receipt is written to a temporary file, fsynced, moved into place without overwriting, then the directory and parent are synced.
- **Calibration bit-exactness:** shortest-form f32 JSON parsed through f64 back to f32 returns the original bits. Any mismatch fails closed.
- **Size limits:** the D=1024 receipt is about 25 KB, under both 64 KiB caps.
- **Test fixtures:** the fixture generation's low/step (-1, 1/16) match `fixture_receipts`. The v6→v7 and v3→v4 schema bumps are consistent across the producer, baseline and reducer, and their tests.
- **APIs and dependencies:** all used APIs exist at the commit: `rustix` 1.1 with `fs`, `tempfile` `persist_noclobber`, `SourceInterval: PartialEq`.
