# Exact e472 wrapper research review

Consultation: e0bfc77420ea4cb2. Completed; source inspection only. Candidate remains unqualified.

# Review of `run_native_scale_build_gate.sh` at e4722262

**Verdict: it needs one small repair (B1), then it is GO for a bounded canary.** It is not native, recall or performance qualification. Everything below comes from reading source only. I ran no wrapper code, no jq evaluation, no native binary and touched no data.

I confirmed the candidate's identity: commit e4722262 has parent 920f2533 and descends from a02c233d, and the script's SHA is 29bfe333…. The commits after a02c233d change only this script. The four native sources are unchanged from bc3082a8.

## Decisive blocker

**B1. Wrapper failures can exit with 2 or 3, which collide with the baseline's own result codes** (`finish`, lines 57–61).
- Only `rc==0` is remapped to 98 when the chain was not verified. Any command that fails under `set -e` leaks its raw status:
  - jq: 1, 2, 3 (compile error) or 5
  - `cmp`: 1 or 2
  - `sort`: 2
  - `xargs`: 123–127
- So a wrapper failure can exit 2 or 3, the same codes as the baseline's INVALID and RESOURCE_REJECT. This breaks the contract's "98 any other failure". It also breaks the rule that glue errors are INVALID, never a candidate outcome.
- In baseline-serving mode, RESOURCE_REJECT is documented as a direct-closure-only result (`check_cohere_native_baseline.rs:103-107`). An exit 3 is therefore more likely a jq compile error, for example from a different jq on the host.
- A related defect: a signal arriving between `verified=1` (line 668) and the exit (line 670) gets labelled `BASELINE_NONZERO_EXIT` with exit 143.
- `terminal.json` still says INVALID in these cases, so this is not a false success. It is a dispositional collision on the exit code.
- **Minimal fix:**
  ```bash
  if (( verified == 1 )) && [[ $rc == "$baseline_exit" ]]; then …
  elif (( rc != 129 && rc != 130 && rc != 143 )); then rc=98; fi
  ```
- **Smallest remote falsifier:** an exact-wrapper admission refusal with `derive_config.path` set to an existing canonical directory. jq fails at line 332 ("Is a directory"), and the wrapper exits **2** instead of 98. No native process runs.

## Checked against source, no discrepancy found
- **Derive** (`build_sq8_source.rs`):
  - Arguments are `--derive CONFIG SHA DIR` (lines 13–17).
  - The receipt is v2 with the same 15 keys. Recipe, `query_or_truth_used:false` and the qualification string match.
  - SQ8 size is rows×(D+12) = 1,036,000,000, and derive mode writes nothing to stdout.
- **Generation** (`build_two_bit_generation.rs:17-33,71,120`):
  - The config has 15 fields with `deny_unknown_fields`, and the snake_case names `semantic` and `scale1m` are correct.
  - It takes 4 arguments and prints the root SHA with `println!`, which is exactly 65 B.
- **ETag:** object_store 0.14.1 produces `"\"{inode:x}-{mtime_µs:x}-{size:x}\""` with quotes (`local.rs:1447`). The wrapper's quoted format and microsecond truncation match.
- **Publisher:** 3 arguments, the v1 config schema, a 7-key receipt, empty stdout, and exit 2 on error.
- **Baseline:**
  - The config struct has exactly the 28 fields; serving `{"mode":"baseline"}`, execution `{"mode":"full"}`, the local backend, fetch parallelism 16 or 32, and the 512 MiB cap all match.
  - `generation_prefix` is the publisher's `metadata_prefix`, and `open_remote` uses it.
  - The baseline prints its summary to stdout.
- **Calibration is bound in native code.** The baseline requires the generation's `low`/`step` values, bit for bit, to equal the derivation receipt's bits ("generation/producer exact calibration binding", around line 1005).
  - So a jq f32 decode or serialisation defect fails as INVALID after the build, never as a false measurement.
  - A pure-literal jq check before the canary only saves compute.
- **Mountpoint:** `scratch_root` must be a mountpoint, and `bootstrap-layout.pending.md:5` mounts the 40 GiB gp3 volume at `prepared-parent`, so they agree.
- **Pending template:** its phase timeouts are at the maxima, the deadline is 9600, and parallelism is 16/16, all matching the wrapper's checks. The worst-case generation config is about 50 KB, under the 64 KiB cap.

## Optional improvements, none decisive
1. **Gate the shell and jq versions at admission.** Lines 450–455 `wait` on process substitutions other than the last one, which needs bash 5.1 or later. On older bash, every phase becomes INVALID after its native work has already run. Today `tools.txt` only records the versions.
2. **`baseline_invoked=1` is set before the deadline check** (line 630), so a deadline refusal still reports the baseline as invoked. That errs on the conservative side, but the flag would be more accurate set after the check.
3. **The derive config is validated before it is authenticated** (line 332, authentication at 491). That is a small time-of-check gap, and its path is not checked to be a regular file. Moving the validation after `authenticate_inputs` fixes both.
4. **The closing rehash is outside the 9600 s gate.** It covers about 10.5 GB (inputs, derive outputs, staged SQ8). At the default gp3 throughput of 125 MB/s that is roughly 85 s, more than the 60 s outer slack if phases run near their maxima. The failure would be INVALID, not a false result. Fix by reserving closure seconds in the baseline's deadline check or by provisioning more scratch throughput.
5. **Signals to the wrapper PID alone are deferred until the running phase ends**, because bash runs traps only after the foreground pipeline finishes. Root should stop the whole cgroup. Adding `</dev/null` to the native call is also worth it.
6. **Keep the 1 MiB stdout caps.** A generation cap under 65 B, or a baseline cap under the summary size, turns a good result into a panic (exit 101) and then INVALID. The schema could enforce a generation cap of at least 65.
7. **GNU time 1.7 reports RSS four times too high**, which would falsely trip the RSS check for processes over 2 GiB. Record or gate the time version.

## Research-critic notes (no wrapper change needed)
- **Authority chain.** Every link is pinned:
  - corpus seal → constant derive-config SHA → producer self-hash
  - receipt bits → generation manifest → baseline bit comparison
  - truth sealed through `complete.json` and checked again by the baseline's cohort validation

  One gap remains. That `low`/`step` match the parameters actually used to encode `sq8.bin` rests on the producer's own consistency (its tests at lines 1016–1017). A mismatch there would look like recall loss, not INVALID. An optional query-free check is one pass over `normalized.f32` to recompute them.
- **Q32 is a weak screen.** The queries are the contiguous ordinals 100000–100031. Wikipedia rows are probably grouped by article (unverified), so the effective sample size may be well below 32. Same-article paragraphs just outside the reserved interval are in the corpus, which may make these queries easier than a random draw. Treat 304/320 as a "not broken" smoke signal only. Report per-query hits and the number of distinct articles in `queries.ids.jsonl`. Do not compare against Turbopuffer's undisclosed query split.
- **Truth vs. served vectors.** Truth is cosine on the original vectors, while the index serves normalized f32. A rounding-induced tie flip cannot be told apart from an ANN miss. That is immaterial at 304/320 but should be stated in the disposition.
- **The baseline output is not in the evidence bundle.** `baseline-result.jsonl` (up to 64 MiB) stays in `query_dir`. Root must put it into the raw-evidence archive before termination, as `bootstrap-layout.pending.md:13` plans. Otherwise the root-only screen has no input.
