# Offered-load implementation consultation

Read-only consultation cd9efb13f24b4bc9 completed exit 0; codex/gpt-6.1-sol/xhigh. Evidence base 0146c799. Recommendations are implementation guidance; the root-owned frozen protocol/config remain launch authority.

Use an explicit-parameter seam and retain the existing six-thread scheduler. A lock around `scoped_runner()` would serialize cold calls; per-offer Python processes would add client startup and IPC to the offered-load experiment.

At `0146c799`, [`scoped_runner()`](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/run_native_semantic_router_cold.py:250) replaces shared functions and `os.environ`. `measured_call()` therefore cannot run concurrently. The production path here does **not** assign `sys.argv`; keep workers below the CLI layer. [`validate_record()`](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/run_native_semantic_router_cold.py:461) also hardcodes port 8080, and `reduce_run()` explicitly rejects overlapping calls.

1. **Make the shared cold call safe for concurrent semantic calls.**

   Modify [`scripts/run_native_cold_first_query.py`](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/run_native_cold_first_query.py:35):

   ```python
   cold_call(..., *, port=8080, check_response=None, check_startup=None,
             post_call=None, spawn=None, env=None)
   ```

   Resolve omitted hooks to the existing functions inside the call. Pass `env` to `Popen`; leave the fresh temporary directory, process launch, TCP connection retries, single HTTP request, wire-completion timestamp and cleanup boundary intact.

   In `scripts/run_native_semantic_router_cold.py`, replace `scoped_runner()` with call-local closures passed through those hooks. Add `port=8080` to `measured_call()` and `validate_record()`. Bind the raw ready header to that allocated port.

   Each native process receives an environment copy containing `BORSUK_NATIVE_MEMORY_BYTES=536870912`, `AWS_MAX_ATTEMPTS=1` and `TOKIO_WORKER_THREADS=4`. No worker changes module functions, parent environment or argv.

2. **Extract only the scheduling loop.**

   Modify [`scripts/run_native_cold_offered.py`](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/run_native_cold_offered.py:64):

   ```python
   schedule_offers(call_one, offered_qps, *, workers=6,
                   base_port=18080, deadline_ns=None)
   # -> records, epoch_ns, terminal_ns, abort_after
   ```

   Keep the existing `measure()` as its legacy adapter. The semantic callback is `call_one(ordinal, port)`, returning the exact `measured_call()` receipt.

   Preserve absolute offsets, immediate capacity drops, no executor queue, and port ownership through cleanup and validation. A small admission lock may protect reservation versus abort publication; it must never cover the service call or waiting.

   Preserve semantic fail-fast behavior: a returned semantic `failed` record aborts further admissions. Previously admitted calls finish cleanup. Every remaining position becomes `arm_aborted`, with zero HTTP/process attempts and an explicit abort receipt. Unconfirmed cleanup poisons the slot and aborts the campaign; it must not return the port for reuse.

3. **Add offered execution and reduction to the existing semantic runner.**

   Keep implementation in `scripts/run_native_semantic_router_cold.py`; no new runner module. Add `run_offered()` and `reduce_offered()`. Reuse `prepare()`, `validate_runtime()`, `validate_record()`, `validate_ready()`, `validate_outcome()` and `reduce_calls()`.

   Root-owned files:

   - `docs/research/performance-architecture-20260930/semantic-cold/offered-config.json`
   - `docs/research/performance-architecture-20260930/semantic-cold/offered-protocol.md`

   Freeze a distinct config/result schema: `borsuk-native-semantic-router-cold-offered-v1` / `borsuk-native-semantic-router-cold-offered-result-v1`. Require rates `[.25,.5,1,2,4,8]`, count 64, k10, workers 6, ports 18080–18085 and the existing resource/identity contract.

   Use rate-major order, then ReLAION/CoHere, then control/candidate: **24 cells, 1536 planned offers**, one 64-query panel per dataset/arm/rate. Reuse the same authenticated vectors, truth and arm-specific references. Prepare inputs before any cell timer; drain each cell completely before starting another.

   Keep the five positional runtime arguments; select execution by authenticated schema. Add an offered-only `--closed-cell-prefix PREFIX` for the root’s bounded checkpoint uploads. Write one immutable `rate{i}-{dataset}-{arm}-records.jsonl` per terminal cell. Retain explicit aborted files for unstarted cells after a controlled failure.

   Required output and gates:

   - **Ledger:** exact cell/ordinal roster; scheduled, dispatch, start, response and cleanup timestamps; allocated port; raw success/failure evidence; actual namespace and HTTP attempt counts.
   - **Counts:** `64 = successful + failed + capacity_drops + aborted`; distinguish admission from process start. No retry or replacement.
   - **Throughput:** successful and admitted-completed counts divided by first scheduled offer through final cleanup. Report `64/rate` as the planned window separately; the last scheduled offset is `63/rate`.
   - **Tails:** p50/p90/p95/p99 for cold service, scheduled-to-response and dispatch delay. Also report scheduled-to-valid-response percentiles over all 64 offers: unsuccessful offers occupy infinite completion time; emit `"UNBOUNDED"` when a percentile reaches them, rather than JSON infinity or a survivor percentile.
   - **Quality:** success-conditioned recall and `successful_hits/640`. A cell passes only with 64 valid successes and at least 608 hits; drops cannot satisfy the quality gate.
   - **Accounting:** sum each process’s latest validated final snapshot, or validated startup snapshot when final telemetry is unavailable. Label incomplete totals as lower bounds. Preserve GET/HEAD/PUT, credential, source/router/SQ8 and byte distinctions; do not relabel submissions as confirmed wire requests.
   - **Resources/cleanup:** native admission and RSS ≤512 MiB, complete cleanup receipts, aggregate cgroup limit 8 GiB, swap zero, no OOM/kill events, and verified port ownership ≤6.
   - **Timing validity:** preregister a dispatch-lateness gate; a concrete starting limit is 125 ms, one interval at 8 QPS. A failed timing gate invalidates qualification even if responses succeed.
   - **Decision:** report each arm’s largest passing tested rate. Compare candidate/control p90 and p95 only for complete, valid matched cells, including scheduled-response tails. Capacity drops are valid overload observations, not a broken campaign closeout.

   Extend `validate_config()` with explicit serial/offered branches. This also lets the unchanged publication helper validate the offered config. Include the scheduler and its imported modules in the offered runtime code-hash closure.

4. **Reuse publication, bootstrap and saved-result auditing.**

   Modify [`scripts/launch_native_semantic_router_cold_spot.py`](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/launch_native_semantic_router_cold_spot.py:197) to expose `--offered aNNNN`, selecting a fixed offered campaign descriptor. Pass config path, schema and artifact roster explicitly through qualification/bootstrap/collection; remote stage/publication use the qualified config path.

   Reuse the existing four publication receipts, ABI qualification, native downloads, Spot launch, terminal authentication and owned-instance termination. Give offered runs separate prefixes and output directories. Upload closed cell bodies and authenticated terminal markers outside cell timers, before the next cell; an interrupted partial cell is discarded and restarted under a new attempt. Collection must accept an authenticated completed overload run even when an attainment gate fails.

   Modify [`scripts/check_native_semantic_router_stats.py`](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/check_native_semantic_router_stats.py:325) so `check_saved()` selects the offered ledger/reducer, revalidates every success and available failure receipt, and checks schedules, port reuse, concurrency, denominators and summary parity. Include the eight consumed input bodies in the artifact roster for offline replay.

   Preserve the exact 399-file native manifest, HTTP/publisher binaries, proof and completed assurance. Python changes refresh controller/runtime hashes; they do not require a native rebuild. Historical a0001–a0004 artifacts remain immutable.

**Principal risks:** six processes mean up to 24 Tokio workers sharing CPUs 0–3; contention and capacity drops are expected outcomes. Six 512 MiB admissions do not establish aggregate memory safety—the cgroup gate is essential. The nominal offer windows total **2016 seconds**, before preparation, drain and uploads; worst-case drain can exceed the current 3000-second worker/3600-second machine limits. Reserve 90 seconds before the worker deadline for cleanup, mark remaining positions aborted, and fail qualification without expanding caps. Also check generated user-data remains below 16,384 bytes after adding artifacts.

**One bounded falsifier:** extend the semantic runner’s existing synthetic fixtures with `--offered-self-check`:

```bash
timeout --kill-after=2 30 env PYTHONDONTWRITEBYTECODE=1 \
  python3 -m scripts.run_native_semantic_router_cold --offered-self-check
```

Use real scheduler threads and mocked, port-keyed native/HTTP boundaries. Require six simultaneous calls to reach a barrier within two seconds; hold cleanup across the seventh offer and require a capacity drop. Assert no port reuse before cleanup, peak ownership exactly six, unchanged parent globals/environment/argv, 64 terminal rows, and full-span throughput including cleanup. Inject wrong authority/port, an extra transport submission, RSS above the cap, cleanup failure, and 607 versus 608 hits: each must produce the prescribed failure/abort or quality result. Force drops and verify offered-population tails cannot become passing survivor tails. Exercise all 24 synthetic cells to assert the 1536-position roster.

This checks concurrency and accounting without native execution. Root retains protocol freeze, integration, AWS launch and final evidence review. No files were edited or experiments run.

## Root decisions

Use explicit per-call hooks and the existing absolute-time scheduler; no lock across service calls and no per-offer Python subprocess. The seam is already assigned to the single working implementation child; do not duplicate it. Keep the offered execution in the existing semantic runner, with separate authenticated schema/config and an offered-specific reducer/verifier. Retain failed/drop/aborted population denominators, bound dispatch lateness to125ms before launch, preserve lower passing rates, aggregate cgroup/OOM and cleanup proof. Stop admissions90s before worker deadline to drain within existing limits. Frozen8QPS attainment requires all offers successful and >=95% meanR10 on both datasets; published444ms context is not a100k universal threshold. No native rebuild or architecture/reviewer cycle.

