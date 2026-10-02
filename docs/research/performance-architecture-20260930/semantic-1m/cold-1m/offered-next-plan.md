Read-only specialist 8bc6f473c0dc4cfc completed (Sol/XHigh, route382530d3). This plan inspects cfcdb70a. Root checkpoint correction: a0002 has since closed at publication admission, so retained serving publication is still unavailable. Do not launch offered load until a later cold attempt closes and its publication/reference authenticates. No algorithm change or new benchmark was performed by this consultation.

Use **two new Python files**, with root-owned config/authority under `semantic-1m/offered-1m/`. Reuse the scheduler and FIRST1M validators; keep existing campaign code unchanged.

At **cfcdb70a**, `run_native_semantic_router_offered.py` does not exist. Offered support lives in [run_native_semantic_router_cold.py](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/run_native_semantic_router_cold.py:623). Its paired FIRST100k runner cannot directly serve this single-arm v8 FIRST1M gate.

1. **Freeze the retained-input contract after cold closes.** Root supplies authenticated cold terminal/closeout receipts, `publication.json`, `publication-reference.jsonl`, request derivative, sealed `truth.i64`, quality reference and qualified HTTP binary/proof. Pin every body by bytes/SHA, runtime/controller code closure, source archive and unchanged native identity. Reuse the retained namespace, head/root authority and SQ8 key/ETag. Keep the new measurement prefix separate from that namespace. Download only the HTTP executable and small envelopes; no raw-vector reconstruction, SQ8 readback, build, publication or publisher invocation.

2. **Add `scripts/run_native_semantic_1m_offered.py`.** Interface:
   `main(config_path, expected_sha, repo, output, *, on_cell_closed=None)`.
   Reuse [schedule_offers](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/run_native_cold_offered.py:64), plus FIRST1M `validate_query`, `validate_startup`, `transport`, `native_cpu` and `close_native`. Call the existing `cold_call(..., port=port, response_check=..., startup_check=..., post_call=..., spawn=..., env=...)` directly.

   Adapt the small FIRST1M measured-call wrapper: its current helper fixes port8080 and patches shared `cold.stop` inside each call. Install that stop hook **once before scheduling and restore after drain**, following the metadata-waves scoped-hook pattern. Preserve per-call observations and CPU sampling.

   Freeze six rate-major cells: `.25,.5,1,2,4,8`, exactly64 positions each—384 total—six cleanup-owned ports18080–18085, no request queue/retry/replacement. Retain125ms dispatch allowance,45s connect deadline,5s payload timeout,60s native limit,3000s worker/3600s machine envelope and90s cleanup reserve.

3. **Implement a FIRST1M reducer and offline replay in that runtime.** Existing reducers assume paired FIRST100k/v7 or serial port8080. FIRST1M truth is `64×100` little-endian **i64**, and startup includes `router/root.bin`.

   Revalidate raw responses, ordered first-ten reference IDs, exact authority, SOURCE/router/SQ8 plans and successful counters; independently recount sealed-truth hits. Attainment requires all64 successes and **≥608/640** hits, valid dispatch/resource/cleanup evidence. Preserve every capacity drop, error and aborted position. A capacity/transport miss stops higher-rate escalation; identity/resource/cleanup failure stops the campaign. Represent remaining cells explicitly as aborted.

   Report planned64 offers, actually dispatched offers, admitted calls, successful responses and admitted terminal completions separately. Calculate their full-span rates from first scheduled offer through validation, cleanup and drain; unstarted cells remain `UNMEASURED`. Report cold and scheduled-response p50/p90/p95/p99, plus all-offer tails using the existing `UNBOUNDED` convention.

4. **Add `scripts/launch_native_semantic_1m_offered_spot.py`.** Reuse `ids.lifecycle()` and the shared bootstrap/trap, with campaign-specific qualification, replay and artifact roster. Reuse metadata-waves’ `on_cell_closed(marker, paths)` seam: exclusive files, fsync, authenticated marker, then conditional upload of records before summary.

   Enforce native512MiB admission/AS4GiB; shared8GiB, zero swap, CPU200%, Tasks512, existing affinities and thread environment. Validate shared descendant cleanup **after drain**; sibling processes invalidate per-call descendant-subset checks. Capture memory/CPU/PID events and page-cache/anonymous observations; missing IO remains explicitly unmeasured.

   Separate process GET/HEAD/status/consumed-payload observations from logical plans and IMDS traffic. Failed-call missing totals remain unknown. Retain instance identity/lifetime, storage byte-time, request populations, transfer observations and frozen unit-price provenance; disclose billing uncertainty. Caps are not actual cost. No sustainable-QPS or vendor claim follows from these finite cells.

**Smallest falsifier:** one `--self-check`, synthetic bodies/processes only, bounded to10s. Hold six calls through cleanup after their mock responses; the seventh must drop without spawning or POSTing. Release cleanup and verify subsequent port reuse. Replay a384-position synthetic ledger; assert608 passes/607 fails, a drop prevents attainment, and authority, premature-port-reuse, missing-cleanup or cgroup-limit mutations fail. Mock S3 must observe records-before-marker upload. This establishes harness behavior only.

**Exact blockers:** a0002 remains live, so its retained publication/reference authority is unavailable for this gate; root must first close, authenticate and decide that cold campaign. Root must then freeze offered authority/config/prices and confirm read access to retained objects. The paired wrappers and serial adapter require the bounded adaptations above.

Read-only source inspection only; no files edited, live a0002 artifacts inspected, queries issued, builds or experiments started.
