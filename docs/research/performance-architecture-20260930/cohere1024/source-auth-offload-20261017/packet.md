# SOURCE authentication offload: source and prospective screen packet

Status: SOURCE UNVERIFIED. Tests are authored, UNRUN. No native, Cargo, rustc,
HTTP/data query, experiment, infrastructure, merge or push action was performed
by this worker. Parent owns all execution, scientific gates and integration.

Base: 634c0691d633528f26c3edc135f351c48ae5da97. Source identity is the containing
commit; exact commit, file SHA256s and static receipts are in the remote handoff.
Worktree: /mnt/borsuk-workspace/source-auth-offload-source-a0001 on Causality
i-0437b3ad203c05b24. Early contract and receipts:
 /mnt/borsuk-workspace/evidence/source-auth-offload-source-a0001.

## Decision and scope

The closed diagnostic recorded 32 executions, eight workloads repeated four
times, exact output/plan/score/charge parity, SOURCE authentication CPU 366 ms
and SOURCE wall 1322 ms. Trace-on wall was about 4.9% higher. Application gap
overlap is NOT evidence that socket bytes were ready but unpolled, and subtracting
authentication CPU from wall would not predict an observed gain. No offload gain
has been demonstrated.

Strict runner selector: omitted or "source_authentication":"inline" is control;
"source_authentication":"offload2" is the candidate. Null, numbers, objects and
other strings reject. Library API:
TwoBitGeneration::with_source_authentication(SourceAuthentication::Offload2).
The default remains inline. Download concurrency stays independently configured;
the proposed screen holds fetch_parallelism=64 in both arms. No routing,
persistent layout, nomination, score, retry, cache policy or SHA provider changes.

Flat SOURCE and remote capsule SOURCE use the same existing ordered drain.
Public SQ8 reads and SQ8 split/direct reads always retain inline authentication.
Local capsule serving also stays inline; offload configuration requires remote
SOURCE. Both cache misses and uncached flat SOURCE use the policy; cache hits
continue to return their generation-pinned authenticated bytes.

## Lifetime, backpressure and memory contract

Each candidate SOURCE batch owns exactly two dedicated 1 MiB-stack worker
threads. A two-permit semaphore bounds the TOTAL queued/running jobs, with a
bounded channel of capacity two. This private SOURCE helper has one operation:
authenticate an owned response; it is not a public executor or configurable pool.
No spawn_blocking, unsafe, borrowed lifetime extension or detached task is used.

Transport permits are independent. At most D downloads and at most two
authentication jobs coexist; at most min(planned_ranges,D+2) response futures
are scheduled. A completed body waiting for compute retains its future and
byte-window permit, applying backpressure. Pending downloads continue to be
polled while compute is held. Byte-window permits span download and authentication.
Bodies move into jobs; no additional body copy is made for flat SOURCE. Capsule
collection retains its existing, already admitted Bytes-to-Vec copy.

Jobs own Arc pins to the original authority and, for capsules, the exact
directory. Capsule jobs run the SAME framing and original-unit verifier as
inline. An opaque verified token is rebound only to matching pins, physical
range and plan charge. Returned proof views borrow the live query plan as before.

All originally planned ranges, including those beyond the initial download
window, drain after an error. Outcomes are sorted by original plan index before
charges, result order and earliest-error selection. Authentication failure
does not stop transport refill. Ranking waits for SOURCE success and full drain.
Probes move with their response and are measured on the worker doing the
authentication; timestamps/CPU values can differ, logical trace content cannot.

The worker owner closes the channel and JOINS every thread on return, error and
future cancellation. Cancellation may synchronously wait for at most the bounded
owned authentication jobs. It does not finish queued unstarted downloads; those
futures are dropped, matching existing cancellation behavior. No computation or
generation pin escapes query destruction. A verifier panic is converted to a
range failure so a worker cannot strand the remaining drain.

Existing full payload, capsule-copy, descriptor and ordinary scheduler budgets
remain charged. Shared authority/directory allocation headers add 128 bytes to
paged generation admission in BOTH arms. Candidate adds, per active query:

    min(max_source_gets, max_parallel_source_gets+2) * 4096
    + 2 * (1048576 + 4096) + 4096

and 256 bytes per configured generation. The 4096-byte pending-future allowance
is checked against the actual instantiated production future in debug/test
builds. Jobs, oneshots, channel/controller state and allocator allowance coexist
with retained payload; there is no credit for presumed early release. With
D=64, source cap128 and one active query, the extra candidate charge is
2,380,032 bytes. SourceAuthentication::additional_memory_bytes is the checked
common calculation used by admission and runner reporting.

Runner admission/startup record source_authentication_extra_library_bytes
separately from the existing ordinary range-state report. Identity and
bound_inputs record the exact selector. Thread runtime/TLS/guard-page and
allocator RSS still require the parent's real resource gate; this is a payload
and explicit-state model, not an asserted whole-process RSS bound.

## Authored tests: all UNRUN

Library exact test names:

- sq8_s3_range::tests::source_auth_offload_parity_and_ordered_errors
  compares actual flat engines: bodies, logical trace, charges, both ordered
  auth/transport error permutations, nine requests beyond a download window of
  two, full drain, restored byte permits and released generation pins.
- sq8_s3_range::tests::source_auth_offload_polls_downloads_and_bounds_compute
  uses the production capsule transport/drain and real original-unit verifier:
  two held compute jobs plus four unfinished body futures, then all four bodies
  complete while compute remains held; refill is bounded at six, all nine drain.
- sq8_s3_range::tests::source_auth_offload_cancellation_joins_workers
  cancels that production engine with compute and downloads live, requires zero
  live worker ownership/body futures, restored permits and released pins.
- two_bit_generation::source_walk_tests::source_auth_offload_generation_parity_and_admission
  checks actual build/open/search, exact plans/plan traces/results/score bits,
  SOURCE and split SQ8 charges, direct zero-SOURCE behavior, source corruption,
  exact candidate model and model-minus-one refusal with zero I/O.
- two_bit_generation::source_walk_tests::source_auth_offload_capsule_parity_and_failures
  applies the same end-to-end checks to genuine repackaged capsules and ensures
  failed SOURCE never starts SQ8.
- source_capsule::tests::owned_span_authentication_preserves_pins_ranges_and_original_unit_checks
  compares inline/owned proof bytes, rejects rebound directory pins, unplanned
  offsets, corruption and inadequate proof memory.

Runner exact test:
tests::source_authentication_selector_is_strict
(target check_cohere_native_baseline), including unchanged download64 and
checked selector memory reporting.

Keep existing focused gates, especially range_future_and_outcome_layout_stay_within_the_modeled_allowance,
capsule_transport_future_is_send_and_fits_scheduler_allowance,
page_range_v2_capsule_http_queued_errors_full_drain_and_permits,
page_range_v2_capsule_http_disjoint_original_units_and_drain,
CPU-profile flat/capsule/split success/failure and trace admission/reuse,
shared byte-window and SOURCE cache tests. The actual capsule future is checked
at its generic production instantiation, including the offload callback.

## Parent qualification gate

Freeze this exact source; compile the affected lib and runner test targets, run
the exact tests above and affected existing tests, and build the exact release
runner. Then pass:

    cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
    BORSUK_TEST_BUILD_JOBS=8 CARGO_BUILD_JOBS=8 bash scripts/check_rust_test_build.sh

The workspace test build must use the real tools with the shim unset. Parent
owns resource guards and original job receipts. Formatting is not compilation,
and no test or performance result is implied by this packet. Keep this SAME
worker for compiler feedback; no replacement or extra review chain.

## Proposed paired screen; no execution authorization

This is a prospective proposal for the parent to freeze or revise BEFORE launch.
It does not amend the closed diagnostic's spent 50-execution allowance.

Use the same eight closed-diagnostic workload units and exact source/query/
truth/generation/layout pins, one frozen release binary, download64, identical
byte windows, logical budgets, transport/retry settings and CPU/RSS ceilings.
Only source_authentication differs. Preserve config hashes and every command,
original job handle and terminal receipt. No fetch128 or SHA-provider repeat.

Proposed new budget: 16 authenticated-input admissions (eight units by two arms),
two disposable canaries (one per arm), 32 trace-OFF measurements (two repetitions
per arm per workload), and 16 trace-ON diagnostic measurements (one per arm per
workload): 66 total native invocations. Parent must authorize the new budget and
runtime envelopes; this packet launches none. Reuse fresh unchanged evidence
where the parent establishes that the same exact inputs and source are covered.

For trace-OFF, alternate ABBA and BAAB order by workload, A=inline, B=offload2.
Use fresh processes with the same cache/connection startup protocol in each arm.
Run trace-ON diagnostics separately and never pool them into latency estimates.
Include worker creation/teardown in timed SOURCE/query phases. Capture process
CPU, query/SOURCE wall, peak RSS, returned IDs+score bits, exact plans/plan traces,
all logical charges, physical attempts/consumed bytes and selector/model fields.

Validity first: exact result/score/plan/logical-charge parity for EVERY pair;
same generation pins and successful original-unit authentication; no transport
failures/retries or extra payload; no resource breach or surviving workers.
Setup, code, timeout or interrupted runs are INVALID, not algorithm negatives.
Preserve failed attempts and do not silently substitute extra runs.

Suggested screen thresholds for parent preregistration: at least 5% lower summed
trace-OFF query wall, no more than 5% higher summed process CPU, and no workload's
median query wall above 1.10x control, while staying within the unchanged RSS cap.
These are proposed screening tolerances, not inherited historical scientific
gates or production acceptance. Report per-workload ratios and all repetitions;
eight workload units do not establish population tail latency or recall.

If the candidate fails parity/resource/cleanup, it cannot advance. If it shows
no measured benefit, retain inline and close this falsifier without another
fanout/provider experiment. A screen win only warrants a separately authorized
larger confirmation; default adoption remains a parent decision.

Simulated objections, not expert testimony:
runtime predicts thread/queue overhead may erase the potential gain; held-body
progress and paired process CPU/wall falsify that claim. Correctness objects to
out-of-order failure selection, borrowed pins and cancellation; adversarial drain,
pin and cancellation fixtures address those. Retrieval predicts exact unchanged
nomination/rank; any score-bit, plan or logical-charge mismatch invalidates the
candidate. No stakeholder simulation is evidence of a speedup or socket readiness.
