# Bounded canonical row coalescing

The metadata gate passed. At covered-record budgets 16, 32, 64, 128, 256,
512 and 1024, the 1000 authenticated shortlists require respectively
15471, 13170, 11383, 9574, 7789, 6321 and 5257 optimal logical GETs
(control: 16000). Total covered records are 16000, 25798, 50837, 101248,
200471, 373169 and 655447. All bridge bytes are charged. This is a cost
counterfactual on one sealed repetition, not measured speed or a selected
performance envelope.

The native result schema is borsuk-cohere-native-baseline-result-v21.
Both arms require canonical_stage ranges, covered_records and bridge_records.
The strict reader rejects old result versions and missing fields through the
actual writer with INVALID and complete=false. There is no redundant stage
marker or legacy reader. Configuration and canonical index layout are unchanged.

Set serving.mode=fp32_rerank and serving.coalesce=true to opt in.
Omitting coalesce or setting false keeps individual-row GETs.
max_fp32_bytes bounds total covered bytes, not scored bytes.
max_fp32_gets bounds logical ranges, not transport retry attempts.
The root chooses the later performance envelope; this patch sets no new
performance default. Coalescing requires the shared byte window to admit the
entire configured canonical byte cap before SOURCE/SQ8 I/O. Individual mode
keeps its one-record window admission. No plan is silently split. Public-path
tests cover one byte below this bound, exact acceptance, and individual-mode
acceptance under the same smaller window.

The planner admits sorted unique physical ordinals, checked record/object
geometry and configured caps before allocating its bounded arrays. Joining
each adjacent gap saves one GET; selecting cheapest gaps gives minimum
covered bytes for each GET count. Equal costs join the leftmost gap first.
The native exhaustive test independently enumerates every partition of every
nonempty subset of eight ordinals over byte and GET caps. The metadata script
has 969 separate tiny exhaustive checks, including large ordinal geometry.

All covered canonical rows authenticate against root-pinned row hashes.
Selected rows also retain exact ID/length and scalar f64 validation; bridges
are never scored. HTTP range/length/ETag validation, shared windows, ordered
earliest error, complete drain and cancellation use the existing range reader.
Source nomination, SQ8 plan, scored roster, pending mutation visibility and
admission-before-normalization are retained.

Memory model: three copies of the configured byte cap cover payload collection
and authentication. The per-shortlist 256-byte allowance covers the retained
expanded shortlist/sorted nominee copy, ordinal array, gap pairs, join flags,
full range capacity, grouped hit payload and flattened/merge hit payload.
Outcome/result descriptors use the scheduler's actual generic Vec<Hit> size
and existing growth bound; active futures use the scheduler allowance.
Normalized vectors, fixed slack, startup metadata, row hashes, retained pins,
shared-window state and every active query remain included. The strict reader
calls the same complete remote-memory model and rejects resealed mismatches.

Caller audit:
- fp32_rerank: checked planner, selected/bridge authentication, modeled payload.
- two_bit_generation: only the canonical fetch plan changes; ordered ranges
  and charges are returned, while SOURCE/SQ8 and scalar ranking stay intact.
- two_bit_index: empty/pending-only results expose an empty canonical plan.
- check_cohere_native_baseline: explicit opt-in config and serialized physical
  ranges, covered and bridge records.
- compare_native_replay/fp32_serving: reconstructs the deterministic plan from
  physical ordinals, validates exact ranges/charges/caps and complete memory.
- compare_native_replay/current_v10: existing FP32 dispatch and model call
  already cover this selector; no additional branch is needed.
- generic src/index.rs: no FP32 call site.

Original affected validation before review:
- cargo test --locked -p borsuk --lib fp32 -- --nocapture: 10 passed.
- cargo test --locked -p borsuk --example compare_native_replay fp32 -- --nocapture:
  4 passed, including native producer through strict reader and actual writer.
- HTTP coverage includes scalar-oracle parity, selected and bridge corruption,
  strong ETag/range/object size/body length faults, reversed completion order,
  full drain, cancellation/window restoration and retry-vs-logical GET caps.
- Admission tests cover one byte below/exact modeled memory and slot acquisition
  before normalization. Planner tests cover caps, tails, ties and overflow.
- Coalesced writer tests accept actual native evidence and refuse resealed range,
  bridge, coverage, charge, shortlist and memory-model mutations.

The first affected compile (a0001) failed on a moved Rust error value; its log
and exit receipt are retained. Corrected affected controller a0002 exited 0.
The original workspace Clippy, actual complete test-build script and release
runner/replay build also passed. Their receipts and original frozen patch are
preserved. Both full parent-owned reviews (group 73f71ce41b88457b) were read;
the combined repair versions the native result, tightens early window admission,
and extends the existing mutation fixture in both modes. It verifies deleted
and updated bridge rows, underfilled and empty visible base rosters, and pending
merge parity with exact ID/ordinal/f64 bits. Final affected, workspace assurance
and release checks on the repaired freeze are recorded in the final handoff.
Memory accounting is source-reviewed; no allocator or RSS proof is claimed.

No corpus/request/truth inputs were opened by the metadata diagnostic.
Native tests use synthetic fixture bytes only. No real ANN/service experiment,
S3 call, infrastructure change, timer extension, child, review, push or merge
was launched by this worker.
