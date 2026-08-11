# Task 3 report — unified public mutation facade on V12 positioned commits

## Scope delivered

- `BorsukIndex::append_positioned_mutation(CanonicalMutationBatch)` is the
  durable positioned append interface. Ordinary add/put/upsert/delete,
  generated-ID writes, collection transactions, and grouped writes prepare one
  canonical batch and publish one V12 positioned transaction.
- `GroupCommitWriter` is now a process-local batching facade. It preserves each
  caller batch intact, batches by delay/record thresholds across bounded worker
  queues, reports the actual positioned source position/checksum/encoded bytes/
  request counts, and implements `drain` as a write-free barrier.
- The primary typed payload carries primary dense rows and the owned named,
  sparse/text, and late-interaction values. Named dense and late-interaction
  children are deterministic read-time projections of that root payload, so 65
  named modalities do not create more than the positioned 64-payload bound.
- ID-directory changes are bundled into one typed payload. Cross-modality
  mutation states are merged deterministically and stored in one typed Parquet
  tombstone table. Typed transaction metadata stores canonical per-modality
  headers and sorted unique nonzero BM25 term deltas.
- Generated IDs are transaction/ordinal-derived collision-resistant BLAKE3 IDs;
  the foreground path no longer reads or CASes `id-directory/generated/NEXT`.
- Exact inserts temporarily retain the existing claim pages, but the positioned
  head CAS is their root authorization. Grouped upserts and derived named
  writes are claim-free. A release ambiguity writes one create-only,
  exact-idempotent root authorization receipt keyed by transaction digest.
- Reopen and refresh reload the positioned snapshot and reconstruct live root
  and child overlays without publishing a V11 collection/manifest mutation.

## TDD and failure evidence

All Cargo gates used this isolated environment:

```text
CARGO_TARGET_DIR=/home/rb/worktrees/borsuk-prod-ready-v9/target-positioned-v12
RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper
SCCACHE_DIR=/data/cache/sccache
CARGO_BUILD_JOBS=2
CARGO_INCREMENTAL=0
RUSTFLAGS='-C codegen-units=8'
```

The initial exact cross-facade gate passed after correcting its test-only search
call to the repository's report-returning API:

```text
cargo test --locked -p borsuk --test group_commit ordinary_and_group_writes_share_one_positioned_protocol -- --exact --nocapture
1 passed; 10 filtered out; 0 failed; 0.05s
```

The first full integration RED was:

```text
cargo test --locked -p borsuk --test group_commit -- --nocapture
12 passed; 2 failed; 0 ignored; 1.10s
```

Both failures were root-path projection defects: named child storage prefixed
canonical root payload paths with `vectors/<name>/`. Tombstone projections and
positioned record projections were changed to read through root
`collection_storage`, while ordinary child-owned WAL reads remain on child
`self.storage`.

The next full integration RED was:

```text
cargo test --locked -p borsuk --test group_commit -- --nocapture
13 passed; 1 failed; 0 ignored; 1.48s
```

The failure was
`late_interaction_replacement_and_delete_reopen_from_one_positioned_log` at
`crates/borsuk/tests/group_commit.rs:515`: root and child projections shared one decoded-run cache
key even though they produce different logical records. Projected cache keys
now include the projection-metadata digest. Full output for this RED is
`/home/rb/.local/share/rtk/tee/1786372697_cargo_test.log`.

The focused regression then passed:

```text
cargo test --locked -p borsuk --test group_commit late_interaction_replacement_and_delete_reopen_from_one_positioned_log -- --exact --nocapture
1 passed; 13 filtered out; 0 failed; 0.09s
```

## Fresh final GREEN evidence

The final full Task 3 integration target was run exactly once after the focused
regression, with the isolated environment above:

```text
cargo test --locked -p borsuk --test group_commit -- --nocapture
14 passed; 0 failed; 1.41s
```

`git diff --check` also exited zero after the final local fix. No other Cargo
target was run after the final integration gate.

## Request bounds and claim-recovery coverage

The green integration target proves:

- exactly two positioned-head PUTs for one ordinary plus one grouped append,
  with zero lane-log and cell-WAL commit objects;
- one caller's 128 records receive one indivisible positioned source position;
- 1, 8, and 32 concurrent producers converge after reopen;
- 65 ID partitions publish one head update, one ID-directory payload, and at
  most 64 total payload references;
- 65 named modalities plus replacement tombstones remain at most 64 payloads
  per transaction and reopen correctly;
- grouped upserts create zero exact-ID claim, legacy transaction, or positioned
  claim-authorization objects;
- `drain` performs zero PUTs;
- accepted-but-reported-failed claim release under two concurrent exact adds
  yields exactly one successful insert, one durable authorization receipt, one
  duplicate rejection, and one live record after reopen;
- ordinary duplicate rejection, grouped last-write-wins, generated IDs,
  ordinary upsert/delete, text/BM25 corrections, late-interaction replacement,
  delete, and reopen semantics.

Additional unit regressions were added in `cell_wal.rs` for exact abort
restoration, epoch-scoped/idempotent authorization, exact normal request
counts, one authorization resolution across all 22 claim pages, the positioned
CAS-to-release crash gap with receipt backfill, and corrupt receipt fail-closed
behavior. The typed transaction-metadata round trip also has a `format.rs` unit
test. Those private unit tests were not run in this task's authorized gate set;
they are listed under concerns rather than claimed as fresh evidence.

## Root-storage and projection invariants

- Canonical positioned payload and envelope paths are root-relative and are
  always read/written through collection storage.
- Tombstone projection marker paths carry the authoritative root payload path,
  checksum, and modality; named children filter the root typed table after a
  checksum-verified collection-storage read.
- Positioned named record projections read the root record payload through
  collection storage. Ordinary non-projected child WAL still reads through the
  child namespace.
- Root, dense-child, and late-token decoded views of one content payload cannot
  alias: projection metadata contributes to the shared decoded-cache identity.
- The current authority epoch is the stable explicit initial epoch `1` across
  positioned writer open/create, claim guards, authorization validation, and
  head-based recovery. It is not inferred from mutable manifest generations.

## Forbidden legacy paths

The integration operation log asserts zero foreground writes under the relevant
facades to:

- `lane-log/`
- `cell-wal/` and `cells/<...>/wal/`
- `transactions/`
- `tombstones/`
- `bm25/` and `lexical/stats-delta/`
- `id-directory/generated/NEXT`

Text and late-interaction replacement/delete tests repeat the modality-specific
legacy-path assertions and verify reopen behavior from positioned truth.

## Task 4 handoff obligation

Task 4 owns materialization/checkpointing. Before a pending/recent positioned
receipt can be evicted from the bounded head window, Task 4 must durably
backfill an exact authorization receipt for every lingering exact-ID claim owner
and/or release that owner to the envelope checksum. Otherwise a crash-delayed
claim whose transaction has left both `pending` and `recent` would no longer be
recoverable from the current head. This ordering is a correctness gate, not an
optional cleanup optimization.

Task 4 must also consume the typed metadata/tombstone/record projections into
the atomic materialized V12 roots without reintroducing foreground V11 writes.

## Known concerns and self-review

- The brief's full `cell_wal` and `named_vectors` integration targets were not
  authorized/run, so Task 3 is `DONE_WITH_CONCERNS` despite the complete green
  `group_commit` target. The new private claim and metadata unit tests likewise
  lack fresh execution evidence.
- There is no separate public test named for collection-transaction retry; the
  public mutation methods do exercise the internal collection transaction, and
  the concurrent release-loss test covers the load-bearing exact-add ambiguity,
  but the broader brief wording remains only partially evidenced.
- No broad workspace suite, clippy gate, full formatting gate, AWS action, or
  production benchmark was run. The replacement example is a positioned smoke
  harness; Task 7/9 still own preregistered performance qualification and final
  removal of unreachable legacy lane/cell protocol code.
- A future source-epoch transition must still publish a collection-level epoch
  marker. Fix round 1 partitions authorization receipts by source epoch and
  validates the referenced envelope exactly.
- No V11 manifest/layout marker or compatibility reader was changed. No push
  was performed.

Static self-review found and closed both projection-specific correctness bugs
above. The final diff is whitespace-clean, the final scoped integration target
is green, and the remaining concerns are explicit test/scope and downstream
Task 4/7/9 obligations rather than known failures in the executed target.

## Full-suite follow-up — run-local global-leaf deduplication

The full library gate recorded in
`/home/rb/.local/share/rtk/tee/1786390763_cargo_test.log` exposed a deterministic
regression in
`global_pq_sidecar::tests::bounded_leaf_ranking_honours_every_production_budget_and_selected_cells`:
at the production page budget of eight, one duplicated run-local leaf reference
consumed budget, yielding eight routed entries but only seven logical pages.

The original V11 ranker deduplicated page references, but that filter was
removed wholesale when equal `(cell, leaf, bundle, offset)` coordinates from
different runs needed to remain distinct. The fused-run representation now
provides the missing discriminator. `rank_pages` therefore restores
deduplication using `(run_ordinal, cell, leaf, bundle path, batch offset)` while
retaining the selected-cell filter and one global page budget. The adjacent
cross-run regression now explicitly asserts that both run ordinals survive.

The existing full-gate failure is the TDD RED. After the shared Cargo lane was
released, both focused regressions passed under the isolated environment above:

```text
cargo test --locked -p borsuk --lib \
  global_pq_sidecar::tests::bounded_leaf_ranking_honours_every_production_budget_and_selected_cells \
  -- --exact --nocapture
1 passed; 519 filtered out; 0 failed; 0.04s

cargo test --locked -p borsuk --lib \
  global_pq_sidecar::tests::v11_page_ranking_keeps_distinct_run_local_pages_with_equal_coordinates \
  -- --exact --nocapture
1 passed; 519 filtered out; 0 failed; 0.03s
```

`rustfmt --edition 2024 --check crates/borsuk/src/global_pq_sidecar.rs` and
scoped `git diff --check` both exited zero. No broader Cargo target was run.

## Review fix round 1/5 — committed cleanup, authority, seam, telemetry, API, construction

This round addressed all six Important review findings with one narrow Cargo
command at a time. Every command used the isolated environment recorded above;
no full integration target, broad suite, clippy, or formatting command ran.

### 1. Irreversible commit with failed claim cleanup

RED:

```text
cargo test --locked -p borsuk --test group_commit committed_cleanup_failure_clears_transaction_and_reports_position -- --exact --nocapture
exit 101: E0599, PositionedCommitCleanupFailed did not exist
log: /home/rb/.local/share/rtk/tee/1786374046_cargo_test.log
```

The positioned CAS is now treated as irreversible. Claim finalization marks the
guard committed before release, and transaction completion captures cleanup
failure without leaving the active collection transaction installed. The new
`PositionedCommitCleanupFailed` error carries source epoch, shard, sequence,
envelope checksum, and the cleanup failure.

GREEN before and after the later append-seam refactor:

```text
cargo test --locked -p borsuk --test group_commit committed_cleanup_failure_clears_transaction_and_reports_position -- --exact --nocapture
1 passed; 14 filtered out; 0.05s

cargo test --locked -p borsuk --test group_commit committed_cleanup_failure_clears_transaction_and_reports_position -- --exact --nocapture
1 passed; 16 filtered out; 0.05s
```

The regression injects an accepted-but-reported-failed claim release followed
by an authorization-receipt PUT failure. It verifies the explicit committed
error identity, same-handle reuse, visibility of the committed row after
reopen, and duplicate rejection/recovery.

### 2. Epoch namespace and exact authorization-envelope validation

Epoch namespace RED/GREEN:

```text
cargo test --locked -p borsuk --lib cell_wal::tests::positioned_claim_authorization_is_idempotent_and_epoch_scoped -- --exact --nocapture
exit 101: five E0061 errors because claim_authorization_path lacked source_epoch
log: /home/rb/.local/share/rtk/tee/1786374212_cargo_test.log

cargo test --locked -p borsuk --lib cell_wal::tests::positioned_claim_authorization_is_idempotent_and_epoch_scoped -- --exact --nocapture
1 passed; 589 filtered out; 0.00s
```

Receipt paths are now
`positioned-log/claim-authorizations/<source_epoch>/<digest-prefix>/<digest>.json`,
so the same transaction ID cannot collide across source incarnations.

Exact envelope RED/GREEN:

```text
cargo test --locked -p borsuk --lib cell_wal::tests::positioned_claim_authorization_requires_exact_envelope_identity -- --exact --nocapture
exit 101: wrong receipt sequence was accepted
log: /home/rb/.local/share/rtk/tee/1786374347_cargo_test.log

cargo test --locked -p borsuk --lib cell_wal::tests::positioned_claim_authorization_requires_exact_envelope_identity -- --exact --nocapture
exit 101: valid receipt used 1 GET, expected receipt + envelope = 2
log: /home/rb/.local/share/rtk/tee/1786374434_cargo_test.log

cargo test --locked -p borsuk --lib cell_wal::tests::positioned_claim_authorization_requires_exact_envelope_identity -- --exact --nocapture
1 passed; 590 filtered out; 0.01s
```

Receipt acceptance now reads the checksum-addressed canonical envelope fresh,
verifies its content checksum, decodes/validates its canonical digests and
totals, and requires exact transaction ID, source epoch, shard, and sequence.
Wrong sequence, another transaction's envelope, missing envelope, corrupt
envelope, malformed receipt, and conflicting receipt identity fail closed.

The rare wide-owner request bound was updated from claim pages + one receipt
GET to claim pages + receipt + envelope:

```text
cargo test --locked -p borsuk --lib cell_wal::tests::one_authorized_owner_across_twenty_two_pages_resolves_once -- --exact --nocapture
1 passed; 590 filtered out; 0.01s
```

Normal successful release still writes no authorization receipt and gains no
request. Task 4's pre-eviction backfill/release obligation remains unchanged.

### 3. One publication seam and no mutable receipt side channel

RED/GREEN:

```text
cargo test --locked -p borsuk --test group_commit facades_return_current_seam_receipt_and_group_failure_cannot_reuse_one -- --exact --nocapture
exit 101: three E0609 errors for absent AddReport positioned receipt fields
log: /home/rb/.local/share/rtk/tee/1786374674_cargo_test.log

cargo test --locked -p borsuk --test group_commit facades_return_current_seam_receipt_and_group_failure_cannot_reuse_one -- --exact --nocapture
1 passed; 15 filtered out; 0.05s
```

`append_positioned_mutation(CanonicalMutationBatch) -> Result<AddReport>` now
contains the publication body itself. Its private durable sibling was removed.
`AddReport` carries the current positioned source position, envelope checksum,
and encoded bytes. Ordinary collection completion and grouped upsert both
consume this report. `last_positioned_commit` and `take_last_positioned_commit`
were removed, so a grouped failure cannot observe a stale prior receipt.

### 4. Exact request telemetry without loss or duplication

The telemetry regression compares every returned request field with the real
operation log for caller-ID add, generated-ID add, upsert, delete, and grouped
append.

RED progression:

```text
cargo test --locked -p borsuk --test group_commit mutation_facades_report_each_physical_request_exactly_once -- --exact --nocapture
exit 101: E0624, upsert_with_report was not public
log: /home/rb/.local/share/rtk/tee/1786374919_cargo_test.log

cargo test --locked -p borsuk --test group_commit mutation_facades_report_each_physical_request_exactly_once -- --exact --nocapture
exit 101: upsert reported {gets: 0, puts: 6}, physical was {gets: 2, puts: 6}
log: /home/rb/.local/share/rtk/tee/1786374960_cargo_test.log

cargo test --locked -p borsuk --test group_commit mutation_facades_report_each_physical_request_exactly_once -- --exact --nocapture
exit 101: grouped append reported {gets: 2, puts: 0}, physical was {gets: 2, puts: 6}
log: /home/rb/.local/share/rtk/tee/1786375051_cargo_test.log
```

Preparation plus publication are now measured from the facade's root request
scope. Each independent group worker rebinds its cloned positioned writer to
that worker's request-counter scope while retaining the shared pinned heads.
This includes positioned requests once without adding the nested writer delta a
second time.

GREEN:

```text
cargo test --locked -p borsuk --test group_commit mutation_facades_report_each_physical_request_exactly_once -- --exact --nocapture
1 passed; 16 filtered out; 0.06s
```

### 5. No public legacy CellWal durability API

Public API policy RED/GREEN:

```text
cargo test --locked -p borsuk --doc public_api_policy -- --nocapture
exit 101: compile_fail block compiled because legacy CellWal durability types were public
log: /home/rb/.local/share/rtk/tee/1786375210_cargo_test.log

cargo test --locked -p borsuk --doc public_api_policy -- --nocapture
1 passed; 3 filtered out; 0.15s
```

`CellWalStore`, run inputs/object paths, prepared/committed transaction types,
transaction ID helper, and prepare/commit/snapshot methods are no longer
reachable from the public crate API. Public claim configuration and logical
cell identity remain available where existing index configuration requires
them. The compile-fail policy test prevents the durable alternative from being
re-exported.

The obsolete external legacy-protocol target was not disabled. It was replaced
with a real two-test `tests/cell_wal.rs` public-facade target covering bounded
claim configuration and exact-add claim coordination with positioned-only
durability/reopen. The stale crash-recovery test that directly prepared an
uncommitted legacy transaction was deleted; equivalent losing-envelope
invisibility belongs to positioned-log coverage.

One exact new integration case was run:

```text
cargo test --locked -p borsuk --test cell_wal public_exact_add_uses_claims_but_only_positioned_commit_durability -- --exact --nocapture
initial fixture RED: 0 passed; 1 failed; assertion incorrectly forbade claim STATE
log: /home/rb/.local/share/rtk/tee/1786375359_cargo_test.log

cargo test --locked -p borsuk --test cell_wal public_exact_add_uses_claims_but_only_positioned_commit_durability -- --exact --nocapture
1 passed; 1 filtered out; 0.03s
```

The corrected assertion permits claim-owner STATE coordination but forbids
legacy transaction COMMIT/descriptors, cell-WAL paths, and cell-local WAL runs.
No `cfg(any())` or other test hiding remains.

### 6. Partial GroupCommitWriter construction cannot deadlock

RED/GREEN:

```text
cargo test --locked -p borsuk --lib group_commit::tests::partial_worker_spawn_failure_drops_senders_before_joining -- --exact --nocapture
exit 101: E0599, deterministic injected-spawner construction seam was absent
log: /home/rb/.local/share/rtk/tee/1786375443_cargo_test.log

cargo test --locked -p borsuk --lib group_commit::tests::partial_worker_spawn_failure_drops_senders_before_joining -- --exact --nocapture
intermediate exit 101: E0282 required an explicit JoinHandle vector type
log: /home/rb/.local/share/rtk/tee/1786375478_cargo_test.log

cargo test --locked -p borsuk --lib group_commit::tests::partial_worker_spawn_failure_drops_senders_before_joining -- --exact --nocapture
1 passed; 591 filtered out; 0.01s
```

On a spawn error the constructor now explicitly drops the failed worker's
sender and every previously retained sender before joining started workers.
The regression injects failure at worker 2, uses channel completion rather than
sleep, requires the constructor error to return within the bounded wait, and
asserts that both started workers exited.

### Fix-round self-review and remaining scope

- Static search finds no `last_positioned_commit`, no private positioned append
  sibling, and no `cfg(any())` in source/tests.
- Canonical root payload projection reads and projection-specific decoded-cache
  keys from the initial Task 3 fix remain intact.
- Normal append still uses the positioned writer's two immutable upload waves
  followed by one head CAS. Normal successful exact-claim release creates zero
  authorization receipt; only ambiguous/incomplete release uses the rare
  receipt path and its newly explicit envelope GET.
- `git diff --check` exits zero. Formatting was maintained manually because the
  controller explicitly prohibited running the formatting command this round.
- The full `group_commit`, full two-test `cell_wal`, and `named_vectors`
  integration targets remain intentionally unrun in this narrow fix turn, as do
  clippy, broad suites, AWS, and benchmarks. Final status therefore remains
  `DONE_WITH_CONCERNS` pending the controller's later broad-gate authorization.

## Controller verification and quality cleanup after fix rounds 1–2

Fresh controller-owned gates after the reviewed fixes produced:

```text
cargo test --locked -p borsuk --test group_commit -- --nocapture
17 passed; 0 failed

cargo test --locked -p borsuk --test cell_wal -- --nocapture
2 passed; 0 failed

cargo test --locked -p borsuk --test named_vectors -- --nocapture
initially 9 passed; 2 failed
```

The two named-vector failures showed that flush bypassed the metadata-aware
positioned projection loader and read a root payload through named child
storage. Fix round 2 routed flush entries through `load_wal_tail_records`.
Its focused regression passed 1/1, independent scoped re-review marked the
finding ADDRESSED with no new Critical/Important breakage, and the complete
target then passed:

```text
cargo test --locked -p borsuk --test named_vectors -- --nocapture
11 passed; 0 failed

cargo test --locked -p borsuk --lib cell_wal::tests:: -- --nocapture
18 passed; 574 filtered; 0 failed

cargo test --locked -p borsuk --lib format::tests::positioned_transaction_metadata_round_trips_exact_header_and_terms -- --exact --nocapture
1 passed; 591 filtered; 0 failed
```

Two additional public-facade characterization tests cover an accepted but
reported-failed positioned head PUT for generated-ID add and grouped append.
Both passed immediately (1/1 each), proving stable returned IDs/receipts, one
visible two-row transaction after reopen, exact request telemetry, and no
legacy writes.

## Review fix round 3/5 — delete the unreachable architecture

Privatizing the obsolete durable CellWal API exposed 33 dead-code warnings.
They were treated as architectural evidence, not suppressed. A full call-site
trace removed unreachable writer-side CellWal prepare/commit/frontier-marker
publication, collection-WAL reservation/commit/pending-write APIs, coordination
counters, lane-log writers/leases/materialization/checkpointing and their
tests, and the lane-bound V11 direct leaf builder/unpublished-run persistence
path and tests. Exact-ID claim STATE/page codecs and positioned recovery remain,
as do still-live read/prune/GC decoders required until Tasks 4 and 8. The group
worker guard is named `_workers` with ownership semantics rather than a dummy
read. No `allow(dead_code)`, `cfg(any())`, or fake call site was added.

The first cleanup Clippy run exposed four mechanical leftovers, and controller
reruns exposed two more unused test imports/helpers; each was deleted without
suppression. Fresh final evidence on the resulting tree is:

```text
cargo fmt --all
cargo fmt --all -- --check
git diff --check
exit 0

cargo clippy --locked -p borsuk --all-targets -- -D warnings
exit 0: cargo clippy: No issues found
```

The deletion wave has not yet received its scoped independent re-review, and
the functional gates must be rerun after it because it changed production
reachability. No completion, commit, push, AWS run, or benchmark claim is made
from this section.

## Fix round 2/5: named positioned projection flush

Controller RED evidence:

```text
cargo test --locked -p borsuk --test named_vectors -- --nocapture
9 passed; 2 failed
failures:
  collection_memory_telemetry_is_shared_across_named_modalities
  mutable_writer_memory_capacities_leave_room_for_manifest_growth
log: /home/rb/.local/share/rtk/tee/1786376014_cargo_test.log
```

Both failures reached `index.flush()` and attempted to resolve the canonical
root payload below the named child's `vectors/lexical/` namespace. Static path
tracing showed that no snapshot transformation drops the projection marker:
both immediate overlay installation and reopen reconstruction clone the primary
run and assign `BPR1` metadata before inserting it in the named child snapshot.
The actual bypass was in `flush_wal_transactions`: unlike read-time tail
loading, it decoded record entries by calling `self.storage` directly. Thus a
correctly marked child projection never reached the metadata-aware root-storage
selection in `load_wal_tail_records`.

The single root-cause fix routes each flush record entry through
`load_wal_tail_records`. Ordinary runs continue to use modality-local storage;
`BPR1` runs use collection-root storage, retain projection-specific cache keys,
and are projected before segment construction.

Requested exact first-failure GREEN (the full target was not rerun):

```text
cargo test --locked -p borsuk --test named_vectors collection_memory_telemetry_is_shared_across_named_modalities -- --exact --nocapture
1 passed; 10 filtered out; 0.13s
```

`git diff --check` exits zero.

### Open quality blocker: 33 compiler warnings

The controller's full-target log reports `borsuk (lib) generated 33 warnings`.
They include newly unreachable privatized CellWal surface (`run`, prepared
transaction construction, writer state, validation/descriptor helpers, and
multiple associated methods), along with legacy collection-WAL, global-leaf,
coordination-counter, lane-materialization, observability, and storage helpers.
They remain unsilenced and unallowed as required. Exact warning inventory:
`/home/rb/.local/share/rtk/tee/1786376014_cargo_test.log`. This is an explicit
quality blocker for a later unreachable-code deletion pass; it was not mixed
into this one functional correction.

Status remains `DONE_WITH_CONCERNS`: the exact requested regression is green,
but the second formerly failing test and full target remain deliberately unrun,
and the 33-warning cleanup is open.

## Public-facade ambiguous-response evidence

Two focused real-operation-log characterization tests now inject a one-shot
accepted-then-retryable-error response on the positioned shard-head PUT. The
collection is created before installing the fault wrapper, so the injected
response applies only to the mutation under test rather than head
initialization.

`generated_add_reconciles_an_accepted_positioned_head_error_once` exercises
`add_vectors_with_report` and requires:

- one successful call with two distinct generated IDs and the current source
  position, envelope checksum, and encoded-byte receipt;
- report request counters exactly equal the physical operation log, including
  the reconciliation read;
- exactly one accepted positioned head PUT and no legacy mutation writes;
- exactly one visible positioned transaction containing one two-row primary
  record batch; and
- the exact returned IDs and vectors visible after reopen.

It passed immediately, so no production change was needed:

```text
cargo test --locked -p borsuk --test group_commit generated_add_reconciles_an_accepted_positioned_head_error_once -- --exact --nocapture
1 passed; 18 filtered out; 0.04s
```

`grouped_append_reconciles_an_accepted_positioned_head_error_once` applies the
same fault through `GroupCommitWriter::append` and requires its current group
receipt, honest physical request telemetry, one head PUT, no legacy mutation
writes, one visible transaction/primary batch, and both records after reopen.
It also passed immediately with no production change:

```text
cargo test --locked -p borsuk --test group_commit grouped_append_reconciles_an_accepted_positioned_head_error_once -- --exact --nocapture
1 passed; 18 filtered out; 0.04s
```

Only these two exact tests were run, one Cargo command at a time. The warning
cleanup and broad gates remain unstarted. `git diff --check` exits zero. Status
remains `DONE_WITH_CONCERNS` solely for the already-recorded warning cleanup and
later controller-owned broad gates.

## Fix round 3/5: obsolete production-path deletion

This round deleted the unreachable write architecture reported by the 33-warning
inventory instead of suppressing it. Removed code includes:

- CellWal prepare/commit/frontier/marker writer entry points, writer-only
  descriptors and transaction builders, and their obsolete unit fixtures;
- collection-WAL reservation/admission/publication plumbing and its writer-only
  tests, while retaining pending/frontier decoders used by current read and GC
  adapters;
- coordination-counter reservation helpers and dummy foreground transaction
  fields;
- lane-log writer, lease, spill, retirement, and materialization entry points,
  their blanket dead-code allowance, and their writer-only tests;
- the V11 direct incremental leaf builder, unpublished-run types, direct
  codebook encoder, future-only publication accessors, and all tests that
  constructed those deleted paths; and
- the unused post-commit observability hook.

The exact-ID claim codecs, positioned authorization/recovery methods, retained
CellWal transaction readers, pruning/GC adapters, and the lane artifact reader
remain because current positioned writes, reopen, refresh, and GC still call
them. `GroupCommitWriter` now names its shared thread guard `_workers`, making
the field's ownership-only purpose explicit without a dummy read.

Stale foreground comments and crash-harness path terminology now describe
positioned heads, envelopes, and canonical positioned payloads rather than
cell-local commit markers. The crash helper selects
`positioned-log/payloads/.../*.parquet` objects.

The required formatting command completed successfully:

```text
cargo fmt --all
exit 0
```

The one permitted constrained Clippy invocation was then run with the isolated
environment recorded above:

```text
cargo clippy --locked -p borsuk --all-targets -- -D warnings
exit 101
```

It reported exactly four cleanup diagnostics: a test referenced the removed
`base` binding, `CollectionWalFrontierHead` and
`collection_wal_frontier_shard` imports were unused, and
`COLLECTION_WAL_RESERVATION_TTL_MS` was dead after reservation deletion. Those
four diagnostics were fixed directly. Per the controller's failed-gate rule,
Clippy was not rerun and no test command was run. Round status is therefore
`DONE_WITH_CONCERNS`: the obsolete paths are deleted and the reported failures
are repaired, but there is intentionally no post-fix green Clippy claim.

## Fix round 4/5: authoritative live-payload corruption target

Fresh controller RED evidence after the obsolete writer-path deletion was:

```text
cargo test --locked -p borsuk --test crash_recovery -- --nocapture
4 passed; 1 failed
failure: byte_mutated_wal_object_is_caught_by_checksum_not_a_wrong_answer
log: /home/rb/.local/share/rtk/tee/1786379569_cargo_test.log
```

The checksum assertion did not fail because production skipped verification.
The test selected the lexicographic median content-addressed positioned payload,
which has no relationship to current read authority. The selected object could
belong to deleted `r0002` or superseded `r0001`, so returning all current live
records without fetching that obsolete payload was correct.

The crash fixture now opens the complete authoritative positioned snapshot,
follows only its primary-dense payload references, verifies each referenced
object against its envelope checksum, and decodes its typed Parquet
`record_id` column. It deterministically corrupts the unique authoritative
payload containing live, never-superseded row `r0000`. The error and no-panic
assertions are unchanged; no production code was modified for this finding.

Fresh focused GREEN with the isolated two-job sccache environment was:

```text
cargo test --locked -p borsuk --test crash_recovery byte_mutated_wal_object_is_caught_by_checksum_not_a_wrong_answer -- --exact --nocapture
1 passed; 4 filtered out; 0 failed; 0.14s
```

No other Cargo target, formatting command, Clippy invocation, broad suite, AWS
action, or benchmark was run in this round.

## Fix round 5/5: positioned fault-boundary retargeting — stopped on production defect

Fresh controller evidence showed six failures in `tests/fault_injection.rs`.
Five injected deleted V11 pending-commit or child-cell-run paths, and the large
segment case placed one ID larger than the positioned append's 64 MiB hard
bound. The accepted-then-error test passed without matching its deleted path.

The test-only redesign follows the current protocol directly:

- permission-denied positioned shard-head PUTs test deterministic pre-authority
  failure without entering the CAS retry loop;
- an immutable envelope PUT failure proves the report facade cannot acknowledge
  before head authority;
- a one-shot immutable payload-wave failure permits content-addressed orphans
  while requiring primary and named modalities to remain jointly invisible;
- accepted-then-error head publication requires exactly one matching PUT, one
  reconciliation GET, one returned current receipt, and one transaction after
  reopen;
- corruption selects the exact `positioned-log/heads/{shard}.json` reported by
  the committed live transaction rather than guessing lexicographically; and
- the multipart fixture uses three separately successful seeded 22 MiB binary
  ID appends intended to materialize into one segment larger than 64 MiB.

Every injected boundary uses `FaultInjectingObjectStore` operation logging.
The no-legacy-authority assertion permits only retained exact-ID claim-owner
`transactions/<id>/STATE` coordination and rejects other legacy transaction,
lane, cell-WAL, collection-WAL, tombstone, and lexical authority writes.

Focused two-job sccache evidence completed before the stop condition:

```text
cargo test --locked -p borsuk --test fault_injection multimodal_collection_transaction_is_invisible_when_root_publication_fails -- --exact --nocapture
1 passed; 11 filtered out; 0 failed; 0.06s

cargo test --locked -p borsuk --test fault_injection transient_root_publication_error_is_resolved_before_returning -- --exact --nocapture
1 passed; 11 filtered out; 0 failed; 0.04s

cargo test --locked -p borsuk --test fault_injection vector_report_api_does_not_ack_when_positioned_envelope_upload_fails -- --exact --nocapture
1 passed; 11 filtered out; 0 failed; 0.04s
```

The next exact real-fault gate exposed a production defect and stopped the
round before any production edit or multipart execution:

```text
cargo test --locked -p borsuk --test fault_injection collection_transaction_is_invisible_when_positioned_head_publication_fails -- --exact --nocapture
0 passed; 1 failed; 11 filtered out; 0.09s
log: /home/rb/.local/share/rtk/tee/1786380617_cargo_test.log
```

The positioned head PUT was denied exactly once, the append returned
`object_store_permission_denied`, and the first reopen correctly exposed only
the previously committed `base` row. After `flush()` and zero-age GC on that
reopened handle, exact search returned `base` twice (`["base", "base"]`). This
violates the current visible-record contract and is not a stale test-path
failure. Per the controller instruction, production diagnosis/fix and all
remaining exact tests stopped here. No full target, formatting command, Clippy,
broad suite, AWS action, benchmark, commit, or push ran in this round.

### Authorized narrow source-retirement correction

The controller authorized a production correction after confirming that
`gc_obsolete_segments -> refresh -> reload_positioned_snapshot` reconstructed
positioned transactions without applying each modality manifest's
`cell_wal_consumed_runs` fence. The existing legacy loader had the required
all-or-none run-identity policy: retain a transaction when none of its runs are
consumed, omit it when all are consumed, and reject partial consumption as hard
corruption.

The first source change extracted that policy into
`retain_unconsumed_cell_wal_transaction` and used it in both the legacy loader
and positioned reload, including named projections. Positioned BM25 deltas are
now installed only when the primary transaction remains unconsumed. The exact
RED remained RED after that first change:

```text
cargo test --locked -p borsuk --test fault_injection collection_transaction_is_invisible_when_positioned_head_publication_fails -- --exact --nocapture
0 passed; 1 failed; 11 filtered out; 0.09s
log: /home/rb/.local/share/rtk/tee/1786381027_cargo_test.log
failure: exact search still returned ["base", "base"]
```

Tracing showed that legacy collection-transaction cleanup immediately compacted
the newly published consumed markers against legacy commits only. Positioned
transactions still authoritative in shard heads therefore lost their retirement
fence. Retained positioned source transaction IDs are now tracked per modality;
their consumed identities survive marker compaction and are excluded from
legacy CellWal prune/retention resolution. Before the legacy/source separation
was complete, the next exact run correctly exposed that mismatch:

```text
cargo test --locked -p borsuk --test fault_injection collection_transaction_is_invisible_when_positioned_head_publication_fails -- --exact --nocapture
0 passed; 1 failed; 11 filtered out; 0.07s
log: /home/rb/.local/share/rtk/tee/1786381140_cargo_test.log
failure: retained manifest references missing cell WAL transaction
```

After separating positioned consumed identities from legacy GC resolution, the
original exact regression passed. The directly affected multimodal exact then
found a second call path:

```text
gc_obsolete_segments
  -> root gc_obsolete_segments_primary / refresh
  -> child.gc_obsolete_segments
  -> child.refresh
  -> child.reload_positioned_snapshot
  -> InvalidStorage("positioned snapshot requires the collection root")

log: /home/rb/.local/share/rtk/tee/1786381340_cargo_test.log
```

Only the collection root owns the positioned reader, and its refresh already
loads every named manifest and reconstructs/retire-filters named projections.
Child GC now consumes that root-prepared snapshot instead of independently
calling the root-only reload. This edit was applied before the controller's
subsequent pause message arrived; that ordering was disclosed immediately and
no further production edits followed.

Fresh final exact evidence on the resulting tree is:

```text
cargo test --locked -p borsuk --test fault_injection multimodal_payload_wave_failure_never_resurrects_after_reopen_flush_and_gc -- --exact --nocapture
1 passed; 11 filtered out; 0 failed; 0.13s

cargo test --locked -p borsuk --test feature_matrix every_dense_sparse_bm25_combination_survives_flush_and_reopen -- --exact --nocapture
1 passed; 8 filtered out; 0 failed; 2.38s

cargo test --locked -p borsuk --test fault_injection collection_transaction_is_invisible_when_positioned_head_publication_fails -- --exact --nocapture
1 passed; 11 filtered out; 0 failed; 0.09s
```

No full target, formatting command, Clippy, broad suite, AWS action, benchmark,
commit, or push ran during this correction.

Concerns for controller/reviewer adjudication:

- The child-GC refresh guard was applied before the controller's pause message
  arrived. It is retained by subsequent controller direction, but has not yet
  received independent review.
- Evidence is deliberately exact and narrow. The complete fault-injection
  target was not rerun.
- The authoritative-head corruption exact, multipart segment exact, and the
  remaining retargeted collection publication exact were not run after the
  source-retirement correction.
- Formatting and Clippy were explicitly out of scope; only whitespace checking
  follows this report update.

### Fix round 5 independent re-review — blocker

The scoped reviewer accepted the immediate one-cycle retirement fix, legacy
CellWal separation, real fault-boundary retargeting, and root-prepared child GC,
but returned NOT PASS on a load-bearing repeated-cycle defect. Marker
compaction seeds positioned retention only from the current pre-flush live
transaction map. A transaction omitted from that map because it was already
consumed can therefore lose its consumed-run fence during a later flush while
the positioned shard head still authorizes it. A subsequent refresh can
reconstruct the transaction and duplicate the materialized primary or named
row.

This is the fifth and final Task 3 fix round. Per the SDD breaker, no further
fix, broad gate, qualification, commit, or push is authorized in this task
loop. Task 3 remains blocked until the implementation plan is revised or the
breaker is explicitly reset with a new reviewed task boundary. The required
architectural invariant is: every consumed identity whose transaction remains
in the modality's authoritative positioned snapshot must survive marker
compaction; Task 4 checkpointing may remove the fence only after that source
transaction is no longer authoritative.

## Qualification continuation after retirement-fence prerequisite

The blocker above was reset through the separately planned and independently
reviewed prerequisite
`docs/superpowers/plans/2026-08-10-positioned-retirement-fence.md`. Its exact
identity fence persists across later flush/GC cycles while the positioned head
remains authoritative. Focused repeated-cycle, failed-head-cleanup,
multimodal, and dense/sparse/BM25 reopen tests passed; the fresh reviewer
returned Spec PASS and Quality APPROVED with no Critical or Important finding.

A subsequent complete `local_index` qualification binary produced 136 passes
and 20 failures. The complete failure log was inspected and divided between
two independent audits:

- 18 tests encoded the retired direct-segment publication boundary, numeric
  generated IDs, or pre-flush routing/manifest assumptions;
- two failures were genuine public-report regressions.

The two report regressions were fixed under focused TDD:

1. `AddReport.total_bytes_written` now counts every payload byte submitted to
   the backing object-store PUT boundary for the complete mutation operation,
   including typed payloads, the position-bearing envelope, shard-head CAS,
   root claim coordination, and retry amplification. The counter belongs to an
   isolated operation scope and does not add object-store requests or write
   work. The test operation log independently records each PUT payload length.
2. Delete input IDs are canonicalized before state allocation; already-deleted
   IDs are no-ops; a previously untracked ID advances durable overlay metadata
   once while a `put`/upsert ID already present in that overlay does not; and
   the report carries the checked post-commit total rather than reading the
   still-precommit local snapshot.

RED evidence:

```text
mutation_facades_report_each_submitted_put_payload_byte_exactly_once
left (reported): 5509; right (submitted): 18767
log: /home/rb/.local/share/rtk/tee/1786385076_cargo_test.log

delete_report_counts_committed_positioned_tombstones_across_reopen
left (reported total): 0; right: 1
log: /home/rb/.local/share/rtk/tee/1786385085_cargo_test.log

duplicate delete refinement
left (reported deleted): 2; right: 1
log: /home/rb/.local/share/rtk/tee/1786385151_cargo_test.log

delete after `put` refinement
left (reported total): 2; right: 1
log: /home/rb/.local/share/rtk/tee/1786386052_cargo_test.log
```

Fresh GREEN evidence:

```text
cargo test --locked -p borsuk --test group_commit \
  mutation_facades_report_each_submitted_put_payload_byte_exactly_once \
  -- --exact --nocapture
1 passed; 20 filtered out; 0 failed

cargo test --locked -p borsuk --test group_commit \
delete_report_counts_committed_positioned_tombstones_across_reopen \
  -- --exact --nocapture
1 passed; 20 filtered out; 0 failed (duplicates, idempotency, reopen, and
delete-after-put covered)

cargo test --locked -p borsuk --test local_index \
  delete_hides_records_from_search_and_get_and_keeps_tombstone_object \
  -- --exact --nocapture
1 passed; 155 filtered out; 0 failed

cargo test --locked -p borsuk --test group_commit -- --nocapture
21 passed; 0 failed
```

The 18 stale fixtures are being migrated to explicit positioned-append versus
materialization boundaries. No broad qualification rerun, commit, push, AWS
campaign, or benchmark claim is made yet.

## Positioned report review fix

Status: **DONE**.

This follow-up replaces the rejected lifetime-delta telemetry and globally
exact delete-count claims identified by `task-3-positioned-report-review.md`.
All Rust commands used the existing isolated environment:

```text
CARGO_TARGET_DIR=/home/rb/worktrees/borsuk-prod-ready-v9/target-positioned-v12
RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper
SCCACHE_DIR=/data/cache/sccache
CARGO_BUILD_JOBS=2
CARGO_INCREMENTAL=0
RUSTFLAGS='-C codegen-units=8'
```

### RED evidence

The first exact integration RED failed compilation because the unreleased
`delete` API still returned `usize` and the requested request-local receipt
fields did not exist:

```text
cargo test --locked -p borsuk --test group_commit \
  overlapping_cloned_mutation_handles_have_disjoint_exact_report_scopes \
  -- --exact --nocapture
exit 101: 19 E0610 errors (`usize` has no report fields)
log: /home/rb/.local/share/rtk/tee/1786387615_cargo_test.log
```

The focused storage RED showed that the saturating counter forwarded and
accepted a PUT after losing byte exactness:

```text
cargo test --locked -p borsuk --lib \
  storage::tests::direct_put_payload_counter_overflow_fails_before_forwarding \
  -- --exact --nocapture
0 passed; 1 failed; 519 filtered out
failure: `unwrap_err()` received an accepted `PutResult`
log: /home/rb/.local/share/rtk/tee/1786387667_cargo_test.log
```

The first overlap implementation run then exposed a second real race rather
than a test defect: summed reports contained 16 PUTs while the store log held
17. Ordinary `BorsukIndex` clones shared `pending_collection_claim`, allowing
one operation to finalize the other operation's claim storage after the latter
had snapshotted its report.

```text
0 passed; 1 failed; 22 filtered out
log: /home/rb/.local/share/rtk/tee/1786387922_cargo_test.log
```

### Production design

- Every outer public add, generated add, put, upsert, and delete runs on a
  temporary mutation clone. One fresh counter scope is rebound through root
  collection storage, primary storage, every named child, exact-ID claims, and
  the positioned writer. The clone restores the prior long-lived bindings on
  both `Ok` and `Err` before replacing the caller's handle state.
- Operation claim guards are fresh as well. This prevents ordinary public
  clones from cross-finalizing claim requests and keeps coordination telemetry
  inside the correct report.
- Long-lived and read-only counting decorators do not accumulate PUT bytes.
  The positioned appender creates a fresh direct-PUT scope for each append;
  nested logical scopes each count a physical PUT once for their own report.
  No request, counter object, or global coordination object was added.
- Direct-PUT byte accounting uses checked atomic addition before forwarding.
  Overflow returns an explicit object-store error, leaves the prior total
  unchanged, and does not increment the PUT request count or write the object.
  Multipart byte coverage is not claimed.
- `delete(...)` is now the sole public delete API and returns
  `Result<DeleteReport>`. `delete_with_report`, `deleted`, and
  `total_tombstoned` were removed. `ids_submitted` is the unique canonical
  request-ID count and `published` means this handle emitted a positioned
  mutation. Stale redundant writers therefore make no false global count
  claim.
- `new_tombstone_ids` remains an internal conservative overlay-capacity upper
  bound. Fenced materialization recomputes the stable bound with checked sums
  over merged page cardinalities; unconsumed positioned suffix metadata remains
  conservatively additive.
- Rust, Python, Node, CLI, benchmark, and non-`local_index` test call sites were
  migrated to the breaking API. `local_index.rs` remained untouched under its
  separately reviewed migration ownership.

### Focused and final GREEN evidence

Each focused integration command passed independently: overlap and summed
request/byte accounting; multi-row add/generated-add/upsert exact bytes and
division; accepted ambiguous head bytes; stale/same/different-writer delete
visibility across reopen, put, and upsert; materialization rebase; and facade
request counts.

```text
overlapping_cloned_mutation_handles_have_disjoint_exact_report_scopes
1 passed; 22 filtered out; 0 failed; 0.02s

mutation_facades_report_each_submitted_put_payload_byte_exactly_once
1 passed; 22 filtered out; 0 failed; 0.04s

generated_add_reconciles_an_accepted_positioned_head_error_once
1 passed; 22 filtered out; 0 failed; 0.04s

delete_receipts_are_request_local_across_stale_writers_and_reopen
1 passed; 22 filtered out; 0 failed; 0.20s

materialization_rebases_duplicate_delete_upper_bound_to_page_cardinality
1 passed; 22 filtered out; 0 failed; 0.11s

mutation_facades_report_each_physical_request_exactly_once
1 passed; 22 filtered out; 0 failed; 0.06s
```

Fresh required final gates:

```text
cargo test --locked -p borsuk --test group_commit -- --nocapture
23 passed; 0 failed; 1.67s

cargo test --locked -p borsuk --lib \
  storage::tests::direct_put_payload_counter_overflow_fails_before_forwarding \
  -- --exact --nocapture
1 passed; 519 filtered out; 0 failed; 0.00s

cargo fmt --all -- --check
exit 0

git diff --check
exit 0
```

No full repository suite, full `local_index` binary, consultation, commit,
push, AWS action, or benchmark was run.

## Public binding migration re-check

The reviewer correctly found that the Rust breaking migration had not reached
the hand-written TypeScript declaration, Python stub, TypeScript API assertion,
or public deletion documentation. Those surfaces now expose only the honest
request-local receipt: `idsSubmitted`/`ids_submitted`, `published`, and
`requests`. The docs explicitly state that the submitted count is canonical
request cardinality, that `published` is handle-local, and that stale writers
may redundantly publish the same convergent LWW delete without implying a
globally linearized count.

RED and GREEN source/type evidence:

```text
npm run build (RED)
api.test.ts:2521:23 TS2339: idsSubmitted did not exist on DeleteReport
api.test.ts:2532:22 TS2339: idsSubmitted did not exist on DeleteReport

npm run build (GREEN)
tsc -p tsconfig.json; exit 0

uvx pyright tests/typing_usage.py (GREEN)
0 errors, 0 warnings, 0 informations
```

The exact Node runtime test remains invalid against the stale checked-out
addon (`idsSubmitted` was `undefined`). The one authorized scoped native build
was read to completion and failed before producing an addon on unrelated
concurrent `SearchReport` drift at `crates/borsuk-node/src/lib.rs:1893-1895`:
the Rust report no longer has `global_delta_approximate_us`,
`global_delta_exact_rerank_us`, or `global_delta_wait_us`. Complete output was
read from unified exec session 35151/cell 20; unlike RTK failures, that npm
invocation did not emit a tee path. Its complete compiler failure diagnostics
are preserved in `task-3-node-native-build-failure.log`. No retry was run.

An attempted exact Python runtime test was also invalid because `uv run`
unexpectedly initiated an unapproved maturin Cargo build against `/data/target`;
it was terminated and no matching process remained. Its complete partial logs
are `/home/rb/.local/share/rtk/tee/1786389319_uv.log` and
`/home/rb/.local/share/rtk/tee/1786389319_uv-error-block.log`. It is not counted
as a gate.

Repository source/docs search found no removed symbols outside the immutable
historical plan `docs/superpowers/plans/2026-07-29-collection-atomic-snapshots.md`.
`git diff --check` passed. No broad suite or additional Cargo command was run.

## PQ run-local page-dedup review

Spec: **PASS**

Quality: **APPROVED**

Finding counts: **0 Critical, 0 Important, 1 Minor**.

The production fix is correct. `rank_pages` filters unselected cells, then
deduplicates on `(run_ordinal, cell_index, leaf_ordinal, bundle_path,
batch_offset)` before distance ranking and the single global `page_budget`
truncation (`crates/borsuk/src/global_pq_sidecar.rs:1483-1537`). Consequently,
an exact duplicate logical reference within one run consumes one slot, while
otherwise identical references in distinct runs remain distinct because their
run ordinals differ. The established distance/level/cell/leaf/path/offset/run
tie-breaking order is unchanged, and downstream byte-budget truncation still
operates on the already deduplicated ranked list
(`crates/borsuk/src/index.rs:14255-14282`). The focused budget test covers every
production page budget, duplicate selected-cell input, rejection of an
unselected cell, one-run duplicate removal, and the encoded-byte ceiling
(`crates/borsuk/src/global_pq_sidecar.rs:1880-1931`).

### Minor

1. **The cross-run regression does not isolate the new `run_ordinal`
   discriminator.** Its two pages use `base.arrow` and `incremental.arrow`
   (`crates/borsuk/src/global_pq_sidecar.rs:1737-1755`). They therefore remain
   distinct even if `run_ordinal` is later omitted from the current dedup tuple,
   because `bundle_path` already differs. The implementation itself satisfies
   the contract, but the regression should give both pages the same bundle path
   (and otherwise identical key coordinates) so it fails specifically when the
   run-local discriminator is lost.

## Index unit-test retirement and resident-open ordering

The complete full-suite RED artifact was
`/home/rb/.local/share/rtk/tee/1786390763_cargo_test.log`. Eight failures were
fixtures for the retired Cell-WAL/lane-log write protocols. They were migrated
to the unified positioned-log contracts without retaining legacy production
paths:

| Former fixture | Classification | Positioned contract now covered |
| --- | --- | --- |
| `bulk_direct_add_locality_orders_records_before_segmenting` | Stale fixture | A positioned add is immediately visible but publishes no segment; explicit flush materializes locality-pure configured segments. |
| `coherent_collection_view_advances_before_omitting_a_pruned_transaction` | Stale fixture | Collection generation advances, and the materialized manifest's consumed identities exactly fence the authoritative positioned source. No obsolete source checkpoint is asserted. |
| `collection_delete_uses_parent_version_for_late_interaction_tokens` | Stale assertion | Every derived token reuses the complete canonical parent mutation stamp, including writer and digest. |
| `explicit_flush_prunes_fully_consumed_collection_wal_transactions` | Stale fixture | Before flush the positioned overlay and HEAD are authoritative; flush materializes the row, records the exact consumed fence, empties the live overlay, and preserves search visibility. |
| `gc_keeps_bm25_pages_referenced_only_by_cell_wal_metadata` | Stale fixture | Inline BM25 correction metadata is decoded from a HEAD-authorized positioned envelope and survives zero-age GC plus reopen. |
| `gc_preserves_pre_ack_payloads_with_a_live_staging_state` | Stale fixture | Claim-free in-memory collection staging publishes no positioned or legacy immutable mutation objects; GC is harmless while it remains staged. |
| `gc_reclaims_abandoned_staging_from_store_object_age` | Stale fixture | Aborting in-memory staging publishes nothing, and reopen observes no record; no fake abandoned object is injected for GC. |
| `lane_log_materialization_uses_cheap_scalar_segment_codes` | Stale fixture/policy | GroupCommit drain is append-only and visible through the positioned overlay; explicit flush materializes codes equivalent to the configured manifest quantizer. |

The ninth failure,
`resident_global_v11_continues_after_mvcc_suppresses_the_first_leaf`, exposed a
real open/refresh ordering defect. `open_with_loaded_manifest` prepared the
cell mutation frontier and resident V11 overlay while the positioned snapshot
was still empty. `open_with_storage` then loaded the authoritative positioned
snapshot, changing the overlay snapshot key after preparation. Search rejected
the stale resident overlay and fell back instead of continuing after MVCC
suppressed the first ranked leaf.

The minimal production correction separates manifest-frontier loading from
mutation-frontier loading. Open and refresh now load or reload the final
authoritative positioned snapshot before preparing the root and named-modality
cell mutation frontiers and resident global mutations. Preparation occurs only
against the snapshots that will remain installed, avoiding duplicate manifest
loads and stale snapshot-key swaps (`crates/borsuk/src/index.rs:3354-3386` and
`crates/borsuk/src/index.rs:4077-4125`).

Once the real ordering defect was fixed, the same regression reached its later
byte-budget branch and revealed one independent stale fixture assumption: a
cap derived from the average encoded size of two nonuniform physical pages may
admit either zero or one complete page. The test retains the production
contract—`MaxBytes`, empty hits, reported bytes at or below the cap—and now
asserts `global_leaf_pages_read <= 1` rather than exactly one. Its primary
continuation contract remains strict: the overlay must exist, dispatch must be
`bounded-arrow-leaf-v11`, the first ranked page is decoded and fully suppressed
by deterministic deletions, and the query must continue to the next wave.

An intermediate staging migration correctly failed because its first object
classifier counted the 64 empty, preinitialized lane `HEAD` control objects as
published mutations. The final helper counts only immutable lane extents,
positioned payloads/envelopes, and Cell-WAL transaction/run objects; the
corrected staging fixtures then passed without weakening the zero-publication
contract.

### Fresh focused GREEN evidence

All nine exact migrated/regression filters were rerun sequentially after the
production ordering change. Each reported `1 passed; 0 failed` (nine passes in
total). In addition:

```text
resident_global_v11_continues_after_mvcc_suppresses_the_first_leaf
1 passed; 519 filtered out; 0 failed; 5.15s

resident_global_v11_refresh_rejects_deleted_backing_object_when_cached_without_swapping
1 passed; 519 filtered out; 0 failed; 0.40s

independent_positioned_adds_compose_without_publishing_a_manifest
1 passed; 156 filtered out; 0 failed; 0.04s
```

No broader Cargo suite, commit, push, AWS action, or consultation was run for
this slice.

## Transactional positioned refresh

Review found that `reload_positioned_snapshot` cleared the live root and named
positioned projections before it had authenticated and decoded every eagerly
required envelope, typed transaction-metadata payload, tombstone payload, and
role. A failure late in that walk therefore returned `Err` after destroying or
partially rebuilding the handle's last coherent view. The advancing refresh
path additionally installed root/named manifests, collection snapshots, ANN
pins, and retry state before the same fallible reload and derived-frontier
preparation.

The focused regression builds a real V11 base with a dense named modality,
loads a positioned upsert plus primary/named delete projection, proves primary
and named visibility, and pins the resident MVCC overlay. It then removes the
already-loaded `tombstones_by_modality_v1` payload, whose eager decoding is an
intentional refresh cost. The ordinary record payload remains lazy and is not
GET/checksummed by refresh.

Exact transactional RED (unified exec result chunk `977a81`; no RTK tee file
was emitted by the proxied environment command):

```text
unchanged_manifest_refresh_rejects_missing_eager_positioned_metadata_without_swapping_view
refresh returned Err, then post-Err snapshot invariance failed at index.rs:26494
left: []
right: two previously coherent CommittedCellWalTransaction entries
0 passed; 1 failed; 520 filtered out; 0.51s
```

The fix introduces owned root/named positioned candidate state
(`crates/borsuk/src/index.rs:1453-1471`). Candidate construction authenticates
and decodes all eagerly required control/metadata/tombstone inputs into that
state without mutating the handle, while preserving lazy ordinary record-run
payload loading (`crates/borsuk/src/index.rs:4886-5220`). Cell frontiers and
resident mutation overlays are then prepared against explicit candidate
manifests, positioned transactions, and lane snapshot keys
(`crates/borsuk/src/index.rs:5222-5262`). Only the infallible install step swaps
the root and named projection fields and derived overlays
(`crates/borsuk/src/index.rs:5264-5296`).

Both unchanged and advancing refresh branches now construct and validate the
full candidate before installing manifests, references, collection snapshots,
ANN pins, retry counters, lane state, positioned projections, or resident
overlays (`crates/borsuk/src/index.rs:4101-4180`). No full `BorsukIndex` clone is
used, so shared mutation clocks, claims, positioned writers, and other mutable
coordination are not driven through candidate state on failure.

The first GREEN compile attempt was read completely from unified exec result
chunk `67c1c8` (again, the proxied environment command emitted no RTK tee or
continuing session). It failed before tests with seven `E0308` diagnostics from
one root cause: the new candidate snapshot-key helpers declared lane HEAD
checksums as `&[String]`, while the production field is `Vec<[u8; 32]>`. The
signature was corrected to `&[[u8; 32]]`; no other production change was mixed
into that correction.

Fresh warning-free GREEN evidence:

```text
unchanged_manifest_refresh_rejects_missing_eager_positioned_metadata_without_swapping_view
1 passed; 520 filtered out; 0 failed; 0.50s

resident_global_v11_refresh_rejects_deleted_backing_object_when_cached_without_swapping
1 passed; 520 filtered out; 0 failed; 0.42s

resident_global_v11_continues_after_mvcc_suppresses_the_first_leaf
1 passed; 520 filtered out; 0 failed; 5.11s

cargo fmt --all -- --check
exit 0

git diff --check -- crates/borsuk/src/index.rs task-3-report.md
exit 0
```

No broader Cargo suite, commit, push, AWS action, or consultation was run for
this transactional-refresh slice.

## Index migration and resident-open ordering review

Spec: **FAIL**

Quality: **NOT APPROVED**

Finding counts: **0 Critical, 1 Important, 0 Minor**.

### Important

1. **A failed positioned reload can partially discard the last valid refresh
   snapshot.** `reload_positioned_snapshot` obtains the positioned head, then
   immediately clears the primary snapshot, positioned identities/BM25 deltas,
   and every named snapshot before it reads and validates the referenced
   metadata, tombstone, and run payloads
   (`crates/borsuk/src/index.rs:4802-4830`,
   `crates/borsuk/src/index.rs:4831-5077`). Any missing/corrupt payload or
   malformed role after those clears returns `Err` with the receiver already
   partially mutated. The unchanged-manifest refresh path invokes this method
   directly on `self` before deciding whether the positioned view advanced
   (`crates/borsuk/src/index.rs:4075-4087`); the advancing-manifest path also
   installs manifests and snapshots before the same fallible reload
   (`crates/borsuk/src/index.rs:4090-4124`). A caller that catches the refresh
   error and keeps using the handle can therefore lose previously visible puts
   or tombstones rather than retaining its last coherent snapshot. The cited
   `resident_global_v11_refresh_rejects_deleted_backing_object_when_cached_without_swapping`
   regression fails during ANN preloading before this mutation point
   (`crates/borsuk/src/index.rs:26355-26383`), so it does not prove transactional
   positioned reload. Build the complete primary/named positioned projection in
   temporary state and commit it only after every payload and frontier validates,
   with a focused unchanged-manifest failure regression.

The successful ordering change itself is sound: open installs the positioned
reader and named handles, loads the authoritative positioned snapshot once, and
only then prepares primary/named mutation frontiers and resident overlays
(`crates/borsuk/src/index.rs:3354-3386`). Successful refresh likewise reloads
positioned authority after installing the selected collection/manifests and
before preparing those derived views (`crates/borsuk/src/index.rs:4090-4124`);
the unchanged-manifest branch rebuilds them only when the primary positioned
snapshot changes (`crates/borsuk/src/index.rs:4075-4087`).

The nine test migrations retain rather than bypass current authority. Adds and
GroupCommit drains remain positioned, visible, and segment-free until explicit
flush; flush assertions bind exact consumed run identities and configured
segment codes (`crates/borsuk/src/index.rs:24783-24859`,
`crates/borsuk/src/index.rs:24979-25048`, and
`crates/borsuk/src/index.rs:26947-26991`). The BM25 test reads typed inline
metadata through the HEAD-authorized source position before and after zero-age
GC/reopen (`crates/borsuk/src/index.rs:25241-25293`), while staging tests assert
that in-memory work publishes no immutable mutation objects rather than
injecting obsolete protocol artifacts. The V11 regression strictly proves
resident-overlay preparation, V11 dispatch, MVCC continuation, and global
wave/page ceilings; its nonuniform-page byte cap honestly allows zero or one
whole page and still enforces `MaxBytes` and `bytes_read <= max_bytes`
(`crates/borsuk/src/index.rs:26724-26825`).

The earlier PQ Minor is resolved. The two cross-run pages now share the same
cell, leaf, bundle path, batch offset, and payload, differing in the dedup key
only by `run_ordinal` (`crates/borsuk/src/global_pq_sidecar.rs:1731-1767`), so
the regression now specifically protects the run-local discriminator.

## Final transactional-refresh review

Spec: **PASS**

Quality: **APPROVED**

Finding counts: **0 Critical, 0 Important, 0 Minor**.

The prior refresh finding is resolved. `PreparedPositionedSnapshot` owns
separate primary and named transaction/identity projections plus primary BM25
deltas, and `PreparedPositionedDerivedState` owns the candidate resident
overlays (`crates/borsuk/src/index.rs:1453-1471`). Snapshot preparation reads
the authoritative heads/envelopes, checks the collection schema and typed roles,
eagerly authenticates transaction metadata and tombstone payloads, and retains
ordinary record payloads as lazy checked references
(`crates/borsuk/src/index.rs:4886-5220`). It mutates only local candidate
collections; the positioned reader path performs reads and envelope validation,
not coordination writes.

Frontier and resident-overlay preparation uses explicit candidate manifests,
candidate positioned transactions, and the applicable lane snapshot keys before
any live field changes (`crates/borsuk/src/index.rs:5222-5262`). The unchanged
refresh branch installs only after this preparation succeeds, while the
advancing branch likewise completes all fallible positioned, manifest-frontier,
ANN-pin, and resident-overlay work before committing manifests, references,
collection state, lane state, retry counters, primary/named projections, caches,
and overlays (`crates/borsuk/src/index.rs:4101-4180`). The final install is an
infallible owned-state swap and derives visible-run counters and live snapshot
cache keys from the installed candidate (`crates/borsuk/src/index.rs:5264-5296`).

The new regression is representative and strict: it uses a real V11 primary
plus dense named modality, verifies positioned put/delete visibility and a
pinned resident overlay, removes an eagerly required tombstone projection, and
after the expected refresh error proves primary/named manifests, references,
collection snapshot, transactions, identities, resident-overlay allocation and
snapshot key, and query results remain unchanged
(`crates/borsuk/src/index.rs:26605-26752`). The adjacent successful-refresh and
V11 continuation gates recorded in the transactional evidence exercise the
neighboring paths. No remaining scoped defect was found.

## Final schema, observability, and compatibility-retirement gate

The last workspace gate exposed and closed three current-contract gaps rather
than masking them with legacy behavior:

- `transaction_metadata_v2` now persists each modality's checked logical row
  cardinality in exact Parquet. Foreground and cold-open late-interaction
  projections therefore report the honest 3 primary entities plus 6 token rows,
  including an omitted optional named field as an explicit zero-row modality.
- Positioned payloads, envelopes, and shard heads have first-class storage trace
  roles and physical-format validation; no production positioned object is
  silently attributed as `unknown`.
- The remaining WAL and CLI fixtures now exercise the sole positioned-log path.
  `borsuk flush --uri ...` explicitly materializes the append-only tail for
  graph, routing, compaction, rebuild, and GC administration tests. No retired
  Cell-WAL or synchronous `WalConfig::disabled()` path was restored.

Deterministic fixture hardening also selects corrupted routing pages by a
persisted negative ID bloom, proves immutable PUT overlap with a barrier instead
of scheduler timing, and permits an empty maintenance shard while still
requiring complete exactly-once segment coverage and final manifest integrity.

Final evidence:

```text
cargo test --locked -p borsuk --test late_interaction_index
4 passed; 0 failed

cargo test --locked -p borsuk --test group_commit
24 passed; 0 failed

cargo test --locked -p borsuk --test positioned_log
31 passed; 0 failed

cargo test --locked -p borsuk --test wal
25 passed; 0 failed

cargo test --locked -p borsuk-cli --test cli
29 passed; 0 failed

cargo clippy --locked --workspace --all-targets -- -D warnings
exit 0; no issues

cargo test --locked --workspace --all-targets
1066 passed; 23 ignored; 0 failed; 70 suites; 475.66s

cargo fmt --all -- --check
exit 0

git diff --check
exit 0
```

Scoped adversarial review of the typed cardinality change initially found the
missing optional-modality zero row. The pre-commit canonicalization and cold
reopen regression resolved it; the re-review verdict was **APPROVED** with
0 Critical, 0 Important, and 0 Minor findings.
