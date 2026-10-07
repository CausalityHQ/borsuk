# Research review of retained paired replay helper

Consultation: 792013084da04245. Reviewed original source and repair diffs through 9120e4a7.

## Review of `scripts/run_cohere_retained_paired_replay.py` @34e906fd

The helper had one required repair: the publisher gate's source identity was never checked against anything. The child has already fixed it in `578d8e4f`, and that fix is correct. One smaller gap from the plan is still open, and root has one check to do at freeze that the helper can't do. I found nothing else that could invalidate the paired gate.

The child branch moved while I was reviewing: 34e906fd → `578d8e4f` (gate-source binding) → `9120e4a7` (log-cap guard). I reviewed 34e906fd in full and read both follow-up diffs. Nothing was edited in the repo and nothing was run.

### 1. Required repair (fixed in 578d8e4f): publisher gate source was unbound
- **Where:** `admit()`, `scripts/run_cohere_retained_paired_replay.py:186`. It only checks that each gate's `source_identity_sha256` is a well-formed digest.
- **Trigger:** root edits the Rust source and builds publisher P′ at source S′. The admission then pairs the old a0006 publisher gate (source S) with P′'s hash under config source S′. The helper accepts this, so a binary no gate ever qualified gets used. The original positive fixture (gate `b*64`, config `a*64`) actually ran this case as a success.
- **Why it's required:** the plan @191944fa says "Bind all three exact binary bytes/SHA and gate-source identities". The precedent `run_cohere_native_preflight.py:82` binds the current-source gate exactly.
- **Repair:** the one `exact(...)` check in 578d8e4f, the fixture change, and the new `adm-publisher-source` refusal test are the smallest correct fix. `source_identity` hashes only `*.rs`/Cargo files, so adding the Python helper doesn't change it and the binding can be satisfied.

### 2. Still open: the two baseline gate sources are not bound
- **Status:** 578d8e4f leaves `original_baseline` and `blocked_scorer` as digest-only. The updated contract says these are "recorded, not bound", which contradicts the plan's "all three".
- **Impact:** small. The committed ledger already pins the A and B binary hashes, so a wrong gate source can't change which bytes run. It can only record false provenance in the receipt.
- **The values to bind to are already committed:**
  - `original_baseline` is the a0002 `native_publisher` gate: `d0e7029d484db02904505b7c83c57a13a3258ab9db804e54338ff3fca0f39676` (i-092b24c0ccb591afe, 6 stages, binary A `ce43842c` under role `baseline`).
  - `blocked_scorer` is exact-sq8-runtime-gates a0001: `4ed25e42ab6caabb94c76e1c7a909cf82a31a15887f06e41a6801ddcda409e03` (i-08769cc86a13173d8; ledger B key points to that run).
- **Smallest repair:** add a constant `GATE_SOURCE = dict(original_baseline=…, blocked_scorer=…)` and replace the publisher-only branch with `exact(gate['source_identity_sha256'], GATE_SOURCE.get(name, config['source_identity_sha256']), 'gate source identity: ' + name)`. The fixture then uses those values.
- **Alternative:** amend the plan sentence to "publisher bound; A/B recorded". Either way it's root's call.

### 3. Root must check this at freeze; the helper can't
The ledger has `native_publisher: null`, so no committed artifact pins the publisher binary. `admit()` checks the gate receipt's bytes and hash but never reads its contents. Before freezing the admission, root must confirm that the a0006 terminal receipt's compiled publisher hash equals `roles.publisher.binary.sha256`, and that its source equals `config.source_identity_sha256`. The preflight precedent has the same trust model, so this isn't a helper defect.

### Checked and sound
- **Native interface:** at the a0006 candidate `a682fe84`, the Rust `RetainedConfig` rejects unknown fields and has exactly the helper's 12 fields. The receipt has exactly the 17 fields the helper requires, and the argv is `--retained CONFIG SHA RECEIPT`. Everything the publisher writes stays under `destination_prefix`, and the head epoch is 1.
- **Publisher scratch:** the formula in `two_bit_store.rs`, applied to the committed ledger sizes, needs 33,841,092 B against the 64 MiB cap.
- **Outer scratch:** about 620 MB in use against the 4 GiB cap, even if hard links are counted twice.
- **Config rebase:** the original 1285 B config has exactly four absolute paths (requests, truth, store_root, scratch_parent) plus the generation prefix and root. The six rebased slots cover all of them, and `neutral()` holds everything else fixed.
- **Envelope:**
  - At a0002, baseline A ran in 77 s with a 72 MB peak, 1.2 KB of log and a 10.3 MB result. That fits the 300 s / 512 MiB limits, the 16 MiB log cap and the 64 MiB result re-authentication cap.
  - Baseline B has no comparable committed measurement here.
- **Exit classification:** `require` raises `ValueError`, so `nonzero_native_exit` falls back to INVALID for kills, timeouts and missing receipts. Hard-link changes to `ctime`/`nlink` are correctly left out of the publication check and included in the fingerprint taken between arms.

Retrying after a failure needs a freshly staged `work_root`, because publication changes `input/`. That is consistent with create-only output, not a defect.

## Root reconciliation

All three gate-source identities will be bound, preserving distinct historical A/B revisions. Historical A source and 1216-byte receipt pin independently matched native-preflight/admission.json and cohort/a0002/verification.json; B source/binary hash and all seven zero stages independently matched exact-sq8/a0001 terminal/root verification/closeout. Required bounded OWN1 repair assigned immediate-1791362492232025055-966615 to the same worker. Root must authenticate actual a0006 publisher ELF hash and full source closure at freeze; helper cannot replace that responsibility. Compiler qualification remains live and helper runtime remains UNRUN.
