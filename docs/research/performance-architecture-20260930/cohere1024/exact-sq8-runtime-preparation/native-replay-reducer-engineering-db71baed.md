# Native replay reducer engineering review

Source: db71baed69ec6c5b7e0389fb8e8ab760f0232db3. Group: c368b33868b14096. Consultation: 07f8f08ce1a64bf5. Read-only; native execution unverified.

**Request changes for `db71baed69ec6c5b7e0389fb8e8ab760f0232db3`.** Three concrete findings from read-only source inspection.

Engineering finding:

- **P2 — Impossible stage sequences are accepted.** [StageTotals::add](/home/rb/worktrees/borsuk-native-replay-reducer/crates/borsuk/examples/compare_native_replay.rs:232) validates each interval independently. Four overlapping intervals—or four zero intervals—pass and become reported stage totals. The native implementation executes discovery → source → planning → SQ8 sequentially; see [source/planning boundaries](/home/rb/worktrees/borsuk-native-replay-reducer/crates/borsuk/src/two_bit_generation.rs:1619). **Minimal fix:** require admitted, ordered intervals for this fixed native profile, and add a reauthenticated malformed-stage case that otherwise passes validation.

Research/methodology findings:

- **P1 — A/B identities need not identify different, correctly assigned implementations.** [The arm check](/home/rb/worktrees/borsuk-native-replay-reducer/crates/borsuk/examples/compare_native_replay.rs:693) requires only A1=A2 and B1=B2. Four distinct executions of the same binary can pass, including the timing gate; swapping the baseline and candidate identities is also accepted. The [input ledger](/home/rb/worktrees/borsuk-native-replay-reducer/docs/research/performance-architecture-20260930/cohere1024/exact-sq8-runtime-preparation/paired-retained-replay-inputs.json:112) specifies exact A/B binary hashes. **Minimal fix:** validate the expected binary for each label and bind source identities to the qualified arm evidence. Add same-binary and swapped-role rejection cases.

- **P1 — Agreement across runs does not bind them to the frozen retained inputs.** [Inputs::validate](/home/rb/worktrees/borsuk-native-replay-reducer/crates/borsuk/examples/compare_native_replay.rs:143) checks hash syntax; [cross-run validation](/home/rb/worktrees/borsuk-native-replay-reducer/crates/borsuk/examples/compare_native_replay.rs:676) checks equality. Consequently, four runs using different query/truth/source/SQ8/order hashes can still receive a measured timing disposition. The existing synthetic fixtures demonstrate the admitted structure by using `"a"` hashes everywhere. **Minimal fix:** enforce the ledger’s immutable input hashes and bind the expected rebound root/configuration from the root’s freeze. Alternatively, make that authenticated outer admission an explicit required gate in the report; the current caveat names resources and cost.

The raw-prefix, seal, full-file SHA/EOF checks, fixed query roster, score-bit/recall/charge comparisons, nearest-rank calculation, integer thresholds, and both paired comparisons look correct on inspection. File/output bounds, no-overwrite handling, file/parent sync, and local-only/uncontrolled-cache/no-vendor-win labeling are present.

No writer-schema mismatch was found. However, synthetic ranking fixtures use only positive `1.0`/`2.0` scores, so they cannot distinguish float total ordering from unsigned bit ordering. Include negative and signed-zero cases in native qualification.

**Verification remains UNRUN:** no builds, tests, network, consultations, edits, or job actions were performed. Source inspection and mock/format checks establish neither compilation nor performance qualification; the protected publisher job was untouched.

