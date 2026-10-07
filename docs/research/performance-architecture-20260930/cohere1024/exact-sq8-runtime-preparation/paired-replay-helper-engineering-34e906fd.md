# Engineering review of retained paired replay helper

Consultation: 93cfdf2cc5e449bc. Reviewed source: 34e906fd4adec0a0ffd04fbffedba04d0d65eda9.

**Hold integration for two repairs.** Reviewed commit `34e906fd`; the helper’s SHA256 matches the supplied contract.

1. **[P1] Publisher qualification is not bound to the current source.** At [helper line 186](/home/rb/worktrees/borsuk-retained-generation-rebind-native-gates/scripts/run_cohere_retained_paired_replay.py:186), `admit()` validates only digest syntax. An admission/config naming source `a*64` therefore accepts a retained-publisher gate naming `b*64`, with otherwise valid receipts and binaries. The positive fixture explicitly constructs this mismatch at lines 564–570. Subsequent checks authenticate binary bytes but never close this source-binding gap.

   **Smallest repair:** require the retained-publisher gate’s `source_identity_sha256` to equal the config/admission source. Preserve the historical A/B source identities; do not require those to equal current source. Correct the positive fixture and add one valid-but-mismatched publisher-digest refusal before native entry. **The root’s suspected gap is a concrete required repair.**

2. **[P2] A log-cap violation can become a completed ordinary native refusal.** At [helper line 365](/home/rb/worktrees/borsuk-retained-generation-rebind-native-gates/scripts/run_cohere_retained_paired_replay.py:365), every call exception is eligible for `BASELINE_NONZERO_EXIT` promotion. Exact trigger: an arm writes more than 16 MiB to its log and exits `2` before the supervisor’s next poll. [The supervisor](/home/rb/worktrees/borsuk-retained-generation-rebind-native-gates/scripts/prepare_hierarchical_cells_100k.py:368) rejects the log cap, then retains exit `2` during cleanup. [The reused classifier](/home/rb/worktrees/borsuk-retained-generation-rebind-native-gates/scripts/run_cohere_native_preflight.py:200) checks drain, exit, OOM and swap—but ignores log size. With assets unchanged and total scratch below 4 GiB, the helper can consequently publish `BASELINE_NONZERO_EXIT`, `complete=true`, instead of the required `INVALID`.

   **Smallest repair:** before promotion, check the retained native and unit log byte counts against `RESOURCES['max_log_bytes']`; preserve `INVALID` on violation. Add one oversized-log-plus-exit-2 fixture. Requiring `resource_gate_passed` alone would incorrectly reject ordinary nonzero exits too.

These are static source findings. No files changed, tests/native execution run, real data/GT opened, network/AWS invoked, or live jobs controlled. Real runtime remains **UNRUN**.

## Root reconciliation

Both findings independently traced in helper/shared supervisor source. Required source-binding repair assigned in immediate-1791361695605541926-966615 and confirmed in immediate-1791361943387766858-966615. Required native/unit-log-cap classification repair assigned in 1791361959348901822-966615 to the same OWN1 helper worker. Integration and runtime remain pending repaired bytes and bounded verification. Research review c12b0b3728e64aee remains running. Original native qualification a0006/session82618 is protected and unchanged.
