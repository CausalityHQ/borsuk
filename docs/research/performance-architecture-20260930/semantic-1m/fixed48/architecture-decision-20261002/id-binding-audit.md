# ID-domain audit

Read-only consultation c1da2f3a53ce4eed, gpt-6.1-sol/high, completed exit0. This resolves the earlier inventory's descriptor gap; it does not establish fresh payload authentication or current-format qualification.

**Missing ReLAION `original_ids.u64` does not block the recorded ordinal-ID comparison.** It is needed only to translate corpus ordinals back to original ReLAION `feature_row_id` values. Moreover, authenticated descriptors for that map are committed. V282 itself already relabels serving IDs and truth to ordinals; calling its internal comparison “stable-ID” would be misleading.

Audited HEAD: `f0de4b0ae2f75fefc3a584491663637277d081a1`. Working tree remained clean. Only code and metadata were read; no payloads or experiments were opened/run.

**Observed ID-domain chain**

Let `o` be a corpus ordinal, `p` a physical position, and `O[p]` the recorded order map:

`truth value o → logical ID o → physical position p = O⁻¹[o] → canonical record[p].ID = o`

Original ReLAION identity is a separate translation: `original_ids[o] = original feature_row_id`.

| Step | Code evidence |
|---|---|
| ReLAION original IDs are saved separately; source `feature_row_id` becomes `0..99999` | [v282_prepare_pair.py:69](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/v282_prepare_pair.py:69), especially lines 75–86 |
| ReLAION truth writes top-k **row positions** as LE u32 | [v282_prepare_pair.py:97](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/v282_prepare_pair.py:97), lines 120–121 |
| CoHere truth likewise uses source ordinals | [v248_score_cohere_graph.py:133](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/v248_score_cohere_graph.py:133), lines 138–148 |
| Ordinary SQ8 construction reads source row `O[p]` and writes logical ID `O[p]`; an explicit application-ID mode also exists | [sq8_source.rs:258](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/sq8_source.rs:258), line 276 |
| Recorded source-completion controller requires a complete ordinal permutation and `candidate_sq8.ID == order` | [run_native_source_completion_http.py:28](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/run_native_source_completion_http.py:28), lines 33–40 |
| Development scoring compares fetched/returned **record IDs**, not physical positions, directly with truth | [native_two_bit_cosine_development.py:41](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/native_two_bit_cosine_development.py:41), lines 44–48; its CoHere runner checks the ordinal roster at line 94 |
| Current builder passes the same order into plane and canonical construction | [two_bit_build.rs:558](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_build.rs:558) |
| Canonical construction reads raw row `O[p]` and copies the SQ8 ID unchanged | [canonical_source.rs:91](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/canonical_source.rs:91), lines 95–117 |
| Native packaging binds order/SQ8/canonical descriptors and copies their bytes | [package_semantic_native_generation.py:153](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/package_semantic_native_generation.py:153), lines 159–184 and 206–214 |

**Recorded descriptor authority—not freshly authenticated payloads**

The [source-completion config](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928/source-completion-http-config.json:24) supplies exact S3 keys and these SHA-256 values:

| Artifact | ReLAION | CoHere |
|---|---|---|
| `truth.u32`, 400,000 bytes | `4bd3ac79fce3919f85359ce3e305491663991f6cc0890ab91682cda34247a24e` | `06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8` |
| `order.u64`, 800,000 bytes | `22abeb087000ea2978c4fef8e85c2f7fda4e4b22c4e5126ba37a1c19816fab18` | `e2314ca14f5aa6f08b02cc328b863c921e55b1461a413ad6d3d66e781ff42d50` |
| `canonical.bin`, 308,000,000 bytes | `c260b7fe0b6a54de98dfba3f2599d272242f724421064ed5ca18c0cd63e6283d` | `38304c3b448f2f62b32da8b443bbe52f20ccc63f5472d0e7885d5c268dd7a6df` |

[Cold-input derivation](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-native-cold-input-derivation.json:1) records first64 slicing of requests and truth, with no ID remapping. Its truth children retain GT100 width; that alone does not establish a current k100 control.

Two committed receipts describe ReLAION’s original-ID map:

- [native-relaion-terminal-receipt.json](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-relaion-terminal-receipt.json:1): `source/original_ids.u64`.
- [V282 original-terminal.json](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/validation-loss-diagnostic-20260928/original-terminal.json:1): `relaion/evidence/original_ids.u64`.

Both record **800,000 bytes**, SHA-256 `ac7ee849bb1dad82e64332ce9472848140168a84d7351d7292ad3ec8ecb50479`. The V282 terminal’s own SHA-256 is `23cf19cdc03c5ce2df116b3eeb70e57c57f9aeaf84ea95ab110dbe598f78b3bc`, also pinned by the [diagnostic config](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/validation-loss-diagnostic-20260928/config.json:1). This is descriptor evidence, not a claim of local availability.

**Inference and remaining risk:** the recorded current comparison has a coherent ordinal domain without the original-ID map. Actual selected artifacts still require independent authentication: a unique ordinal roster alone cannot detect an incorrectly paired permutation. Historical packaging also does not prove current-format admission.

**One actionable plan:** root freezes the prospective input manifest from these descriptors, labels truth and logical IDs explicitly as corpus ordinals, and independently authenticates order/canonical/request/truth bindings before any approved cell. Obtain the original-ID map only if an external original-ID comparison is intended. Keep the format unchanged and retain the pending operator-approval boundary.

**Bounded falsifying test, proposed only:** root performs one input-admission check per corpus: authenticate descriptors; verify `O` is a full permutation; verify every canonical ID equals `O[p]`; check truth IDs are in range and query alignment is exact; compare a predetermined small sample of canonical normalized rows with authenticated source row `O[p]`. Any mismatch falsifies the binding and makes execution INVALID. No ANN, tuning, fresh panel, or performance claim is needed for this check.

