# Thin retained paired replay execution

Status: implementation direction superseded by the operator on 2026-10-07: use Rust for benchmark execution, validation, metrics and result output; stop new Python/controller work. The unlaunched Python helper was removed from the current tree. Its exact source, reviews and bounded-check evidence remain in commits ebecb552/8f802bb8 and the source contract. Existing frozen launch tooling may run native binaries without edits.

The scientific paired method and fifteen-input ledger remain authoritative. No real paired replay was launched. The section below preserves the earlier helper design for audit; it is not a current implementation assignment.

## Historical helper design

Reuse the existing native_stage / owned_stage supervisor and native-preflight local authentication, closure, inventory and terminal helpers. Add only scripts/run_cohere_retained_paired_replay.py. Keep the existing transport/lifecycle untouched in this slice. The helper accepts already staged opaque assets and three qualified binaries.

The committed paired-retained-replay-inputs.json is authoritative: 15 assets total 551480739 bytes, INCLUDING store/semantic/index/head.json (170 bytes, SHA256 617af5f166065f2b95b1459866493f75a7ab2f5793f5ffc5c4492692766c97a3). Authenticate it normally with all assets; compare its small metadata to original_head_authenticated and retained_approval. Do not create a replacement head or reconstruct original build assets. The prior draft instruction about a missing head is superseded.

Before native calls require a pinned root admission with terminated, all-zero complete gate rosters: retained publisher 5 stages, original baseline 6 stages, blocked scorer 7 stages. Bind all three exact binary bytes/SHA and gate-source identities. Root freezes this only after actual qualification. The source ledger's body_authentication_pending and runtime_rebound_root_pending remain true; only actual output evidence closes these notions. Never accept self-asserted runtime completion.

Stream SHA/length/exact EOF all assets and binaries before publication. Derive the staged SQ8 ETag afterward. Invoke native retained publisher once with exact approval, destination semantic/rebound, explicit native limits and 64MiB publisher scratch. Read only bounded publication metadata; bind actual new head/root and staged metadata roster. Baseline configs are the authenticated original 1285-byte config, with ONLY requests.path, truth.path, store_root, scratch_parent, generation_prefix and generation_root_sha256 changed. Native source identities, dimensions, k, population, limits and all algorithm choices are unchanged.

Run publisher, A1, B1, B2, A2 serially through the existing supervisor; CPU1/512MiB/noSwap, <=300 seconds each, <=1800 seconds whole execution, 4GiB total scratch. Each arm uses byte-identical query config and one rebound root. Require scratch empty between calls and original plus rebound assets unchanged. Keep native result JSONL and logs opaque: SHA/length/EOF only, no result parsing or Python ANN/truth/score logic. Native owns query seals and GT order. Root alone checks all complete collected results and parity before performance interpretation.

Terminal states are NATIVE_CHAIN_CLOSED, BASELINE_NONZERO_EXIT or INVALID, with original native exits and resource/drain receipts retained. Stop at the first failing arm. Use create-only output and terminal-last publication. No performance or parity success claim from glue.

Use one bounded source-only self-check through real orchestration/supervisor with labelled fake natives and mocked systemd; verify intended refusal reasons, no native start before admission/authentication, role drift, head/approval mismatch, original config drift, output collision, publisher failure, changed original/rebound files, arm failure, deadline/scratch and closure failures. No local Cargo/native/data/network. Root verifies the coherent source commit before integration. Actual input admission, separate staging canary, freeze and one paid replay remain separate mandatory gates.

The frozen scientific method in paired-retained-replay-method.md remains authoritative. Local-file replay proves no S3 or vendor comparison. Matched own S3 Vectors evaluation and the 10M/8QPS/600s comparison remain future product work.
