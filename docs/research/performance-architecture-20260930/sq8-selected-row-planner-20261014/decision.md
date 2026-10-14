# Authenticated selected-row SQ8 planner: native qualified

Selected implementation: `cb34ca25da00b8282e4885639c22b80eef4ef062`, based on public `b20abe258723635de0abf804811ab9e24ff78dda`. Only `sq8_page_authority.rs` changes in the Rust library relative to public base. The new dataset-independent API admits strictly ordered physical row ordinals, authenticates SQ8 geometry through PageAuthority, and returns their complete block cover under explicit GET, byte and owned-planner limits. It uses existing shortest-gap coalescing, charges bridge bytes and partial tails, refuses the whole plan if insufficient, and checks the conservative `512*N+4096` payload bound before allocation. Borrowed caller payloads and allocator/transport overhead are separate.

The API is colocated with page authority, rejects SOURCE-plane authorities before allocation, and uses authenticated record_bytes. The shared budgeted-page file is identical to the public base, preserving its comparator inclusion. No persistent layout, serving selector or default changes here. SOURCE128/width64/windowOFF remain selected. Actual shortlist recall, cold performance, lifecycle cost and matched vendor comparisons are unmeasured by this increment.

| Gate | Disposition |
|---|---|
| Original two missing-planner fixtures | Expected native failures; historical source4558 retained |
| Original a304 comparator integration | Workspace Clippy E0433; source held, not released |
| Valid SOURCE-plane regression at582 | Expected native101: invalid544..561 range instead of refusal |
| Repaired affected library compile | 0 |
| Repaired comparator compile | 0 |
| Three exact planner tests | 1 passed each, exits0 |
| Unchanged planner regressions | 10 passed, exit0 |
| Authority regressions | 5 passed, exit0 |
| Release library build | 0 |
| Workspace correctness/suspicious Clippy | 0; not a warning-free claim |
| Actual shim-unset full workspace test build, jobs1 | 0 |

All ten stages independently checked: exact commands/source, 470 before/after source checks, receipt SHA manifests, original native and manager exits, drained units, CPU0–3/400%,14GiB/swap0/pids512 and zero OOM. Qualification command `bf6f9f82-5b35-4bd9-833b-6550ac0184e2` finished Success0 on `i-0957f7103f2876e39`; instance terminated, root volume `vol-04a3300b6f5885ca6` absent and external timer inactive. First c7i Spot request was refused for capacity and no instance was created; the recorded m7i capacity-only attempt kept source, commands and native limits unchanged. Its original create-only input prefix was reused because the refused launch created no stage evidence.

Complete original stage files are consolidated in `receipts/stage-bodies.json`: each entry contains path, original bytes, SHA256 and UTF-8 body. Encode body_utf8 as UTF-8 to reconstruct the exact original bytes. Independent verifier and review reconciliation are retained. Qualified code follows completed dual-review repairs; no mock result substitutes for native qualification.

## Falsifiable next gate

Use the production SOURCE scoring callback and exact frozen Cohere1M/D1024/cosine/k10/full1000 inputs. Reproduce every sealed control ID, score bit, trace and charge before interpreting a shortlist. Then evaluate1024/2048/4096/8192 eligible rows with ephemeral authenticated32-row SQ8 blocks, preserving SOURCE128/routing/cache and recording shortlist loss, physical expansion/bridge recovery, SQ8 ordering loss and exact GET/bytes. Baseline recall97.80%, prospective floor97.60%, no cumulative ratchet or arbitrary2x bytes gate. Preserve valid Pareto points. Offline local-file timing does not establish cold S3 latency.

Four test-only falsifier fixtures are authored on private sourceb51c4ebe but remain uncompiled/unrun; full actual-input driver is incomplete. Run native fixtures, actual-input admission and a disposable staging smoke before measured candidate execution. Only useful measured quality/byte tradeoffs justify persistent layout/lifecycle integration and fresh paired cold timing. Product goal, matched10M comparison, concurrency/lifecycle/recovery qualifications remain active.
