# Sampled boundary nomination: native qualification

2026-10-11. Exact source `9c40e0a5d5b84e6dbe7662d331601780facd066a`, parent `7916288a8c04736e118dd5037e52c2239f179c71`.

| Gate | Result | Scope |
| --- | --- | --- |
| New library/example tests, debug and release | 18 named passes | Sampling, decode, checked allocation/votes, base-only arcs, ties, authentication and seal ordering |
| Existing coverage and sealed-loss tests | 12 named passes | Existing SOURCE closure, provenance, refusal and publication invariants |
| Release example | Exit 0 | Authenticated ELF, 6,380,056 bytes, SHA256 `299ee9c2c7b73fe37f37f91f772ea527d1451b27067e5a4859ea773d7168fd07` |
| Workspace/all-target Clippy correctness+suspicious | Exit 0 | Other warning classes remain warnings |
| Real shim-unset workspace test build | Exit 0 | Complete test surface compiled; not all workspace tests executed |
| Real-data admission / separate smoke / coverage screen | UNRUN | Required next gates |

All nine stages ran serially on causality Spot `i-02f1480305f20e32a`, SSM `88b4f1db-4cf6-4008-b892-172e2c0f7252`. Each stage authenticated all 463 source files before and after, matching the root archive; 430 native files have identity `244a7a2e25917820ac6ac0ec11e075e9797530ff6b7cc7b8f26804f966b93164`. Compiler envelope: jobs 2, CPUs 0–3/400%, 16 GiB RAM, zero swap, pids 512. Every stage recorded zero OOM kills and drained its owned cgroup. Compiler peaks are not product RAM measurements. Batch completed 9/9, exit 0. Instance terminated and waited; root volume `vol-05d188531a3d55435` deleted; external timer inactive.

Both independent implementation reviews approved bounded correctness execution. Research critic fell back from unavailable Opus to Sol; engineering critic used Astra. Full labelled results are retained. No new algorithm repair was required by these reviews.

Decision: accept this Rust prototype as compiler/synthetic-correctness qualified. Keep serving defaults unchanged. This does not establish real-data recall, resource feasibility, cold latency, capacity, lifecycle cost or a vendor win.

Next causal arm remains exactly the committed planning contract: midpoint SQ8 samples, sequential nondiverse graph (m=16/m0=32, ef=64), retained directed base arcs only, per-leaf top 16, fixed L2 first 40 plus eight weighted boundary nominations. No layout, score, normalization or transport change. The versioned sidecar authenticates source/order/calibration/geometry and is durable before request access; paired selections seal before truth access.

Run source-bound real-input admission and a separate disposable smoke on this ELF before freezing one consumed-32 coverage screen. Preserve original producer identity, all input pins and historical outcomes. Screen survival requires candidate SOURCE-scored page closure at least 315/320 versus control 314, and gains greater than losses; no zero-loss requirement. A survivor only licenses a returned-recall falsifier. No tuning sweep, cold benchmark or production adoption is licensed by synthetic qualification.

Immutable full stage artifacts and logs: `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/sampled-boundary-native-qualification-a0001/`. The committed per-stage SHA256 manifests authenticate those bodies. Qualification receipt, protocol, exact rosters, full source hash map, source review results and cleanup receipts accompany this decision.
