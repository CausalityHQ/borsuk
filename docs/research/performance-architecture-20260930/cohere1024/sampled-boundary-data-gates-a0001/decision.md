# Sampled boundary: real-input admission and smoke passed

2026-10-11. Qualified Rust source `9c40e0a5`, delivered on main `904c324a`. Exact ELF SHA `299ee9c2c7b73fe37f37f91f772ea527d1451b27067e5a4859ea773d7168fd07`; exact config SHA `91855a93e057a815ca3d3ebb849abb0bd8d388ff0ffe5ecbb0475884a489088f`.

| Gate | Original status | Evidence / scope |
| --- | --- | --- |
| Setup / opaque transport | Success, exit 0 | All eight real input bodies and ELF exact bytes/SHA authenticated on EC2 |
| Native real-input runtime admission | Native 0 / manager 0 / drained | Original controls reproduced, complete typed report, adjacency durable before requests and paired selection durable before truth |
| Separate fresh-output smoke | Native 0 / manager 0 / drained | Same exact CLI/assets/config, real kernel/systemd controls, publication and cleanup; none mocked |
| Scientific coverage | UNRUN | Candidate quality in admission/smoke remains unread |

Admission and smoke each took 17.79 seconds, process RSS 151048/151400 KiB, cgroup peaks 151408640/151470080 bytes, CPU0/100%, 512MiB cap, zero swap/OOM. These are local-file diagnostic resources, not cold ANN latency/QPS/product memory. Both original native logs and output manifests are retained; raw adjacency/selection/report bodies are opaque to root and immutable in S3.

Same causality Spot c7i.large `i-0941ab2a5eabaca3e`, eu-central-1b, terminated immediately after smoke terminal. Wait succeeded, root volume `vol-0ec038049143edacd` deleted, external timer inactive. Source and all original producer/data identities unchanged from freeze. No algorithm, scientific threshold or resource changes during execution.

Two collection-only defects were repaired without repeating native work: AWS CLI automatic pagination omitted IsTruncated, so list verification now uses --no-paginate and checks false; the success marker is in systemd-run.log, not native.log. Both failed collection states/scripts are preserved. Full per-stage SHA manifests and all declared resources are now independently verified.

Decision: both required input/environment gates passed. Freeze one separate consumed32 scientific coverage screen using the same Rust/input/config policy. Require SOURCE-scored page closure >=315/320 versus original314 and gains>losses. Do not impose zero losses, tune counts, adopt serving, or infer returned recall. Admission/smoke quality is not a scientific datapoint. No cold S3/performance/vendor win is claimed.

Artifacts: `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/sampled-boundary-data-gates-a0001/`. Frozen bodies/config/support and stage hash manifests are committed alongside this decision; raw scientific outputs stay in that immutable archive.
