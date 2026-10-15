# Source-shortlist v9 actual-input correctness

Scope: Cohere 1M, D1024, cosine k10; fixed query indices 0,1,127,255,511,767,998,999 from the unchanged 1000-query cohort. This is an actual-input correctness and staging attempt, not cold performance acceptance.

| Gate | Native evidence | Scope |
|---|---|---|
| Query-free fixture | IDENTITY_REBOUND_PROOFS_ONLY, complete | Preserves SOURCE/router/SQ8 payload; changes generation schema and SQ8 authentication proof granularity to32 rows |
| Actual8 reader | VALIDATED_PANEL, complete | 75/80 GT hits in the deliberately fixed subset; not an estimate of full-cohort recall |
| Historical rank parity | EXACT_SELECTED_RANK_PARITY, complete | Exact selected output matches immutable historical count report |
| Independent oracle | EXACT_RECORDED_SOURCE_COVER, complete | Full-sort/cover check within the recorded SOURCE frontier, not independent routing |
| Separate one-query smoke | Native panel and completed reader exits0 | S3-backed native binary path plus independent input/header validation |
| Object CLI smoke | Chain completed | Real create-only PUT, conditional GET, DELETE, verified404; not native-publisher qualification |
| Control canary | CONTROL_CANARY_PASS | Real systemd exit17, client timeout124, duplicate evidence preservation; absence-status inputs are mocked |
| Supervision | Seven native stages exit0, drained, no forced cleanup | Original native/timeout/manager statuses kept separately |
| Compute closure | Watcher exit0 | Instance terminated, root volume absent, external timer inactive |

Frozen library source ee598553b227660d9e78500086411481456e1a0e; runtime support source1c4616c772c39c0b0ba1fe1cc8aa9d57ff517c38; support archive b9fd8d0eb6a52351254111589724ab4225851c63894b2068bb63ebb58c2c03f5. Baseline ELF912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995; helper0845bddd426a6e7b9a56317f73ff92fdfa15b14b2f1d109cf62d7a77bbb54fbd. New root460d095ac98f2dba70ff765907ba89d3bd7bbdb03e1cb4781da637ef5d19baf6.

Original SSM setup bbcd743d-742c-4cdc-9577-6a28a95d4227 and native4c27b102-d8dd-438a-80f5-7419371ca125 both Success0. Spot m6a.large i-0492d73d5303f4041 terminated and root volume removed. Terminal archive226264B SHA f18d72d4d8308c03f846f7f82ee0c0f829d070892a964bc9acf6b5ae5d8881a4; all382 roster files independently hashed without parsing scientific result JSONL.

Measured resource receipts (correctness tooling only): fixture peak RSS7752KiB / cgroup peak346316800B; panel63384KiB /76398592B; oracle272408KiB /276164608B. Cgroup peaks include charged file cache and are not RSS. These observations do not establish cold query latency or concurrent throughput.

Disposition ACTUAL8_CORRECTNESS_PASS after root checks and independent evidence audit3b1532c09fa642b9 completed0. Original pre-run actual_input_verified=false and terminal compute_closure_pending=true are preserved lifecycle snapshots; later native and infrastructure receipts close those checks. No cold/vendor win claim. Next is a separately frozen matched full1000 cold comparison with explicit original-control and candidate source/binary/generation authority; protocol planning2b51feca83da47b7. Preserve quality/Pareto gains even if an efficiency screen misses, and distinguish execution INVALID from an algorithm result. Product, lifecycle and matched competitor goals remain incomplete.
