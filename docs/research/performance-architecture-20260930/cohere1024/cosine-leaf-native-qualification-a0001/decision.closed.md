# Explicit cosine leaf ranking: native-qualified library increment

Qualified source: `bf7b1b7bcb148f2ecfe58c856ff172bca3b82b82`, based on canonical `b723ca878b89fe210374e7333078c2240335dfef`. Only production/source path changed is `crates/borsuk/src/semantic_unit_router.rs` (SHA256 `7796a6802f78eadd0ffb167ace2cb22b0b9c6b9589fd6ec3c09e52b1c4aff325`). No Python, TypeScript, Cargo, persisted format, training, membership, physical layout, or production serving default changed.

## Library change

`SemanticUnitRouter::rank_leaves_by_metric(query, metric)` ranks all authenticated leaf IDs, with deterministic ID ties, for squared Euclidean or cosine. The caller explicitly takes an admitted count and calls `nominate_selected`. Cosine uses f64 norm/dot accumulation; exact-zero prototypes are neutral, while zero/nonfinite queries are refused. Its routing convention intentionally differs from `VectorMetric::distance` near zero. Unsupported metrics are refused.

Existing serving uses the same L2 rule: Native100k first8/1.15 boundary/max16, Scale1m/Fresh1m48. This API has no production serving caller. It enables one causal metric comparison; it does not establish that cosine improves recall. No compatibility alias or legacy reader was added.

## Native evidence

One causality EC2 Spot instance `i-029219dbcf473f5a6` in eu-central-1, CPU0-3/400%,8GiB,swap0,pids512,Cargojobs2,serial stages. Original native6000s/machine8400s/external8520s deadlines retained. Resource and source receipts are pinned below; original test-only counterexample and intermediate pass remain immutable.

| Exact-source gate | Outcome | Wall time | Maximum process RSS |
|---|---:|---:|---:|
| Affected router module | 15 passed,0 failed/ignored | 81.24s | 5,758,556 KiB |
| Locked release library + baseline binary | exit0 | 420.25s | 4,552,024 KiB |
| Locked workspace/all-target Clippy correctness+suspicious | exit0 | 124.51s | 3,875,696 KiB |
| Actual override-unset workspace/all-target test compilation | exit0 | 199.48s | 5,539,028 KiB |

All stages native0/manager0/cgroup drained; all 463 authenticated source-file hashes matched before/after and against the exact Git source. Native map has430 paths. Actual workspace tests were compiled, not executed. The four new tests cover independent fixed48 metric orders, zero/negative/ties/extreme finite values, geometry/unsupported refusals, and authenticated public dispatch plus unchanged default nomination.

The test-build cgroup reached8,589,934,592 bytes and recorded883 `memory.max` events; zero OOM/kill events and swap0. Process RSS is not total cgroup usage. The enforced cap remained unchanged; no compiler-resource widening was used.

Original RED source916 compiled and failed precisely the assertion: current L2 ranks1..48, angular oracle0..47. This proves a policy distinction, not an error in the declared L2 root. Intermediate green488 passed4 tests; review then strengthened the public-dispatch fixture and documentation in bf7. Both labelled review answers and reconciliation are retained. No duplicate review or paid launch was used.

Lifecycle closed: SAME instance terminated; root volume `vol-09dfa872d48d8b5c2` deleted with exact InvalidVolume.NotFound receipt; external timer inactive; background marker cleared. Compute was not kept for convenience. No real query/truth/vector/SQ8 execution occurred in this qualification campaign.

## Evidence retention

Immutable complete native receipts/logs/source before-after maps are under `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/cosine-leaf-native-qualification-a0001/stages/<stage>/`. The committed per-stage `SHA256SUMS` pins every full body; the Git slice retains terminal/command/resource receipts and extracts, rather than duplicating complete compiler warning logs. Stage names are cosine-leaf-red, cosine-leaf-green, source-repair-r2, source-repair-r3, cosine-leaf-affected-r3, cosine-leaf-release-r3, cosine-leaf-clippy-r3, cosine-leaf-test-build-r3. Original source archive/support pins are preserved by the frozen campaign protocol and input receipts.

`qualification.closed.json`, `native-source.json`, `source-files.sha256`, and the exact-source Git comparison are the current qualification authority. See `review-results.md`, `review-reconciliation.md`, `next-gate.md`, and `coverage-probe-source-seam.md` for the bounded next decision.

## Scientific disposition

No recall, latency, QPS, cold S3, cost-per-query, or vendor gain is claimed. The previous1M consumed32 result remains313/320 FAIL, with6 SOURCE-coverage misses and1 covered ranking loss. Next: fixed48 paired L2/cosine SOURCE-scored closure coverage, both selections sealed before truth, unchanged SOURCE128GET/64MiB. If control reproduces314/320 coverage, require315 for positive net coverage gain; report gained/lost slots and fetched bridge pages separately. Only a survivor earns returned-recall testing with unchanged SQ8 limits32GET/16,773,120 bytes. The consumed panel is diagnostic only; fresh unused holdout precedes population/vendor acceptance. Full1000 and matched1M cold comparisons remain unrun.
