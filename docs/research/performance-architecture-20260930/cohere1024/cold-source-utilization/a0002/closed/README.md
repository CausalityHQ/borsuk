# Closed source-utilization analysis — a0002

Exact candidate 44e116b9b8754cef0393fad1f96259bd583f3b7b passed 33 native tests, locked release, workspace Clippy correctness/suspicious, and actual unshimmed workspace test compilation. Full 425-file source matched before/after. Instance i-0e69784d7ebc74107 terminated and waited before independent verification. Compiler-failed a0001 remains immutable.

Rust replay authenticated all 1,000 closed B1 records. No ANN, vectors or truth bodies opened; this is cost accounting, not latency/recall evidence.

| Quantity | Recorded SOURCE + SQ8 | Counterfactual direct closure SQ8 |
|---|---:|---:|
| Logical GETs, total | 51,943 | 26,938 |
| Payload bytes, total | 25,671,329,664 | 35,315,366,912 |
| Direct bytes per query, median / p95 / max | — | 35,273,728 / 40,843,264 / 48,799,744 |

Direct reads remove 25,005 requests (48.14%) and add 9,644,037,248 bytes (37.57%). All 1,000 direct plans exceed the historical 16,773,120-byte SQ8 cap. No new envelope has been admitted.

**Decision:** stop finer SOURCE-unit packing for this workload: scored rows fill the existing closure; hindsight 32-row cover saves zero bytes for every query. Do not select direct closure as a production default on accounting alone. Evaluate one generic Rust direct-SQ8 fetch candidate only with a prospectively admitted envelope, actual scalar ranking/bridge/tail correctness and frozen real-data recall falsifier. Different gap bridges can remove incidental rows; larger nominated closure does not prove recall parity. A successful candidate then needs a matched cold run and lifecycle-cost measurements.

Existing cold BORSUK numbers remain recall@10 97.23%, p90 116–119 ms, p95 124–127 ms, serial QPS ~10.1. Saved S3 recall 96.56%, p90 118.479 ms, p95 149.056 ms, QPS 13.754; backend cold conditions differ. Turbopuffer 10M published results are not population-matched. No overall measured win.

Original raw evidence remains under research/semantic-router/20261008/cold-source-utilization-a0002 in the benchmark S3 bucket. Source archive SHA 08868242ab8285fccf56b61b7d9eb94828fb8a6b5bc2bc1a7f6c753781cf9884; evidence archive SHA 158de7ecc9b697ce130e7860e654032f9259edb7c4670f2fc49ead66060dab93. Independent verifier runs local bounded metadata checks; numerical analysis runs Rust remotely.
