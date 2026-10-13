# Native byte-window benchmark qualification

Decision: NATIVE_QUALIFIED on candidate `8a01f6dee985d905757e11ab9e02f93617045a73`, based on public `2f049daf44dfcbee820b8648b4b7764f891ea0d0`. Cold performance is UNMEASURED.

The Rust baseline now accepts an explicit optional shared SOURCE/SQ8 download-byte window. The generation constructor admits its actual controller state against remaining memory before allocation. Unconfigured behavior is preserved. Routing metadata, leaves, authentication, scoring and query concurrency are outside this window.

The four-run A1/B1/B2/A2 reducer authenticates both runtime configurations and all four sealed runs. It requires exact returned IDs, score bits, trace, physical plan and logical charges. It rejects transport failures, invalid state accounting, duplicate evidence and changed authority. Its prospective screen requires the 97.80% recall anchor, a potentially binding window, positive p95 improvement in both pairs and at most 5% p90/QPS regression. It preserves measured negative results. Chronology, binary qualification, cache state and lifecycle cost require separate root evidence; the reducer establishes no vendor win or statistical significance.

| Native gate | Result |
| --- | --- |
| Affected library/bin/example compilation | 0 |
| Baseline byte-window fixtures | 2 passed |
| Real HTTP generation fetch-path parity | 1 passed |
| Four-run reducer fixtures | 7 passed |
| Baseline regressions | 46 passed |
| Comparator regressions | 186 passed |
| Generation regressions | 33 passed |
| Release baseline + comparator | 0 |
| Workspace correctness/suspicious Clippy | 0 |
| Actual shim-unset workspace test compilation | 0 |

All ten stages ran on identical authenticated 470-file / 437-native-file source. Independent manifest, exact-command, full test-roster, source-before/after, resource, zero-swap, zero-OOM and drained-cgroup checks passed. Native execution used causality EC2 Spot `i-08ce32be93a23780f`, CPU0–3, 14GiB memory, swap0 and compiler jobs1. Original native SSM `6a2bda1c-d61d-43e2-bdcf-0d0cbf561422` completed Success/0. Original watcher completed 0: instance terminated, root volume `vol-0a53a6afe2e42c6dc` absent, external timer inactive. No duplicate execution was launched.

Retained release artifacts:

| Role | Bytes | SHA256 |
| --- | ---: | --- |
| check_cohere_native_baseline | 17488856 | a720d55350eed11b2fcba24b9a91e51d29e24070803cbc3c28cdabd83bdee033 |
| compare_native_replay | 9451904 | c320a05d4b95af56e3430fd598dc801c529e3bb1986cdf10e739a2f953e46155 |

Bodies remain under `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261013/byte-window-benchmark-green-a0001/binaries/`. Opaque downloads independently matched both lengths and hashes; they were not executed locally.

Next gate: extend the native interval diagnostic to retain largest individual SOURCE and SQ8 ranges from the existing authenticated closed width64 run, without opening corpus/query/truth bodies. Choose capacity at least that maximum and below some phase totals. If no distinct binding arm exists, record that outcome rather than run a no-op campaign. Then source-bound real-input admission and disposable staging smoke precede frozen cold ABBA measurements. No speedup follows from these correctness gates.
