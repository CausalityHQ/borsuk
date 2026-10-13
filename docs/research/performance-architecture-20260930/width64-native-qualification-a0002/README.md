# Width64 baseline admission qualification — 2026-10-13

The Rust benchmark baseline now accepts explicit fetch widths 16, 32 and 64. The default remains 16. The generic library already accepts bounded positive concurrency; this slice changes the benchmark selector and its tests, not routing, scoring, persistent layout, cache behavior or production defaults.

Source `fbebe3dd3a4cf512f4e046689626e16c482d624a` passed eight remote gates, including the three individually selected admission/selector/pool tests, all 43 baseline regressions, release build, workspace correctness/suspicious Clippy and the actual shim-unset workspace test compilation with jobs=1. All 470 support blobs and all 437 native blobs match the authenticated frozen source before and after every stage. CPU0–3, quota400%, memory14GiB, swap0, pids512; no swap/OOM. The process pool test queues128 jobs at widths16/32/64 and verifies the cap and complete drain.

GET, byte, library memory and active-query limits remain fixed: SOURCE128GET/64MiB, SQ832GET/16,773,120B,512MiB library and one active query. Effective fetch width remains bounded by each batch's request count. Width64 has not been measured on the retained real corpus. No recall, latency, throughput, vendor-win or product-completion claim follows from these synthetic correctness gates.

The original native command ended Success0. Instance `i-0fbdfe9a3868e6206` terminated, root volume `vol-054926d0c862eff3c` was deleted and the external timer stopped. Collection initially timed out on one receipt; missing/unverified files were resumed and every manifest rechecked. No native job was repeated.

Retained baseline ELF:17,112,816 bytes; SHA256 `3dfad91671d20bb6501ca4050af047ac877e8ae20510deb9fc5598aad2513655`. Exact full logs and stage manifests are immutable under `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261013/source-width64-native-a0002/stages/<stage>/`; source inputs are under the same campaign's `inputs/`. The small receipt mirror here preserves original bytes, including original local/remote paths.

Next: qualify the Rust comparison reader for ordered widths32/64 with the same source and ELF in both arms; run real ordinal527 admission and separate ordinal0 canaries; freeze all1000-query ABBA32/64/64/32 measurement. Require exact IDs/score bits/trace/physical-plan/logical-charge parity. Retain all results. Runtime selection follows the preregistered cold hill-climbing policy; no default promotion from synthetic qualification.
