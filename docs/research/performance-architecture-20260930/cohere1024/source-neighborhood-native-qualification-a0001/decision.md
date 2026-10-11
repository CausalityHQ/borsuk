# Query-free source-neighborhood native qualification

Accept the exact Rust example source from d72fd7a6365acf02b75b068de1844c045baf8904 for compiler and synthetic correctness qualification. The final slice changes only compare_native_replay.rs; production library, dependencies, serving layout and defaults remain byte-identical to the prior main snapshot.

| Gate | Result |
| --- | --- |
| Independent source reviews | Engineering Astra and research Sol fallback GO for native qualification only |
| Neighborhood debug/release | 17 exact names in each stage; all pass, zero ignored |
| Prior boundary/coverage/seal regressions | 4 + 8 + 4 exact names; all pass |
| Optimized example build | Exit 0; retained ELF bytes/SHA pinned in qualification.closed.json |
| Workspace/all-target Clippy | Correctness and suspicious categories denied; exit 0 |
| Complete workspace test compilation | Actual shim-unset check_rust_test_build.sh, jobs 2; exit 0 |
| Source/resource/closure | All 463 paths before=after=root, CPU0-3/16GiB/zero swap, OOM0, every cgroup drained |
| Infrastructure | Same original Spot instance terminated+waited; root volume deletion verified; external timer inactive |

Eight stages completed on one causality EC2 attempt. There are 50 named test passes (34 neighborhood plus 16 preserved regressions), not a claim that the entire workspace test suite executed. Source/checkpoint and both reviews are preserved. All closed-stage bodies were authenticated against their original SHA256SUMS; selected receipts are committed here and the full immutable bodies remain under research/semantic-router/20261011/source-neighborhood-native-qualification-a0001 in borsuk-bench-453182569524-euc1. SHA256SUMS files intentionally describe the full remote stage roster, including logs and the ELF, rather than only this selected local evidence subset.

## Next falsifiable gate

The 1GiB charged RAM / 8GiB scratch / CPU0 / 1200s query-free diagnostic proposal is still unqualified. First admit the exact authenticated canonical, generation, plane metadata, router, membership and leaves through the qualified Rust runtime on bounded causality EC2; requests, truth, raw corpus, SQ8 bodies and producer outputs stay unopened. Pass a separate disposable smoke before one frozen scientific source-neighborhood diagnostic. Local reduction of vector/report arrays is prohibited; read only the completed Rust scalar human summary after terminal acceptance. A surviving report alone never establishes success: require original exit 0, pinned seal/report/source/config/ELF, resource/OOM/drain receipts and infrastructure closure.

The diagnostic is exploratory and in-sample. It does not qualify graph cuts, held-out recall, cold performance, vendor parity or 100M feasibility. Both prior 312/320 KILL arms remain immutable; do not repeat or tune them. Product acceptance remains matched recall, end-to-end cold tails, throughput and total lifecycle cost plus the complete library maintenance/recovery requirements.
