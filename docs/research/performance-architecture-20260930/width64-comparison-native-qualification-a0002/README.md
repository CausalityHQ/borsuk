# Exact-source width32/64 comparison — native qualification

The Rust comparison reader adds --paired-width64-scale with configuration schema borsuk-scale-width64-native-reduction-config-v1 and report schema borsuk-scale-width64-native-reduction-v1. It requires ordered widths32/64 and identical binary, source, generation, backend, scoring and input pins. Only width/configSHA differ. Full sealed1000-query results must retain exact IDs, score bits, recall, trace, physical plan and logical charges. No source/binary exceptions are admitted.

Source f4d66fc784ace5bec6401614b5197173bc15ed05 passed9 remote stages: compile; new exacttest1; scale5,paired4,completed4,scheduler1 regressions; release; workspace correctness/suspicious Clippy; actual shim-unset workspace test compilation with jobs1. All470 support and437 native blobs match frozen source before/after every stage. CPU0–3/400%,14GiB/swap0/pids512; no swap/OOM. The unrelated historical training fixtures were not executed.

Test-first source0533023b failed exactly on missing schema. Initial implementation c1bdbd38 compiled but its new fixture incorrectly reused the configSHA for both widths; the existing distinct-pin guard correctly refused. Separate fixture repair f4d66fc7 supplies distinct pins and adds an explicit identical-pin refusal. Production guards were retained. Both earlier attempts and their cleanup remain immutable historical evidence.

Release ELF9,196,824 bytes, SHA256 955210af66106974db10b92ecfabc307719d0eb4d26534e1d1c9596ea4b83842. Exact full logs/manifests and ELF are under s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261013/source-width64-reducer-native-a0002/stages/<stage>/. This mirror preserves small original receipts. The original command ended Success0; i-0406e77df72c7571d terminated, vol-0e5041186b5d283ea deleted and external timer stopped.

This is compiler/parser correctness evidence, not actual corpus recall, performance, vendor comparison or product readiness. Next: same qualified baseline ELF for both widths, real ordinal527 admission, separate ordinal0 canaries, then frozen all1000-query ABBA32/64/64/32. Preserve all results and apply the cold hill-climbing policy.
