# Real 1M paired importer admission

| Gate | Original result | Disposition |
|---|---|---|
| a0001 setup | Failed / 1; native unrun | Environment INVALID: nominal 8 GiB host reports 7,962,196 KiB; incorrect lower classification threshold |
| a0002 setup | Success / 0 | All 30 transferred bodies authenticated; tracer package identity and integrity verified |
| Consumer canary | Native 2 / INVALID; manager 0 | Expected wrong-config-SHA refusal |
| Real paired import | Native 2 / INVALID; manager 0 | Both actual publications validated and every unit group compared before exact absent-query ENOENT |
| Corrupt membership | Native 2 / INVALID; manager 0 | One changed byte refused before requests; approved original restored |

All 133 retained objects (754,886 bytes) passed server SHA/length checks and exact per-stage manifests. Both attempts' instances and volumes are closed; watchdogs stopped. Native source 043ee882 is byte-identical through the accepted glue chain 898adfee → 4a34513b → 45ea675c. Existing eight-stage Rust qualification remains applicable; this change adds only three Bash experiment scripts.

The traced import prefix took 0.91 seconds with 262,360 KiB maximum RSS on this local-file admission host. This is engineering admission evidence, not query latency, QPS or cold S3 performance.

Acceptance covers the source/layout import prefix and consumer/platform refusal mechanics only. The native INVALID results remain unchanged. Queries, truth and SQ8 were not staged or opened. No SOURCE recall, returned-quality, held-out, cold, default-promotion or vendor claim follows.

Next: freeze one paired SOURCE screen using the existing 32 consumed ordinals and unchanged producer controls, seal selections before truth, then assess returned quality and cold performance separately.
