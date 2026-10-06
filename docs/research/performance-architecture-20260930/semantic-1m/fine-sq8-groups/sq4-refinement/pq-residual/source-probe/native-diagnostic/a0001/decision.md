# PQ residual source probe: REJECT

Valid completed native run, not an environment failure. Original process exit 0; all 33 terminal bodies independently hashed and all 128 sealed source results recounted. Same instance i-03af7b00dc59cac50 terminated. No requests or ground truth opened.

| FIRST100k D768 source, fixed4096 cohort/64 source anchors, k100 | Top100 intersection mean vs unchanged native SQ8 | Fourth-smallest hits | Frozen requirement | Result |
|---|---:|---:|---|---|
| ReLAION | 98.234375% (6287/6400) | 96 | >=99%, >=98 | REJECT |
| CoHere | 96.296875% (6163/6400) | 93 | >=99%, >=98 | REJECT |

Whole native probe wall 4.328770359s; cgroup peak 76,423,168 bytes; swap/OOM zero. This includes source authentication, training, cohort encoding, all scoring and closure; it is neither per-query serving latency nor QPS. Total lifecycle cost not measured.

Stop this frozen refinement arm. Do not run its matched GT experiment or tune bits, bins or cohort after observing failure. Compiler-qualified codec remains experimental; it is not a production default or quality claim. Preserve unchanged SQ8 as the reference and use the closed failure in the next architecture decision.
