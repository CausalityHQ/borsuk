# Direct SQ8 closure: real-input admission

The qualified Rust candidate 2fb771bdb31f9490a3341bdbfa2b4ad510cfb487 passed both full 1,000-query real-input arms on the frozen 100k Cohere D1024/cosine/k10 cohort. Original instance i-0be614b9c2b3b60e5 was terminated and waited before collection.

Baseline: 9,723/10,000 hits. Direct closure: 9,724/10,000 hits (97.24%); no query lost hits and one gained one. Both sealed all results before truth. Baseline IDs, score bits, traces and charges match both historical B1/B2 records. Every direct GET/byte plan matches the prior closed counterfactual audit.

GETs: 51,943 baseline to 26,938 direct (-48.14%). Fetched bytes: 25,671,329,664 to 35,315,366,912 (+37.57%). Query SOURCE GETs are zero in direct mode; startup authentication remains. Direct modeled memory is 298,129,247 bytes against 512 MiB. Kernel limits and closed resources passed with no OOM, swap or leaked scratch.

This is input/resource/quality admission, not a formal cold timing comparison or a vendor win. Next gate: separate disposable staging/CLI/cleanup canary, then freeze one baseline/direct cold comparison. Existing S3 reference is retained; Turbopuffer published workload is not an exact matched population.

The first local closed-record audit had an unset service WorkingDirectory and failed before source lookup. Setting the exact repository WorkingDirectory repaired the audit environment; the unchanged verifier then exited 0 in 841 ms, 167.4 MiB peak and zero swap. No remote algorithm or limits changed.
