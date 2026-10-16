# Fixed32 SQ8 diagnostic a0001

**Valid, closed diagnostic; mechanism unresolved.** This is not a population
p95/QPS measurement, a vendor comparison, or a default-adoption decision.

Frozen runtime source: `898b3fcf82f75085d8b01cd2e462652db34dcd41`.
The A/B actor binaries and scientific thresholds were unchanged. The qualified
Rust reader authenticated the closed files and within-actor trace-off/on parity.
All 22 stages / 44 START-END records passed; peak tasks 62/128, OOM and swap zero,
all owned units drained without forced cleanup. Actual instance
`i-02d2118e1a18e2675` terminated; volume `vol-09f83adc00c3243fa` is absent and the
external timer is inactive. Root verification exited zero.

## Observations

Two of the 32 frozen ordinals met the preregistered SQ8-excess reproduction rule
in both traced pairs. Values below are B-minus-A milliseconds from the completed
native report, rounded for display. Columns show pair 1 / pair 2.

| Ordinal | Extra SQ8 | Extra fetch-to-last-completion | Extra after barrier |
| --- | ---: | ---: | ---: |
| 483 | 29.55 / 31.27 | 24.12 / 24.61 | 5.44 / 6.65 |
| 773 | 41.88 / 48.02 | 30.91 / 36.90 | 10.97 / 11.12 |

Neither ordinal met the post-barrier >=80% falsification rule in both pairs.
The native report labels their evidence `UNRESOLVED`. This leaves the
predominantly-pre-barrier account unfalsified for these traced observations;
it does not prove an S3/network cause. Header time includes client/connect/TLS/
server effects, and body gaps include polling/scheduling. The largest B body
gaps overlap authentication of another range; temporal overlap is not causal
attribution. Critical-range body, copy and authentication evidence is retained.

The untraced/traced timing differences are descriptive, not proof of timing
neutrality. Server caches were uncontrolled; the separate fixed-panel canary
accessed the same queries before formal runs. Deterministic32 selection and two
pairs do not establish statistical independence or the historical population tail.
No queries were replaced and no thresholds were changed after observation.

## Evidence and next step

SSM setup `4e5c4d05-acf1-40f6-b4a4-707420a4176f` and native
`cba9a540-9b2d-46a0-b827-229a32ce398a` both closed successfully. Source-bound
controls exercised real systemd ownership/drain, nonzero/timeout handling,
publication failures and duplicate partial-receipt preservation before scientific
inputs were transported. Their synthetic actors/AWS spies are not ANN evidence.

Archive: 7,230,615 bytes, SHA256
`8794d773c99ccf3323cda692fead6377609e521196f355e653ce2290f2ae93fe`,
at `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261015/sq8-panel-diagnostic-a0001/terminal/evidence.tar.gz`.
The terminal marker was published last. Root admitted 1,204 archive entries,
156,109,551 regular bytes and the single source-declared summary hardlink;
all payload hashes passed. Raw scientific JSONL was not parsed locally.

Engineering review `24d9a2b2a7c94cfe` and manual research fallback
`89b2b406e58d40fd` supported a bounded canary. Original research job
`4666f118d05249d7` contained only a quota banner and is not a substantive review
or dual-review pass. Root explicitly authorized the complete sequence conditional
on the actual control/admission/canary/freeze gates.

A contextless source specialist (`4524fb16f68b45b5`) is deriving one next causal
intervention/falsifier from this evidence. Investigate download/polling/copy/auth
interaction and range shape before inferring a storage-service cause or selecting
an unprofiled compute rewrite. Any targeted slow-query falsifier must be followed
by a separately frozen full1000 confirmation before population claims.

The previous full1M quality/latency tradeoff remains unchanged historical evidence.
The Rust GC source is separately packaged for native correctness qualification;
this diagnostic does not qualify lifecycle behavior or complete the product goal.

Root's first optional summary-render query used `keys` on the report's string
`evidence` field and failed; the corrected rendering read that string directly.
This changed no native input/result, threshold, verifier or historical disposition.
