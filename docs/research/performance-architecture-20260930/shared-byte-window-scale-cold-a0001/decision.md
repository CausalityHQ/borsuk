# Shared 9MiB window: measured slowdown, preserve width64 control

The exact frozen Cohere1M/D1024/cosine/k10 corpus, reserved 1000 requests and truth, native generation/SOURCE/SQ8/order pins, source cache off and one active query were used. Source806a4520; qualified baseline a720d553 and comparator a22578c6. Only the candidate's shared SOURCE/SQ8 window (9437184 bytes,64 waiters,1048576 modeled state bytes) differs; fetch width64, source GET budget128, routing, scoring, quality target and all other native configuration fields are identical.

| Run | Arm | R10 | p90 ms | p95 ms | p99 ms | Sequential QPS | Native peak RSS bytes | First query ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| A1 | width64, no window | 97.80% | 173.827857 | 192.374104 | 242.183108 | 6.752179676 | 83226624 | 170.966897 |
| B1 | width64, shared9MiB | 97.80% | 233.769719 | 250.793940 | 296.838161 | 4.844621897 | 78630912 | 221.008274 |
| B2 | width64, shared9MiB | 97.80% | 233.702362 | 251.890786 | 288.871462 | 4.860179363 | 75857920 | 215.270645 |
| A2 | width64, no window | 97.80% | 170.483828 | 185.014933 | 246.699244 | 6.770863513 | 78249984 | 193.323146 |

All4000 queries passed exact returned-ID/score-bit, semantic trace, physical plan and logical GET/byte parity. Every arm9780/10000, underfill0; logical137785 GET and44137574400 bytes. The Rust ABBA report is MEASURED/complete and performance_screen_passed=false. Native p95 ratios B1/A1=1.3036782747, B2/A2=1.3614619205; QPS ratios0.7174900743 and0.7178079064. Both independent pairs regress decisively relative to the declared modest-win screen. Within-arm p95 A2/A1=0.9617455216 and B2/B1=1.0043734948; this is a noise reference, not a statistical-significance claim.

SOURCE p95 controls94.665631/97.878920ms, candidates134.551208/135.679720ms. SQ8 p95 controls81.778437/72.036703ms, candidates102.131710/105.244599ms. These component percentiles cannot be added to produce query p95. Query CPU sums controls91.982799242/96.556060879 seconds; candidates101.043684045/98.820637781 seconds. RSS is modestly lower in the candidate, so preserve that memory/speed Pareto point. The result does not prove an ENA shaper, TCP retransmission, observed permit waiting or a unique internal cause.

## Disposition and next causal gate

Keep width64/no shared window as the speed baseline. Do not adopt9MiB as a default. Preserve the qualified optional byte controller as a library resource-control capability and this negative measurement as immutable evidence; do not rewrite its scientific result or lower recall to turn it into a pass.

The next narrow scheduling falsifier is SOURCE GET budget96 at width64 with no shared window, based on the already qualified native cover curve (average107.882->92.222 SOURCE GET,27.429->29.493MB; all1000 within64MiB). The library already admits this generic read limit. Only benchmark selector/authority and native paired validation need a new exact-source qualification. This is not yet measured or launched. Preserve IDs/scorebits, routing and SQ8 while validating the changed SOURCE cover through the native planner; unchanged9780-hit anchor. Do not combine it silently with9MiB.

Native-format research's smaller authenticated SQ8 blocks plus a shortlist remains a separate algorithm/layout falsifier; its smaller-byte estimate is not a result. Serving is already Rust-native. Parquet/Arrow is not in this serving hot path. A prospective algorithm arm may use the separately accepted97.60% floor; this schedule experiment has no quality relaxation.

## Verification, provenance and comparison limits

Original instance i-08e4b371e275a53a3, c7i.2xlarge Spot/eu-central-1a, matched retained AMI and96GiB encryptedgp3root vol-0b031deb4633e425a. Original setup6f37ddb6-d339-4aab-a8dc-72787f920642 and run36317685-6e4d-457c-8855-29e12edb2b5f Success/0. Original watcher81802exit0: instance terminated, root volume absent, external timer inactive, BGoff. Original collector17050exit0; root verifier32951exit0. Exact17 ordered native stages, allnative0/manager0/drained/no forced cleanup, unchanged before/after binary/config/input hashes, CPU0/100%,1GiB/swap0/pids128, OOM/kill/swap0; exact native four-run authority and9780-hit recall verified.

Query wall includes native query work and excludes process startup/external ingress. S3 service cache eviction is unproved. First query is reported separately. Wire/billed bytes and total lifecycle dollar are not measured. A lower p95 on this client protocol does not establish a matched end-to-end vendor win. Retained S3 Vectors was96.10% recall,215.0226ms p95,7.35836 sequential QPS under its disclosed different cache/timing details. BORSUK control has higher recall/lower p95 but lower QPS. Turbopuffer has no matched reproduction.

Complete raw sealed evidence remains create-only at s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261014/byte-window-scale-cold-a0001/terminal/evidence.tar.gz; the exact SHA256/length and get receipts accompany the committed native reports. No incomplete measurement rows were inspected. The original collection helper finished before any replacement; its repeated archive decompression was identified as local evidence-handling overhead, not ANN timing. Future collection should use one metadata-only extraction pass.
