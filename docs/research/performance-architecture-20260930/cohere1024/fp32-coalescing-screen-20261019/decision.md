# Canonical read coalescing: closed first-32 screen

## Decision

Retain the opt-in GET/byte Pareto option; leave the default off. No consistent latency improvement is established, so do not scale this arm to a full-1000 performance run solely from this screen. No vendor, lifecycle-cost or fully cold backend win is claimed.

Source: `70c1c1d094b2ab926ec38fd9220d78ea1ecfcbf1`. Same qualified binaries, generation and frozen first 32 development queries; cosine k10, Cohere Wikipedia1M/D1024. A reads 16 individual canonical records; B coalesces within 64 covered records/262656 bytes. Both allow 16 logical ranges and 16 concurrent reads. Order A1/B1/B2/A2, fresh processes/source cache off, provider cache eviction unproven. Percentiles use nearest rank. These are 32 repeated queries, not 128 independent samples or production tail qualification.

| Arm | Recall | p50 ms | p90 ms | p95 ms | Serial QPS | Canonical p50 ms |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 319/320 | 195.621 | 295.304 | 363.005 | 4.876 | 34.604 |
| B1 | 319/320 | 154.379 | 185.900 | 196.550 | 6.336 | 33.017 |
| B2 | 319/320 | 142.940 | 185.466 | 234.144 | 6.526 | 32.876 |
| A2 | 319/320 | 141.482 | 171.915 | 183.228 | 6.825 | 32.486 |

All 128 query records independently match reference IDs, physical ordinals and sequential-f64 score bits; raw truth recount gives 319/320 in every arm. Complete selected SQ8 rosters and upstream plans/charges are identical. Independent gap planning matches every range and canonical charge.

Per 32 queries, canonical reads fall 512->328 (-35.9375%), while canonical verified bytes rise 2101248->6968592 (3.3164x). Total serving logical GETs fall 4189->4005 (-4.3925%); total verified/consumed query bytes rise 905267328->910134672 (+0.5377%). SOURCE stays2826 GET/749506560B and SQ8 stays851 GET/153659520B in every arm. Native query transport records the same attempt counts, only206 responses, and zero transport/stream failures. These counters exclude startup and are not wire or billed bytes.

The A control p95 drifts363.005->183.228ms. B1/A1 p95 ratio0.54145 and B2/A2 ratio1.27788 disagree. Canonical p50 stays about32.5-34.6ms; process CPU is about2.95-3.10s/32 including startup. No cause of the order/control drift is proven, and a first-cell improvement cannot be selected as a win. RSS is about100MiB; this is an observation, not an allocator bound.

## Qualification and provenance

11 library tests,4 producer/reader/writer tests and1 schema-binding test passed, plus workspace all-targets Clippy correctness/suspicious, actual env-unset workspace test-build and release runner/replay. Result format is v21; every bridge record is authenticated but never scored. Coalescing checks its byte-window envelope before upstream I/O. Actual-input admission and separate canaries passed before timing; each closed cell was checksummed to S3 before the next.

Immutable screen terminal: SHA256 `45f75f97ffc9ee83e75c52e73d874d631e3b495d8405d790c75e241c46add2c5`; archive `aaf68f058eb75d30ec5cc70024de7d6cd673baa53a7e1a1f7c59758315e6cb5a` under `research/semantic-router/20261019/fp32-coalescing-screen-closure-a0002/`.
Independent verification: `67639dd59ea3f650d674e8dd23063232cb436de2f5522a335a934fb629d3458a`; supplement terminal `cffc69a097bb2a1fd96e3f22ae13e8b374f65d005a7bbde7ac5e3c25ecc0a244`. Its null process-last-boundary display was not used for a transport claim; `transport.json` extracts the actual per-query before/after counters.

Prior window a0001 passed admission/canaries but measured no cells before its declared launch cutoff. Preserve that operational disposition; it is not an algorithm rejection. Original instance i042 was terminated; this screen used Spot i-03574ebc6188ad0f5 with retained EBS,8CPU/8GiB/swap0 and an unchanged qualified source.

## Next falsifier

Prioritize evidence for the dominant upstream stage rather than another larger coalescing run. A fresh contextless planning consultation `f6d9a0289f73439b` was requested from the closed metrics; its verdict is pending. Select one CPU/stage or quality-loss falsifier using the existing qualified runner where possible, then implement only a measured causal intervention. Preserve the prior full1000 quality gain9803/10000 vs9746/10000 under its original source/protocol; do not relabel this first32 result as a population gain.

This follows an adapted [autoresearch loop](https://github.com/karpathy/autoresearch): keep evaluation fixed, make a bounded intervention, record outcomes, and retain useful measured tradeoffs. Here correctness and recall are fixed, and request/byte/latency tradeoffs remain separate; no single score hides them.
