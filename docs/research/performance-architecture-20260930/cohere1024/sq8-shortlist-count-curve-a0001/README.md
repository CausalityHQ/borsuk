# SOURCE shortlist count curve — 2026-10-15

## Decision

Advance the **256-row shortlist** to production library integration. It improves recall over the frozen baseline while reducing planned SQ8 reads and bytes. Retain 512 rows as the higher-quality frontier point. This decision does **not** adopt a production default or establish a cold-latency improvement.

| Choice | Recall@10 | Hits / 10,000 | SQ8 GETs / 1,000 queries | SQ8 bytes / 1,000 queries |
|---|---:|---:|---:|---:|
| Frozen baseline | 97.80% | 9,780 | 29,903 | 16,708,608,000 |
| 256 rows | **97.86%** | 9,786 | **28,358** | **11,497,411,968** |
| 512 rows | 97.89% | 9,789 | 29,716 | 20,971,524,224 |
| 768 rows | 97.89% | 9,789 | 29,944 | 25,994,947,328 |
| 1,024 rows, control | 97.89% | 9,789 | 29,982 | 28,671,109,376 |

The 256-row arm gains six baseline hits and displaces none. Relative to baseline it reduces planned SQ8 bytes by **31.19%** and planned GETs by **5.17%**. Its average SQ8 charge is 10.965 MiB per query; SOURCE and router traffic are separate. All four arms exceed the fixed 9,760-hit floor. The 768- and 1,024-row arms have the same aggregate recall as 512 with more planned traffic. No arbitrary byte-ratio gate was used.

## Protocol and scope

- Same frozen Cohere Wikipedia corpus: 1,000,000 rows, D1024, cosine, k=10, 1,000 queries and 10,000 ground-truth IDs. See `config.json` and `transport.json` for immutable pins.
- Source: `6fb66100c6b9c98bc8892b8c2a6585bd6e6c90d4`. Only shortlist count varies. SOURCE128 scoring, routing, normalization, SQ8 record bytes, original physical order and the greedy 32-row block-admission rule remain unchanged.
- All 4,000 query/arm cells are complete and `MEASURED`. Valid points remain recorded regardless of the quality floor. Current arms retain ordered result IDs, score bits and target ordinals.
- The 1,024-row control reproduces the historical report's per-query geometry and quality summaries. That historical artifact omitted ranked results, so **historical returned-rank parity is not proved**. Baseline replay separately checks exact plan, charges, IDs and score bits.
- These are planned authenticated SQ8 range charges from an offline native diagnostic. They are not observed physical S3 requests, billed bytes, cold query latency or QPS. No vendor win is claimed.

## Verification

The exact Rust revision passed eight remote gates: count fixtures (7), reader fixtures (9), coordinate fixtures (5), selector regressions (36), the actual shim-unset workspace test build, workspace correctness/suspicious Clippy, release reader fixtures (9), and release example build. See `qualification.json`.

The qualified release ELF is 12,349,968 bytes, SHA-256 `9027abb7e8b208e9e6e4a3687ed9cf22c0468eae45795484a2b8c5b538c10015`. Before science, the native validator authenticated the actual corpus/layout, all requests and truth, followed by a separate one-query/four-arm smoke with truth unopened. See `actual-input-and-smoke.json`.

Full run: instance `i-0e12219d7564b6695`, SSM `6a12bbd1-fa41-46fa-8160-04a443d5a622`, profile `causality`, region `eu-central-1`, Spot `m6a.2xlarge`. Native limits: CPU0–3, 8 GiB, no swap, 512 tasks, 1,800 seconds. Offline wall time was 314.44 seconds; RSS 417,548 KiB; cgroup peak 429,547,520 bytes; no swap or OOM. These resource figures describe the diagnostic, not product query RAM or QPS.

Independent closed-run verifier invocation `70a68e91907e421a8715ba8886a58e6e` exited 0. The instance is terminated, volume `vol-0a6777f9043430ef4` is absent, and the external timer is inactive. The native report SHA-256 is `31dfa30cbf176254e7bec7cc8eb5288071f85cfbb9bb98455b61b3c87f5ec786`. Complete scientific rows remained opaque locally; `native-summary.json` is the native aggregate pass-through, checked against native stdout.

Authenticated terminal evidence is retained under `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261015/sq8-source-count-full-a0001/terminal/` (`terminal.json`, `evidence.tar.gz`, `evidence.tar.gz.sha256`). Qualification and canary prefixes are respectively `sq8-source-count-repair-a0001` and `sq8-source-count-canary-a0001` under the same dated prefix. Original failed attempts and prior arms remain immutable.

## Falsifiable next gate

Implement the 256-row policy with generation-bound 32-row SQ8 proofs in the Rust library, preserving SOURCE scoring, routing, exclusions, shared memory/concurrency admission and failure drain. Prove native rank/score-bit parity, final partial blocks, corruption/cap refusals, restart, publication and compaction behavior. After exact-source qualification, actual-input admission and disposable smoke, run a frozen paired cold S3 baseline/candidate comparison. Actual p90/p95, throughput, RSS, physical requests/bytes and lifecycle cost decide adoption.

The current cold reference remains 97.80% recall@10, p95 174–176 ms and approximately 6.9 sequential QPS. There is no matched measured win against S3 Vectors or Turbopuffer. A floor pass alone does not establish equal-quality vendor performance.
