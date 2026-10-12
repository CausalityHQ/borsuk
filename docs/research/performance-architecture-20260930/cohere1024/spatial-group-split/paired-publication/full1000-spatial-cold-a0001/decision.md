# Full1000 native S3 measurement: spatial Pareto candidate

Exact source023e12a59d4cc99f1fae23351b8fa11960807696; unchanged native tree
qualified at ddbcd91cf8e5e0388eaa15ab8d7a20032428936f. Cohere Wikipedia1M,
D1024 cosine k10, same original1000 query/truth bytes for both arms.

| Arm | R10 | p50 ms | p90 ms | p95 ms | p99 ms | Sequential QPS |
|---|---:|---:|---:|---:|---:|---:|
| Radial | 97.76% | 330.513 | 374.702 | 396.489 | 484.712 | 3.05884 |
| Spatial | 97.80% | 321.151 | 363.006 | 377.895 | 428.037 | 3.14763 |

Native Rust reductions independently authenticated exact closed whole-file
headers/seals/1000 query records/terminal. Zero underfilled queries. Source-bound
ordinal527 admission passed on both arms before separate ordinal0 canaries;
this specifically reconciles the original closed512-page structural failure.
Fresh1m historical limits remain unchanged; qualified Scale1m structural bound
3072 is separate from unchanged SOURCE64MiB/128GET and SQ8 16773120B/32GET.

Spatial total stage intervals over1000 queries: discovery1.460s, source217.679s,
planning19.385s, SQ879.122s; query wall317.699s. These include stage auth/fetch/
compute responsibilities and are not network-only timers. Source fetch/auth is
the largest observed stage. SIMD on routing alone cannot remove the majority.
A fetch16/32 parity falsifier is the next justified causal speed intervention.

Spatial cumulative logical charges: SOURCE107882 submitted GETs/27428966400
verified bytes; SQ829903 GETs/16708608000 bytes; zero failed GETs. These are
logical charges, not wire/billed totals. Query process CPU98.819s.
Spatial peak native process RSS67048KiB; cgroup peak96518144B. Query host and
modeled cap512MiB, CPU0/100%,swap0,pids128,fetch16,query concurrency1.

All12 stages native0/manager0, exact order, cgroups drained, swap/OOM0. Original
SSMc38a78e0-a26d-4c2f-9cd2-2117650e7ec3 Success0 in667.497s. Spot instance
i-082ae65ec20d6a897 terminated at terminal; root volumevol-0b8fe01d664551875
absent; external timer inactive; BGoff. Original complete rows retained in S3,
not decoded or re-reduced locally; only native-produced reports were read.

Cache scope: payload disabled, router resident, SDK connections reused,
provider cache uncontrolled. Sequential QPS is not concurrent service capacity.
Order/repetition effects are unqualified. No matched1M S3 Vectors result exists:
its qualified runner is pinned to100k with different input hashes. Published
Turbopuffer10M references lack matching IDs/split/region/recall. No vendor win,
production readiness, default promotion or total lifecycle cost claim.

Spatial improves observed recall and tails versus radial in this one paired
population run, so retain it as the next Pareto candidate. Its97.80% R10 does
not meet an internal98% target; that fact is preserved. Do not silently relax
quality or reinterpret100k service recall as a1M matched floor.

Evidence:
s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261012/full1000-spatial-cold-a0001/terminal/
Next: same generation/source, fetch16 versus32 with exact ID/score-bit/trace/
logical-charge parity; independently qualify the matched1M Rust S3 Vectors
adapter. Maintenance/scalability/product qualification remains outstanding.
