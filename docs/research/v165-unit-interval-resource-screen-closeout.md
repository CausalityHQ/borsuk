# V165 32-row-unit interval resource screen: closeout

## Decision

**Kill the pure unit-only schedule at the preregistered resource gate.**
On used ReLAION-1M D768 validation-1000, changing the V164 physical fetch
atom from 512 to 32 SQ8 rows while keeping the positive-vote maximizing
V114 planner reduced bytes, but still planned **13,545,018,240 bytes**
over 1,000 queries. That is **21.65% above** V155 cached sparse's
11,134,007,040-byte paired resource limit. Planned GETs were **17,696**,
below V155's 22,126. Every query obeyed the same 32-GET/16,777,216-byte
caps and the replay passed, but the joint resource gate failed.

| Used ReLAION-1M D768 validation-1000, planned SQ8 only | V155 cached sparse baseline | V164 smooth 512-row pages | V165 smooth 32-row units |
| --- | ---: | ---: | ---: |
| Bytes / 1,000 queries | **11,134,007,040** | 14,568,253,440 | 13,545,018,240 |
| GETs / 1,000 queries | 22,126 | **16,776** | 17,696 |
| Bytes/query median / p95 / max | not repeated here | 16,773,120 / 16,773,120 / 16,773,120 | 15,375,360 / 16,698,240 / 16,773,120 |
| GETs/query median / p95 / max | not repeated here | 18 / 31 / 32 | 15 / 32 / 32 |

Against V164's same order and rosters, V165 planned 7.02% fewer bytes
but 5.48% more GETs. The result isolates fetch granularity under the
frozen vote-maximizing objective. It does not show that a 32-row format
cannot be competitive under a different objective. The objective gives a
positive vote to every remaining nominee and spends budget to capture it;
the V165 median still spent 15,375,360 encoded SQ8 bytes. The next
change must control marginal value per charged byte/GET rather than sweep
unit width or tune against this dataset's truth.

## Provenance and limits

One Causality Spot `c7i.xlarge` instance `i-0d5eebf56d98c07ed` ran from
pushed source `ea73f6f4dd4bc11cfca99a559df04aba4506f65c` at
`s3://borsuk-bench-453182569524-euc1/research/v165-unit-interval/ea73f6f4dd4bc11cfca99a559df04aba4506f65c/runs/a0001/`.
The source archive SHA-256 was
`0c8d4eb2c03d5f596af7e3e77229ac2d26fc4b8b404c6cc56acd9c60cf5ea02a`;
the complete terminal SHA-256 was
`06b94b4df683cac7cba050fe8cafc186dbe6f8c6f7c250f9d7e75028c57bfb08`.
The original launcher exited zero, EC2 confirmed `terminated`, and the
controller streamed and matched byte length/SHA-256 for all seven
terminal-listed S3 artifacts. The GT-blind plan seal SHA-256 was
`1bec1a4f6603856bef6118e772219d62be932cadcd3c3163ed7cf1b00670e932`.
The second pass checked all 1,000 rosters, plans, resource totals and
decision (`pass` / `killed`). It reuses the same V114 interval optimizer,
so this is deterministic replay, not an independent optimizer proof.

No truth, SQ8 body, returned IDs, exact-source scores or V155 replay was
opened in this screen. **There is no V165 Recall@100, latency, throughput,
charged serving RAM or live S3 GET measurement.** The resource gate alone
was decisive for the frozen pure unit-only policy. Planning took 8.06
seconds wall and 107,636 KiB peak process RSS on the worker; these are
offline campaign resources, not serving performance.

## Next gate

Keep V155 as the strongest measured 1M point and V164 as a transfer-pass
architecture candidate. Design a source- and query-derived marginal-value
admission objective with explicit recall/resource tiers, 32-row unit costs
and no corpus-name or vector-count branch. Preregister its objective,
bounded resource profile, and exact-source quality gate before using GT.
The first screen should separate primary capture, newly fetched candidate
benefit and byte/GET charge; a useful policy must meet V155's returned
quality and resource totals on a paired cohort, then transfer to fresh
data and live S3. Lean can prove conditional budget and memory monotonicity
once implementation counters refine the model; it cannot infer observed
recall or latency from these GT-blind resource counts.
