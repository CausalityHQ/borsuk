# V158 D768 PQ-primary returned-quality gate

## Decision

Can the first 100 score-ordered PQ64 nominees replace exact-SQ8 scoring of
all 512 nominees as the primary set without a material returned-quality
loss under the same 32-GET and 16,777,216-byte physical SQ8 plan? This is
one candidate method, with no dataset-name or vector-count branch. V157
showed that fetching all nominee pages through the page-aligned S3 schedule
already breaches the cap on 294/1,000 ReLAION-1M validation queries. A
positive 100k result only licenses a frozen 1M paired gate; it cannot
establish live S3 latency or 100M behavior.

## Frozen used development cell

- Corpus: ReLAION-100k D768, source SHA-256
  `a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`.
- V114 complete Spot attempt at
  `s3://borsuk-bench-453182569524-euc1/research/v114-exact-local/3443d7674432e281e6709bbb5ecee30f96b54a1e/runs/v114-100k-20260923T215331Z/a0001/artifacts/`.
  Its `requests.jsonl` has 1,000 frozen development queries and 512 PQ64
  nominees; `reference.jsonl` contains exact-SQ8 primary sets and plans.
  The SQ8 object and manifest are from that same terminal. Required hashes:
  requests `b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0`,
  reference `fa42050d6610630576f3f00232aecb8c43e6a0af350bf0ac90094aaba00c9b7b`,
  SQ8 `5d215d5983da54015038083cd3f1cda16983a2660e7dd66be74bba96debe3375`,
  manifest `14a12fa3f7a571a99bf4e4411fea0a9f4a2180579decc3e539db558accda5be5`.
- Exact GT100 is the already frozen V85 ReLAION-100k development query
  artifact at
  `s3://borsuk-bench-453182569524-euc1/research/v85-competitive-rescore/fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d/runs/v85-100k-dev1000-20260920T094401Z-fb976932/a0001/inputs/truth-100k.parquet`,
  512,093 bytes, SHA-256
  `ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7`.
  The V85 and V114 input declarations bind it to the same 100k source and
  ordered development queries. Source IDs are sparse signed int64 values;
  map them explicitly to physical SQ8 positions. This split is **used**;
  no held-out claim.

## Paired method

Before opening GT, authenticate the V114 inputs, validate each query and
roster, reproduce each historical exact-primary route and cap, then plan
the PQ-primary arm by replacing only the 100-row primary set with
`nominees[:100]` in the same V114 513/1 weighted physical interval
algorithm. Seal the two plans and their source identities. Do not retrain
PQ64, change nominee count, tune page votes, or use GT to select parameters.

After sealing plans, load GT and score the same SQ8 object over each
arm's fetched ranges using V114 returned-score arithmetic. Record all
per-query returned IDs, GT hits, physical GT coverage, GETs, bytes,
primary overlap and paired wins/ties/losses. Require unique returned IDs,
physical coverage at least returned hits, and zero cap violations.
Independently recount every per-query hit, physical bound, cap, and
summary from terminal-bound raw records. Upload the raw, seal, summary,
checker output, logs and terminal to an immutable Causality Spot attempt;
discard an interrupted cell and restart a new attempt. Stop compute after
the terminal marker.

## Decision rule

Promote PQ-primary to a frozen 1M paired gate only if all 1,000 queries
complete and its total hits are at least 99,000/100,000 and no more than
100 below the exact-primary same-run control, its p05 hits are at least
98 and no more than one below control, its count of queries below 90 hits
does not exceed control, and all plans respect 32 GET/16,777,216 B.
Otherwise reject this substitution and return to a generic authenticated
exact-score tier, a score certificate, or an explicitly larger charged
transport allowance. A passing offline gate does not freeze defaults.
