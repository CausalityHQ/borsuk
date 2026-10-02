**SOURCE cap128 → cap32: conditional geometry PASS, native candidate unmeasured.**

The authenticated closed CoHere FIRST1M D768 sealed64 trace reproduces the
offered-http/a0002 rate5 ordered top10, leaf GETs/bytes, SOURCE GETs/bytes,
and exact SQ8 ranges/GETs/bytes for all 64 queries. Replaying the pinned Rust
cover rules at cap128 also matches native SOURCE counts/bytes for every query.
Cap32 covers every byte of the same nomination closure within 64 MiB.

| SOURCE geometry | Current cap128 | Candidate cap32 |
|---|---:|---:|
| Total GETs | 3,147 | 2,028 |
| Total bytes | 1,101,708,800 | 1,290,124,800 |
| Maximum query bytes | 21,299,200 | 32,563,200 |
| Largest single range bytes | 5,632,000 | 6,348,800 |

This saves 1,119 GETs (35.56%) while adding 188,416,000 bytes (17.10%).
Query ordinal 54 has the largest byte increase: 75 → 32 GETs and
21,299,200 → 32,563,200 bytes, an increase of 11,264,000 bytes. Six queries
(7, 36, 37, 43, 52, 61) already use at most 32 GETs and are unchanged.
These numbers are computed from authenticated inputs; the prior plan's
reported totals are not constants or acceptance criteria in the replay.

The historical trace does **not** contain native SOURCE range endpoints.
The JSON explicitly marks those endpoints as unavailable and labels its SOURCE
ranges as modeled. Exact recorded range comparison applies to SQ8 only.
SOURCE count/byte agreement cannot by itself establish native endpoint parity.
Likewise, reusing the recorded closure and ranking establishes conditional
geometry, not candidate ranking correctness or a latency improvement.
The current measured candidate remains viable: its historical rate5 result is
96.71875% recall@10, cold p90 541.095 ms at 8 offers/s and finite full-span
7.597044 QPS. No quality, ground truth, ANN or latency computation was rerun.
Historical caps remain diagnostics for their frozen arms, not universal KILL
criteria.

Input authority starts at base commit
`334313ce0957f2bbd6108964800f0e9e6351f78f` and the fixed SHA/length of
`offered-http/a0002/root-audit.json`. Its authenticated config references
`cold-http/a0005/config.json`, whose scientific proof pointers locate the
closed measurement under
`scientific-execution/python-replay-a0002/a0002/screen/`.
The script checks terminal, independent closed-validation receipt, COMPLETE
roster, measurement receipt, trace and requests before replay. The native source
manifest authenticates the six relevant Rust files against the working tree.
The evidence lists all 22 authenticated bodies (5,123,187 bytes), including
individual hashes/lengths; it does not claim to revalidate all historical
artifacts or qualify a new native binary.

| Authority / deliverable | SHA256 |
|---|---|
| Offered root audit | `3e8c655726b5eacf346e75ec29be7ac71b4e8ba7aa50a1f432c7f90b98624a11` |
| Closed scientific trace | `631fd161bf859d77b9d772576de806d4b71e155bd70b9d6f6b9b68bcfeac6be8` |
| Scientific measurement receipt | `7a8f51d33b304ea1a2f160dd3da2901d3a73f33c4282ee6d117edbe506836d23` |
| Offered rate5 records | `2c313cc6225146edd78f76571a7b16620b0c86b0b611f9bdb8112d01c61ab332` |
| Native source manifest identity | `addf62bce23ceee034f22e4d1dfc0b318564d924c6e0fcee34b9336d1532813e` |
| Replay Python | `d5ea9cb2bd06e2efb5251612cd91fef9cea30fed95a842eaa15c1b3136d59100` |
| Replay JSON | `e53ad52bc3c0933551d03c4a449d4141963c5f3e5752b83af6764bcc046ea181` |

Verification commands, run from the base worktree plus this three-file slice:

```bash
python3 scripts/replay_fixed48_source_get_caps.py --self-check
/usr/bin/time -f 'elapsed=%e peak_rss_kib=%M exit=%x' \
  python3 scripts/replay_fixed48_source_get_caps.py \
  --output docs/research/performance-architecture-20260930/semantic-1m/fixed48/source32-geometry/replay_fixed48_source_get_caps.json
git diff --check
```

Self-check exit 0: 889 exhaustive tiny covers, literal equal-gap/tail cases,
invalid geometry and insufficient-byte rejection, file length/SHA tamper
rejection, and 12 ordered-top10/leaf/SOURCE/SQ8 parity mutations. The exhaustive
oracle enumerates partitions rather than reusing the planner's greedy merges.
The initial empty cover implementation failed the first tail assertion (exit 1)
before implementation. The full authenticated replay exited 0 in 0.38 seconds
with peak RSS 50,636 KiB. The CLI enforces 256 MiB address space and 120-second
CPU/wall limits. An existing output is rejected; reruns must use a fresh path.

A second replay using `--repo` on a temporary copy of the 22 authenticated
bodies produced byte-identical JSON (exit 0). Same-length mutations of the
scientific trace, rate5 records and Rust planner, plus a missing trace, each
returned exit 1 with `BLOCKED` and created no evidence file. `python3 -O ...
--self-check` also returned exit 1. Syntax compilation via Python's `compile`
and `git diff --check` passed. No Cargo, native executable, cloud, network,
consultation, child, operator contact or push was used.
An independent `git show BASE_COMMIT:path` byte/hash comparison also passed
for all 22 input bodies, and the recorded script hash matches the final script.

The cheapest genuine native **geometry** falsifier is one root-owned focused
Rust test invoking the existing `plan_two_bit_source_cover` on the JSON's 64
closure-page lists at caps 128 and 32. Compare every range endpoint and charge,
including ordinal54; add the existing 513-row tail/tie/budget fixtures. This
directly tests the actual production planner without vectors, a scorer, truth,
ANN or network. Proposed test-process ceiling: one CPU, 256 MiB, 120 seconds,
at most 2 MiB output; compilation admission remains a separate root decision.
Any endpoint/count/byte mismatch falsifies this replay model.

For behavioral parity after that gate, the cheapest meaningful probe is one
sequential native pair for ordinal54 using the authenticated real retained
fixture and existing diagnostic search path, with only `max_source_gets`
changing 128 → 32. Compare ordered top10, complete nomination trace and SQ8
ranges, plus observed SOURCE GETs/bytes; then extend to all64 if it passes.
This needs the real SOURCE/SQ8/router payloads, which were not authenticated
or loaded here. A prospective single-process probe can start from the existing
512 MiB scorer allowance, two threads, zero swap, 120 seconds and 2 GiB fixture
scratch, subject to root admission of the exact retained assets and binary.
One-query parity cannot establish all64 parity or cold latency. The root owns
native edits, execution resources, frozen protocols and any later paired cold
measurement; this report neither changes those protocols nor authorizes a run.
