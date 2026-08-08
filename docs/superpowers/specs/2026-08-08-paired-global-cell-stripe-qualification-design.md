# Paired Global-Cell Stripe Qualification Design

**Status:** Approved for autonomous implementation from terminal v67 evidence

## Problem

The v67 AWS arm proved that fetching a bounded global-PQ cell envelope can
remove the dependent exact-rerank wave without changing recall. It did not
prove that the current physical transfer shape is a production default. At
eight writers, 1 MiB stripes reduced mean post-drain GETs from 5.55 to 4.70 and
mean latency from 103.841 ms to 101.940 ms, but post-drain p95 regressed from
203.884 ms to 210.608 ms. A four-request wave inherits the slowest request's
tail, so a lower request average alone is not sufficient evidence.

Changing the constant from 1 MiB to 2 MiB or 4 MiB based on one failed run
would confound code changes, index contents, queries, cache state, and S3
variance. Rebuilding the index for each arm would add unnecessary write-path
variation. Reusing a warm cache would conceal the uncached S3 behavior that is
the blocker.

## Decision

Add a bounded `OpenOptions` control for the physical global-PQ envelope stripe
width and a read-only mode to the existing `group_commit_bench` example. The
mode replays queries from a completed immutable group-commit cell against the
same immutable S3 index. It performs no append, drain, compaction, or
publication operation.

The qualification runner compares three transfer shapes:

| Arm | Stripe width | Maximum GETs for a 4 MiB envelope |
| --- | ---: | ---: |
| `s1m` | 1 MiB | 4 |
| `s2m` | 2 MiB | 2 |
| `s4m` | 4 MiB | 1 |

All arms retain the existing 8 MiB query-stage envelope budget, 16-stripe
stage cap, 4 MiB maximum envelope, exact candidate/rerank behavior, and
standard Arrow/Parquet objects. The option changes request partitioning only;
it does not change durable bytes or logical search work.

## Runtime contract

`OpenOptions::global_pq_prefetch_stripe_bytes` is a positive byte width no
larger than the 4 MiB envelope cap. The default remains 1 MiB until paired AWS
evidence promotes another value. Root and named-vector handles share the value
through `CollectionReadRuntime`, so one collection handle cannot silently use
different physical plans across modalities.

The global-PQ planner receives the configured width and accounts the actual
number of stripes before I/O. The storage call uses the same width. A plan is
therefore rejected or downgraded to code-only before I/O if it would exceed the
existing stage stripe budget.

## Read-only benchmark contract

The new `read-qualification` protocol accepts:

- the completed immutable index URI;
- the completed cell's `samples.csv` and its SHA-256;
- the pinned Cohere dataset descriptor and SHA-256;
- the base campaign source and manifest identities;
- the candidate source and qualification-manifest identities;
- dimensions, writer count, operations per writer, records per operation,
  query count, maximum searched segments, stripe width, and repetition.

It reconstructs each inserted query vector using the same writer/operation to
dataset-row mapping as the production campaign, opens a fresh read-through
cache directory, and invokes the same approximate `k=10`, SrhtPqScan,
`max_segments=4`, 16-candidate search path used by the production gate. Query
latency wraps the complete public search call, including cold lazy setup.

Every successful arm writes standard CSV measurements and a terminal marker.
The summary records all source, base-index, dataset, sample, protocol, arm, and
cache identities. Any recall miss, write-like storage request, malformed input,
identity mismatch, reused output/cache directory, or incomplete query count is
a hard failure.

## Paired AWS methodology

- Use the terminal v67 `c2000/r01/l1/w8` index and samples unchanged.
- Run 100 fixed inserted-vector queries per arm.
- Run five paired repetitions.
- Rotate arm order by repetition: `1,2,4`; `2,4,1`; `4,1,2`; then repeat.
- Start a fresh process and a nonexistent cache directory for every arm.
- Run arms sequentially on the dedicated `c7g.8xlarge`; reject any competing
  benchmark process or non-shell tmux workload.
- Preserve raw reads, summaries, stdout/stderr, resource telemetry, storage
  access traces, manifest, and identity files under one fresh result prefix.
- While incomplete, observe only terminal markers, the retained process, and
  EC2 health. Do not inspect measurement CSV files.
- At terminality, run the fail-closed validator before opening any CSV.

## Selection rule

Every arm must retain inserted-ID recall@10 of 1.000 and zero PUT/DELETE
requests. Selection is paired and latency-first:

1. Reject an arm if any repetition has recall below 1.000, missing artifacts,
   invalid identity, or fewer than 100 queries.
2. Compare each candidate with the 1 MiB control using per-repetition p95 and
   the pooled 500-query p95; disclose median, worst repetition, GETs/query, and
   bytes/query.
3. Promote only an arm whose pooled p95 is below 200 ms and whose paired
   repetition evidence is not worse in at least four of five repetitions.
4. Break a qualifying tie by lower worst-repetition p95, then lower
   GETs/query. Bytes/query must remain disclosed and cannot be optimized away
   by cache reuse.
5. If no arm qualifies, retain no claimed fix and redesign the read plan from
   the terminal traces. Do not relax the production gate.

This qualification selects a physical range-read shape. It does not by itself
establish 32-writer, 100M-vector, dense/sparse/text/late-interaction, or final
production readiness.

## Rejected alternatives

- **Promote 2 MiB immediately:** one v67 run cannot distinguish a better
  transfer shape from ordinary remote variance.
- **Use a warmed-cache benchmark:** cache should make the library faster after
  the uncached core path is sound, not hide S3 tail behavior.
- **Rebuild one index per arm:** the option changes only physical range
  partitioning, so rebuilding creates avoidable confounding and cost.
- **Create a second search implementation:** the qualification must exercise
  the same public search path and telemetry as production.

