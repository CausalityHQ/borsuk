# Global Cell Stripe Confirmation Design

## Decision

Run a new, explicitly versioned confirmation comparing only the existing 1 MiB
control with the 4 MiB candidate. Keep the terminal v68 result and its failed
selection immutable. The confirmation increases each arm from 100 to 500
deterministic queries per repetition while retaining five paired repetitions,
fresh processes, fresh disk caches, the same immutable S3 index, and the same
public search path.

This is a production diagnostic for one physical read parameter. It does not
qualify ingest throughput, 100M-vector scale, multi-client conflict behavior,
or sparse, text, and late-interaction features.

## Alternatives Considered

1. **Change the v68 rule or reinterpret its result.** Rejected because the
   frozen four-of-five rule failed and changing it after reading measurements
   would be outcome-driven.
2. **Duplicate the entire runner and validator.** Rejected because independent
   copies of artifact reconciliation and terminality checks are likely to
   drift.
3. **Add a new exact benchmark protocol and manifest while extending the
   validator with campaign-specific schemas.** Selected. The old campaign
   remains accepted under its exact v1 schema, while the new campaign has a
   distinct protocol name, exact shape, and selection contract.

## Frozen Inputs

The confirmation replays the terminal v67 `c2000/r01/l1/w8` immutable base:

- index:
  `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260808T091300Z-v67-40911df/index/cells/c2000/r01/l1/w8`
- samples:
  `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260808T091300Z-v67-40911df/results/cells/c2000/r01/l1/w8/samples.csv`
- source SHA-256:
  `4ea819fbb9cb4e203811410e40f7c158dca5fc18a3644012d96341155aa52423`
- base manifest SHA-256:
  `81c849548d9ef7300cffd88a0a13aca2023645ae0af40e66f0da5a60ad37408a`
- samples SHA-256:
  `7ec84babc5dc24bdc6275898155d362bf7e4c487c39491d1e136e2ba9906f578`
- Cohere 1M dataset descriptor SHA-256:
  `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254`
- dimensions: 768
- writers: 8
- operations per writer: 1,000
- records per operation: 16
- maximum searched segments: 4

Every query uses `SearchOptions::approx(10, LeafMode::SrhtPqScan)`, at most four
segments, and at most 16 rerank candidates per segment. The query selector takes
500 deterministic, evenly spaced entries from the canonical 8,000 base samples,
and each sample must reconstruct its exact inserted Cohere dataset row.

## Paired Protocol

The arms are 1,048,576 and 4,194,304 stripe bytes. Run five repetitions with
these orders:

1. 1 MiB, 4 MiB
2. 4 MiB, 1 MiB
3. 1 MiB, 4 MiB
4. 4 MiB, 1 MiB
5. 1 MiB, 4 MiB

Each of the ten arms runs as a new process with a unique nonexistent disk-cache
directory. Each arm records 500 raw query rows, summary data, 100 ms resource
telemetry, storage-access telemetry, environment identity, process exit status,
and terminal markers. It may issue no PUT or DELETE request. Query IDs and their
order must be identical across arms. Logical bytes per query must be identical
between arms; a larger stripe may reduce GET count but cannot pass by reading a
different logical search set.

The production runner rejects a nonempty output prefix, reused local output or
cache paths, a dirty tracked source tree, identity mismatches, a non-S3 base, or
missing terminal base markers. The launcher uses AWS profile `causality`, the
dedicated `c7g.8xlarge` worker, and refuses to start alongside another benchmark.

## Selection Rule

The validator checks root terminality before opening any measurement CSV. A
candidate is promoted only when all of these are true:

- all ten arms and required artifacts validate;
- inserted-ID recall@10 is exactly 1.0 in every arm;
- all arms issue zero PUT and DELETE requests;
- query identities, source identities, base identities, and logical bytes match;
- the 4 MiB pooled p95 across 2,500 queries is below 200 ms;
- the 4 MiB worst-repetition p95 is below 200 ms;
- the 4 MiB p95 is no worse than the paired 1 MiB p95 in at least four of five
  repetitions;
- the 4 MiB pooled p95 is at least 10% lower than the 1 MiB pooled p95; and
- the 4 MiB pooled p50 is no more than 5% higher than the 1 MiB pooled p50.

The 10% effect floor prevents a production default change for measurement noise;
the p50 guard prevents buying a tail improvement with a material typical-query
regression. Failure leaves the 1 MiB default unchanged and becomes diagnostic
evidence for the next architectural investigation.

## Code Boundaries

- `group_commit_bench.rs` gains a distinct
  `read-stripe-confirmation` protocol. Its exact 500-query shape and two-arm
  alternating order are validated separately from the frozen
  `read-qualification` v1 protocol and structural smoke.
- A new confirmation manifest is the sole production configuration source.
- A new runner and launcher own confirmation paths, markers, and AWS lifecycle.
- The existing validator is extended with exact schemas for the historical v1
  and confirmation v1 campaign IDs. Shared artifact reconciliation remains one
  implementation; selection is campaign-specific.
- Tests prove both campaign schemas, fail-closed terminality, paired order,
  exact shape, identity/recall/request/byte reconciliation, and the complete
  confirmation selection rule.

## Verification and Delivery

Use TDD for Rust protocol and Python/shell harness changes. Run focused Rust and
Python tests, shell syntax checks, repository policy checks, formatting, strict
Clippy, and one full Rust and Python assurance gate. Commit coherent verified
slices and fast-forward push each directly to `origin/main`. Only then may the
AWS confirmation launch. Terminal measurement CSVs remain unread until the root
validator is eligible to run.
