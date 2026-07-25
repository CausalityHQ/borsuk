# Publication Methodology v2 Design

## Purpose

Publication methodology v2 is a confirmatory protocol for claims that BORSUK
has lower client-observed p95 query latency at matched or higher recall on
weaker disclosed hardware. It replaces single-campaign and within-process
repetition evidence with independent, immutable campaigns and explicit
statistical claim gates.

The active `20260725T144241Z` campaign is a pilot. It may be used to debug the
runner, estimate cost, and freeze configurations, but it cannot supply the
confirmatory headline result.

## Evidence classes

Every external number belongs to exactly one evidence class:

1. `direct-controlled`: measured by this project from raw query samples under a
   shared dataset, query set, ground truth, metric, `k`, region, client, and
   schedule. Amazon S3 Vectors is the only commercial system in this class.
2. `vendor-reported`: copied from a dated first-party commercial source.
3. `paper-reported`: copied from a cited paper or its official artifact.

Vendor- and paper-reported values are stored separately from direct
measurements. They are never silently pooled, averaged, or plotted as though
they came from the controlled runner.

## Primary claim

The primary claim is:

> On the named public dataset and cache condition, BORSUK has lower
> client-observed p95 latency at equal or higher recall@10, using no more CPU,
> RAM, accelerator capacity, or local-storage class than the comparison
> hardware disclosed by the source.

The claim is allowed only when all of the following match:

- dataset identity and checksum;
- corpus and query counts;
- distance metric and vector representation;
- top-k and recall definition;
- cache condition;
- latency statistic and client/server boundary;
- concurrency;
- failure and timeout treatment.

For a managed service whose server hardware is undisclosed, including Amazon
S3 Vectors, the result can establish lower observed latency, higher recall, or
lower documented cost under the same client contract. It cannot establish
weaker server hardware.

Primary-claim p95 is confirmatory. P99, throughput, build time, index size,
requests, transferred bytes, and cost are secondary results.

## Confirmatory configuration

The final runner consumes a checked configuration manifest. The manifest fixes:

- source archive digest;
- datasets and checksums;
- BORSUK search parameters;
- query count and order seeds;
- cache states;
- repetition count;
- instance type, region, disk class, and resource limits;
- S3 Vectors query contract;
- primary metric and claim gate.

Execution refuses manifests marked `pilot`, fewer than three independent
repetitions, fewer than 1,000 queries for a p99 result, unfrozen search sweeps,
or reuse of an existing result/index prefix.

The default confirmatory campaign uses five independent repetitions. Three is
the hard minimum. Each repetition has a fresh process, fresh BORSUK index
prefix, fresh local cache directory, and fresh S3 Vectors bucket/index.

## Dense and direct S3 Vectors schedule

The scheduler deterministically permutes dataset order from a recorded master
seed. For the Fashion-MNIST direct comparison, BORSUK and S3 Vectors execution
order alternates by repetition. Both systems use the same query subset and
query-order seed.

S3 Vectors emits `first_pass` and `repeated_pass`; these are not relabelled
`uncached` or `disk_cached` because service cache state is opaque. BORSUK cache
states retain their operational definitions. A cross-system claim is made only
for a cache pairing declared in the manifest and justified in the claim
registry.

All final dense latency measurements use at least 1,000 queries. P95 is the
primary tail statistic. P99 is published only when at least 1,000 samples exist
per independent repetition and remains secondary.

## Hybrid retrieval protocol

Pilot sweeps may examine candidate depth, segment budget, RRF constant, and
cache fractions. The confirmatory run accepts one frozen configuration per
dataset and mode; it does not select a winning configuration from test qrels.

Each hybrid repetition uses:

- a fresh index prefix and process;
- a deterministic query permutation derived from the master seed and
  repetition number;
- a fresh cache directory;
- one priming pass over a seeded query cohort;
- exactly one measured pass over all selected queries.

The primed cohort is therefore independent of source-file order. The measured
pass cannot progressively remeasure already observed queries. Requested cache
fraction, observed cached-byte fraction, cache tier, backing reads, and
transferred bytes remain separate fields.

## Statistics and claim gates

The analysis keeps repetition as the independent experimental unit. Query
samples are nested within repetitions and are not described as independent
campaigns.

For direct BORSUK versus S3 Vectors evidence:

- report every repetition and raw query sample;
- report the median repetition-level p95 and recall;
- compute a deterministic hierarchical bootstrap confidence interval by
  resampling repetitions and then query samples within repetitions;
- report the BORSUK/S3 Vectors p95 ratio and recall difference;
- permit `lower-latency-at-matched-recall` only when the upper 95% confidence
  bound of the latency ratio is below `1.0` and the lower 95% confidence bound
  of the recall difference is at least `0.0`;
- include failed and timed-out requests in the failure rate and latency
  distribution according to the frozen timeout policy.

For vendor- and paper-reported evidence, the registry records the point estimate
and source assumptions. No confidence interval is invented. A superiority
statement is allowed only when the full comparability checklist passes and
BORSUK's conservative confidence bound still dominates the reported point.

## Hardware comparison

Hardware is `weaker-or-equal` only when BORSUK uses:

- no more logical CPUs;
- no more RAM;
- no accelerator when the comparison has none, or no stronger accelerator;
- no stronger local storage class for the measured path.

Unknown fields fail the hardware claim gate. Managed service internals are
recorded as `unknown`, not inferred from client-side resources.

Every run captures instance type, AMI, availability zone, CPU model, logical
CPU count, RAM, kernel, Rust toolchain, EBS type/size/IOPS/throughput, package
lockfiles, source digest, UTC interval, and resource traces.

## Reported-comparison registry

An auditable CSV registry stores one row per external figure:

- system and evidence class;
- source URL, title, publication date, and access date;
- dataset, scale, metric, k, recall, latency statistic, cache state, and
  concurrency;
- hardware and resource scope;
- reported value and units;
- comparability result and explicit mismatch reasons;
- permitted wording.

Charts and tables render evidence classes distinctly. Vendor and paper numbers
must include an inline citation and `reported` label. The report generator
refuses an unqualified superiority statement for a row that fails a gate.

## Artifact and failure policy

Every repetition writes immutable coverage, environment, configuration, raw
sample, summary, and resource files. Completion checkpoints are written only
after validation and cannot overwrite an existing object.

A failed repetition remains in coverage. It is not silently retried into the
same repetition identifier or removed from analysis. A replacement uses a new
identifier and both attempts remain visible.

The final artifact includes:

- the frozen methodology manifest;
- exact source archive and SHA-256;
- dataset/source manifests and checksums;
- schedule and seeds;
- raw and summarized measurements;
- validation output;
- claim registry and generated claim decisions;
- limitations and excluded claims.

## Implementation boundaries

The implementation will:

- add a deterministic confirmatory scheduler and manifest validator;
- make hybrid query order seedable;
- move hybrid repetition outside the measured process and give every
  repetition a fresh cache;
- add independent dense and S3 Vectors repetition orchestration;
- add a claim registry validator and statistical comparison generator;
- update publication documentation and artifact validation;
- retain existing pilot runners for historical evidence.

It will not run direct benchmarks of Faiss, TurboVec, Pinecone, Turbopuffer, or
other commercial systems as part of the publication campaign. Existing control
scripts remain available as engineering tools but are excluded from the
confirmatory claims pipeline.

## Test strategy

Unit tests cover manifest rejection, deterministic scheduling, order
alternation, hardware gates, evidence-class separation, confidence-interval
determinism, and claim decisions.

Shell dry-run tests prove that repetitions receive unique result/index/cache
locations and that final execution remains explicitly paid-operation gated.
Rust tests prove that a fixed seed produces a stable permutation and different
seeds change the cohort without changing query membership.

An end-to-end fixture builds synthetic raw samples, runs analysis, validates the
artifact bundle, and demonstrates both an allowed and a rejected superiority
claim.
