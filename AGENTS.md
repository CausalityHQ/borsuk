# BORSUK Repository Instructions

## Pre-release architecture policy

BORSUK has not been released and has no compatibility contract. Until the
first release, schema stability and backward compatibility are non-goals.

- Prefer the simplest, fastest, and most coherent production architecture
  supported by correctness and benchmark evidence.
- Breaking changes to public APIs, defaults, persistent schemas, object
  layouts, storage formats, manifests, and configuration are allowed.
- Do not retain legacy readers, migration layers, aliases, duplicate write
  paths, or deprecated behavior solely for compatibility unless the user
  explicitly requests them.
- When a persistent layout changes, increment or replace its format/version
  marker, update fixtures and tests, and reject incompatible artifacts clearly.
  Current code does not need to read old experimental indexes.
- Treat old benchmark artifacts as immutable historical evidence tied to their
  recorded source archive and configuration. Never compare results across an
  architecture or format change as if they came from one frozen system.
- Freeze production defaults only after the architecture qualification gates
  pass. Run publication and large-scale comparison benchmarks from that exact
  frozen revision.

## Current implementation priority

- Prioritize the self-contained Rust ANN library: routing, index layout,
  recall, p90/p95 latency, QPS, memory efficiency, threading and scalability.
  Select each substantive change from measured stage timings and recall-loss
  evidence; optimize SIMD only when profiling identifies a compute bottleneck.
- Optimize Rust design for measured runtime performance. Long compilation
  times and substantial build resources are acceptable; advanced Rust features
  are allowed when they improve the runtime. Compilation time is not a product
  acceptance metric or a reason to reject a runtime improvement. Keep build
  resource safety limits and qualify expensive builds on bounded remote hosts.
- Do not initiate Python or TypeScript features or controller milestones.
  These languages may provide only minimal glue to run and verify an exact
  native experiment. Preserve existing workers, original jobs and receipts.
- Run a fast native correctness and recall falsifier before a long scale
  benchmark. Qualification requires actual native execution on frozen source.
- Preserve every historical arm's original protocol and GO/FAIL result.
  Its memory, GET, byte and recall thresholds are specific to that arm,
  not universal admission gates for a changed architecture. Preregister new
  resource envelopes from measured stage RSS, scratch, resident bytes,
  requests, generation pins, maintenance and concurrency. Keep host safety
  limits unchanged; qualify larger candidates on a bounded remote host.
- The product goal remains matched quality, end-to-end tail latency, QPS and
  total lifecycle cost against both S3 Vectors and Turbopuffer. Published
  results under different conditions do not establish a measured win.

## Delivery and evidence policy

- Before handing off Rust implementation work, compile the affected test
  target and pass `cargo clippy --locked --workspace --all-targets -- -D
  clippy::correctness -D clippy::suspicious` on the exact source revision.
  Run `bash scripts/check_rust_test_build.sh` before integration to compile
  the complete workspace test surface with bounded compiler concurrency.
  Record commands, revision and exit statuses. Mock/controller checks do not
  establish Rust compilation or Clippy success. If an authorized build
  environment is unavailable, report the work as unverified, not complete.
- Do not create pull requests.
- Commit coherent, verified slices and push them directly to `origin/main`.
- Never force push. Before every push, verify that `origin/main` is an ancestor
  of the commit being delivered so the update is a fast forward.
- Continue production research, implementation, and benchmark work from
  `main`.
- Use AWS profile `causality` for BORSUK research infrastructure.
- Use EC2 Spot capacity by default for every new interruptible benchmark
  campaign. Preregister interruption handling, sync terminal repetitions to
  S3, discard and restart an interrupted measurement cell, and record every
  instance identity. Use On-Demand only when Spot is unavailable or a
  non-resumable method requires it, and document that exception before launch.
- Stop or terminate benchmark compute immediately after its terminal marker;
  never retain an idle experiment instance for convenience.
- Preserve frozen campaign methodology and immutable historical artifacts.
  Monitor incomplete campaigns by terminal markers and infrastructure health
  only; never inspect incomplete measurement CSV files.
- Use commercial first-party or paper numbers only. Product comparisons must
  be honest paired reproductions under disclosed equivalent conditions.

## Current benchmark focus

The operator narrowed prospective benchmarking on 2026-10-06 to one dataset
used by Turbopuffer: Cohere Wikipedia embed-multilingual-v3, D1024, k=10.
Use the same frozen corpus and query bytes for BORSUK, Turbopuffer, and S3
Vectors. Start small and scale this corpus; do not launch new ReLAION or
multi-dataset campaigns. Preserve historical receipts and existing bounded
Rust correctness work. See `docs/research/performance-architecture-20260930/single-dataset-focus.md`.
Published Turbopuffer results do not disclose exact selected IDs/query split;
dataset-family agreement alone does not establish a matched measured win.

Cohere is an evaluation fixture, not a specialization of the library. Keep
Rust indexing, routing, scoring, storage and maintenance independent of
dataset names, model names, fixed corpus populations and benchmark query
splits. Admit dimensions and resource limits explicitly; any supported
dimension ceiling must be documented and tested, rather than silently
assuming D1024. Benchmark-specific choices belong in experiment configuration
and fixtures. A Cohere result proves performance on that workload only; it
does not establish generalization to other vector distributions.

## Experiment admission order

Before each new paid cold or performance experiment:

1. Run a bounded source-bound admission check on the experiment's exact authenticated inputs through its runtime validator. For ANN measurements this includes real requests, records and truth; a preregistered query-free source-neighborhood diagnostic must keep requests and truth unopened and check its source/layout pins instead. Synthetic checks alone are insufficient.
2. Pass a separate disposable staging/canary smoke covering imports, SDK, asset admission, CLI, exit status and cleanup. Declare which operations are real and which are mocked; canary output is not scientific performance evidence.
3. Freeze source/configuration and run the measured experiment only after both gates pass. Record commands, source identities, runtime and terminal receipts. Code, protocol or resource errors are execution INVALID, never a performance KILL. Preserve frozen historical dispositions.

Environment or configuration failures do not reject the proposed solution. Fix the demonstrated setup defect, validate the repaired environment, and rerun the unchanged algorithm and scientific limits in a new recorded attempt. Preserve the failed attempt; distinguish an observed algorithm resource-limit failure from an environment that failed to establish or enforce the declared limits.
