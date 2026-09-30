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
