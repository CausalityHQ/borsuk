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
