# Actual frozen-format Rust serving check

Reuse the existing signed HTTP range fixture. Build a two-row nonuniform
source/SQ8 generation through the public builder, publish/read head in the
conditional InMemory store, remotely stage metadata and search via OneAttemptS3.
Assert ID/range/If-Match/signing and submitted GET/verified-byte accounting;
reopen and test extreme query magnitudes. Mutated data and changed ETag must
fail with charged physical requests and no hidden retries. Fixture proves Rust
integration/protocol behavior, not AWS authentication, service timing or quality.
Run the focused lib test on Spark; no library algorithm/format change, paid job
or full-suite duplication. Existing generation publication tests remain evidence.
