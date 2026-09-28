# Rust source packages (development)

BORSUK is not yet a production release. The current production dependency graph
can be packaged as three source archives: `borsuk`, `borsuk-fma`, and
`borsuk-pq4`, each at version `0.1.0`. Private V25/V41 campaign diagnostics are
outside that graph; reproduce them from their original frozen revisions.

Create the archives from the repository:

```sh
cargo package --locked --allow-dirty --no-verify \
  -p borsuk-fma -p borsuk-pq4 -p borsuk
```

`--no-verify` only creates archives. It does not prove compilation. Registry
publication and installation are unverified; the helper archives must be
provided locally for the development smoke. Extract all three together and
add a Cargo configuration with their absolute paths:

```toml
[patch.crates-io]
borsuk-fma = { path = "/absolute/package-directory/borsuk-fma-0.1.0" }
borsuk-pq4 = { path = "/absolute/package-directory/borsuk-pq4-0.1.0" }
```

From the extracted `borsuk-0.1.0` directory, pass that configuration with
`cargo --config /absolute/helpers.toml`. Resolve the patched lockfile once,
then use `--locked` for tests/builds. The package contains the project license,
README, native source-builder example and immutable test fixtures.

The [recorded package smoke](../research/production-package-boundary-20260928/decision.md)
checks native source preparation, authenticated HTTP create/publish/open/search
and failure handling, package metadata and the runnable source-builder example.
Its two-row HTTP fixture uses local fake storage credentials; it does not
qualify live IAM, full-dataset recall, cold cloud latency, mutations, compaction
or competitor performance. Those remain in the [release checklist](rust-v0.1-rc-checklist.md).
