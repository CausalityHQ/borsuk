# Experimental source builds

Use a pinned repository checkout for the Rust APIs and their local workspace
dependencies. Inspect public signatures and generate local documentation from
the repository root:

```sh
cargo doc --locked -p borsuk --no-deps
```

The [generation fixtures](../../crates/borsuk/tests/two_bit_generation.rs) and
[application-ID/lifecycle fixtures](../../crates/borsuk/tests/two_bit_application_ids.rs)
show current API composition. The [HTTP example](../../crates/borsuk/examples/two_bit_http.rs)
is a development measurement harness.

BORSUK is unreleased; registry installation and cross-platform release packages
are not established by this guide. See [experimental status](../production-readiness.md)
and [LICENSE](../../LICENSE).
