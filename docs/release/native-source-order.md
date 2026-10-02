# Experimental source ordering

The native builder accepts an explicit physical permutation while preserving
logical application IDs. SOURCE nomination, SQ8 records, and canonical source
must agree on that order. Invalid orders and duplicate IDs are rejected.

The [application-ID fixture](../../crates/borsuk/tests/two_bit_application_ids.rs)
checks ordered build, publication, reload, ranking, and maintenance.
Read the [generation guide](two-bit-generation.md) before building artifacts.
