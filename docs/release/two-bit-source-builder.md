# Experimental SOURCE construction

`two_bit_source::TwoBitSource` builds two-bit nomination records from validated
raw/SQ8 inputs. `build_with_order` accepts an explicit permutation. The builder
checks source identity, geometry, order, and payload admission.

`TwoBitPlane::open` authenticates the prepared nomination data under a payload
cap. These records nominate candidates; they do not contain the original
vectors used for canonical recovery or provide lossless final scoring.

See [SOURCE fixtures](../../crates/borsuk/tests/two_bit_source.rs),
[application-ID/order fixtures](../../crates/borsuk/tests/two_bit_application_ids.rs),
and [native generations](two-bit-generation.md).
