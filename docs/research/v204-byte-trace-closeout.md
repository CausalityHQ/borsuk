# V204 byte-trace negative closeout

Source `1b6d257e10e528c971539b9e25da257548cebf2f`, Causality Spot
attempt `a0001`, instance `i-09224d990332e733c`, terminal SHA-256
`73abe738de69d9b1c2fa546468fe4851990c8e7ea3048ebad8ea4d6e9c3884a5`.
The worker terminated after terminal and all six artifacts were read back.
`scripts/check_v204_byte_trace.py` independently passed closed input,
targeted Rust tests, 1,000/1,000 V198 physical witness parity, tier and
percentile replay on the **already-used** ReLAION-1M D768 validation-1000.

| Planner-only metric | V203 baseline | V204 measured |
| --- | ---: | ---: |
| Hard two-cap p95 | 52.384 ms | 53.712 ms |
| Complete hierarchy p95 | 55.588 ms | 57.544 ms |
| Peak Rust process RSS | 43,606,016 B | 43,560,960 B |

The preregistered <30 ms and <128 MiB joint screen was **not met**. There
is no measured speed or RAM benefit. The preregistration's packed-Bool
premise was incorrect: on this Rust target `size_of::<bool>() = 1` and
`size_of::<u8>() = 1`; `Vec<bool>` is ordinary byte-element storage here.
The change swapped one-byte elements and added conversions, so it did not
test a packed-versus-byte trace trade-off. Preserve the preregistration as
the historical hypothesis and return implementation to the V203 Boolean
trace baseline. The next optimization must address state-loop work or use
an exact dual certificate. V204 has no new recall, S3 or scale evidence.
