# V115 S3 transport compile cell

Status: preregistered before launch. This is one remote compile and narrow unit
test of the Rust `sq8_s3_range` adapter. It is not an ANN performance or
quality measurement and has no development query or GT input.

Use one Causality `c7i.4xlarge` Spot instance in eu-central-1c, with a
3,300-second test timeout. The latest observed Spot price for that zone was
$0.3684/hour at 2026-09-23 20:00 UTC; it is variable and not measured spend.
No On-Demand fallback. The test command is
`cargo test -p borsuk --lib sq8_s3_range::tests --jobs 4 -- --nocapture`.
The prerequisite Python/Rust page verifier and standalone 23-test gate have
already passed. This cell checks the full BORSUK crate compilation and the
conditional range test without consuming devbox build memory.

The exact working-tree snapshot uses base commit
`3607cd814d34a3a4712fcfb9143a816d2eafbd1f` plus four overlay files
listed with SHA-256 in `snapshot.json` inside a deterministic source tarball.
The tarball is 11,529,997 bytes, SHA-256
`37333f125fd9642dc6d8829c491e7bdf9ce4563d277be692a044ede7d5fd75b2`.
One immutable reservation and terminal bind the tarball hash. Collect the
test exit status, log, `/usr/bin/time -v` resources, instance ID and all
artifact hashes; terminate after the terminal. On Spot interruption, discard
the cell and use a new attempt prefix. A compile or test failure requires a
source fix and a new snapshot, not repeated runs of identical source.
