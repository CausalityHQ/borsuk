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

## Attempt a0001 closeout and a0002 correction

Attempt a0001 terminated `failed`, exit 101, after 39 seconds on Spot
`i-0ae05236906499a1f`. Its terminal and four artifact hashes verified.
The test did not compile BORSUK: the runner called Cargo from the work root
without `--manifest-path`, so Cargo could not find `Cargo.toml`. This is a
runner error, not a code or quality result. The instance terminated.

Attempt a0002 corrects the runner to use `repo/Cargo.toml`, imports
`ObjectStoreExt` in the Rust test, and bounds streamed response bytes before
collecting them. The new deterministic archive uses actual base commit
`ec0becf299b407b060c1c2ea99a7b84cbcaba616` plus four overlays listed
in its `snapshot.json`. It is 11,531,054 bytes, SHA-256
`f4914e7b417b589803edc502dfb52b4229b73d91627dd214702336b4cc3e02d9`.
The same narrow test command, Spot class, time cap and immutable terminal
protocol apply under a fresh `a0002` prefix.
