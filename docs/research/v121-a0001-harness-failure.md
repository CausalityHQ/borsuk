# V121 first attempt: compose harness failure

The immutable V121 attempt at
`s3://borsuk-bench-453182569524-euc1/research/v121-deep-image-paired/2c80018f240735c97ceaaed9766a05ada7f204cd/runs/v121-20260924T012400Z/a0001`
ended with a failed `compose` terminal, exit code 1. Terminal SHA-256 is
`33d7c02f303bc65f9bae978e8861f85048290f846bc2244332b9f94f7ce68bc3`.
The `c7i.12xlarge` Spot `i-0f24022d5f6b5ad02` terminated. All 19
terminal-listed artifacts were independently downloaded and SHA-256 checked.

V120 index download and Rust compilation succeeded. The frozen first-16 Rust
nomination screen took 3.59 s of process wall time, 3 s by the runner's
whole-second clock, below the 32 s rejection rule. Full 1,000-query Rust
nomination took 3:03.15. These are offline worker measurements; there was no
live S3 serving latency measurement. The runner prepared 1,000 query records
and 1,000 Rust nominee rosters, but failed before any returned-score replay or
GT download. There is **no V121 recall result** from this attempt.

The compose traceback at `scripts/v121_deep_image_paired.py:253` shows that
CLI dispatch passed `args.requests` (unset in the `compose --queries` call)
to `compose`, which then tried to read `None`. This is a harness argument
wiring error, not a routing or quality observation. A separate launcher error
returned shell exit 0 after printing the failed terminal; terminal status and
exit code were authoritative. Both bugs have focused failing-then-passing
regression tests. The scoring method, index, query cohort and preregistered
gates remain unchanged in the next immutable attempt.

The first 1,000 query vectors were processed in a0001, but no GT values were
downloaded or inspected. The retry is a continuation of the same frozen method
to obtain the originally preregistered paired result. A later algorithm change
will require a new held-out split and preregistration rather than relabeling
these already processed queries as untouched.
