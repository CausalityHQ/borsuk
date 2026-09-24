# V117 no-implicit-retry S3 reader compile closeout

The production-facing `OneAttemptS3` constructor disables `object_store`
0.14.1's default request retry loop (`max_retries = 0`) and keeps the generic
store fetcher private. A future query coordinator must count any explicit
retry against the 32-GET cap. This removes one source of hidden data GETs;
it does **not** yet verify actual wire-request counts or HTTP headers.

The compile snapshot was base commit `7c9571229eb5fe835be3e8bf8dc4760ec32ea036`
plus `crates/borsuk/src/sq8_s3_range.rs` at SHA-256
`81d114a191c494534c6ddd077f90fcbe11fbf4c0d7faf100326e8c3c4605ac90`.
Source archive SHA-256:
`ba00584b4309f62656c73721fc2a4a8bab14aa1847c6ae139f21ff2dec570e6a`
(12,066,356 bytes). The immutable attempt is
`s3://borsuk-bench-453182569524-euc1/research/v117-s3-no-retry-compile/7c957122/runs/v117-20260924T000200Z/a0001`.

Causality Spot `c7i.4xlarge` `i-095c81bc1834a0978` completed the full
BORSUK crate compile and targeted
`sq8_s3_range::tests::conditional_short_tail_rejects_mutated_object` test
with exit 0 in 150 seconds. One test passed; 1,601 were filtered. All four
terminal-listed artifacts were independently checked against SHA-256 and
byte length (52,550 bytes total). The instance was terminated. The test
uses in-memory storage; it does not exercise a real HTTP/S3 response,
retry, Content-Length, 206/412, or concurrent query accounting.

Before a live query claim, run an adversarial local HTTP fixture for one
attempt, range/status/ETag/length faults, and midstream truncation. Then
record actual and logical GETs per query and test conditional S3 reads from
one pinned generation. No latency or cost figure was measured here.
