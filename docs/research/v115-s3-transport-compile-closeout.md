# V115 S3 transport compile closeout

The corrected a0002 Causality Spot compile cell passed at frozen source
snapshot SHA-256
`f4914e7b417b589803edc502dfb52b4229b73d91627dd214702336b4cc3e02d9`.
It ran `cargo test -p borsuk --lib sq8_s3_range::tests --jobs 4 -- --nocapture`
on `c7i.4xlarge` `i-07a34b40ebf17020e` in eu-central-1c. The terminal
reported `complete`, exit 0, 139 seconds. The test built the full BORSUK
crate and passed `conditional_short_tail_rejects_mutated_object` (one test;
1,599 filtered out) in an in-memory object store. The Cargo command took
98.03 seconds wall and had 4,837,192 KiB peak process RSS. These are build
resource observations, not query latency or serving memory.

The terminal SHA-256 is
`8fbcb24ef92e197944f7225410cdbe58f12339bfed75fc230f88f78b87ea9e40`.
All four terminal-listed artifacts were independently fetched and checked
for byte length and SHA-256. The instance was confirmed terminated after
terminal closure. The immutable attempt is at
`s3://borsuk-bench-453182569524-euc1/research/v115-s3-transport-compile/f4914e7b417b589803edc502dfb52b4229b73d91627dd214702336b4cc3e02d9/runs/a0002`.

The original a0001 source snapshot SHA-256
`37333f125fd9642dc6d8829c491e7bdf9ce4563d277be692a044ede7d5fd75b2`
failed in 39 seconds, exit 101, before compilation because its harness called
Cargo outside the archived workspace without `--manifest-path`. All four
a0001 artifact hashes were checked and its Spot instance terminated. The
a0002 archive corrected that harness error and also included the
`ObjectStoreExt` test import and bounded response collection.

This test establishes compilation and a conditional short-tail/mutated-object
case for the frozen adapter source. It does not exercise real S3 HTTP status,
Content-Range, retry behavior, GET counts, returned ANN IDs, live latency,
throughput or charged serving memory. A later generation-identity fence and
router allocation fix were developed after the a0002 snapshot and have only
their focused standalone tests as of this closeout. The next gates are a
frozen 1,000-query composed returned-ID/hit replay, adversarial HTTP range
tests, and then live paired RAM/NVMe S3 execution.
