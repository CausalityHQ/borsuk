# V118 HTTP range fault fixture closeout

The `OneAttemptS3` adapter was exercised through the real `object_store`
Amazon S3 HTTP client against a loopback server with static credentials.
The fixture counted the wire requests and tested an authenticated short
final page, the emitted `Range` and `If-Match` headers, and fail-closed
handling of 200 instead of 206, wrong Content-Range, changed ETag, mutated
page bytes, truncated body, 412, and 500. Each fault produced one request:
the configured `max_retries = 0` did not hide another data GET in this
fixture. This is transport evidence, not a live AWS S3 timing result.

The frozen test snapshot was commit
`2d9e145f1d2c9200e0ff21319145de6dd10b6859` plus
`crates/borsuk/src/sq8_s3_range.rs` SHA-256
`c9c56b1a4370685f6aac3d0293e029a17907187c3962641cb347e17102be7ba2`.
Archive SHA-256:
`13856f63d63690bad818f68506523d4047905315c38ca050c0fa6279b621ce89`
(12,070,369 bytes). The immutable attempt is
`s3://borsuk-bench-453182569524-euc1/research/v118-http-fixture-compile/2d9e145f/runs/v118-20260924T001525Z/a0001`.

Causality Spot `c7i.4xlarge` `i-03e40076d8b4d383a` compiled the full
crate and ran both targeted `sq8_s3_range::tests` successfully, with 1,601
other tests filtered. The terminal was complete with exit 0 after 146
seconds. All four terminal-listed artifacts were independently checked
against size and SHA-256 (52,642 bytes); the instance was terminated.

The next transport gate must use one generation-pinned live S3 object,
count logical and actual GETs at C1/C8/C32, and measure returned quality,
tail latency, throughput, charged memory and S3 errors. This fixture did
not cover AWS endpoint behavior under network interruption, concurrency,
hydration, or generation rollover. An exact wire `Content-Length` header
check is still absent from the `ObjectStore` abstraction; the current
reader verifies the collected payload length and page SHA-256.
