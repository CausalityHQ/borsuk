# SOURCE stream ownership witness

This diagnostic is compiled only in the native library test ELF. Production
transport, collector policy, public API, schema, defaults and layout are unchanged.

## Root-only inputs and admission

Set BORSUK_SOURCE_OWNERSHIP_INPUT to a UTF-8 JSON file with exactly:
manifest_path, sidecar_path, manifest_sha256, generation (positive u64),
bucket, region, prefix, key, etag, units (positive integer), report_path.
Paths and S3 pins come from the trusted generation root. Do not infer pins from
untrusted response metadata. ETag includes its original quotes. Prefix is the
physical namespace; key is the logical SOURCE records key.

The input file, pinned v3 SOURCE manifest and complete digest sidecar together
must fit 8 MiB. PageAuthority::load_two_bit checks the manifest and sidecar hashes,
generation and source geometry. The range is units [0, units), including a final
partial-size unit only when it is the complete original final unit. It must fit
4 MiB. Report destination must not exist. No queries, truth, corpus files, scores
or cached bodies are loaded.

Run these exact native ELF commands separately, with TEST_ELF replaced by the
qualified ELF path:
- Admission: BORSUK_SOURCE_OWNERSHIP_INPUT=/absolute/input.json TEST_ELF
  --exact sq8_s3_range::tests::source_ownership_admission --ignored --nocapture
  --test-threads=1
- Disposable canary: TEST_ELF --exact
  sq8_s3_range::tests::source_ownership_local_http_canary --nocapture --test-threads=1
- Root witness: BORSUK_SOURCE_OWNERSHIP_INPUT=/absolute/input.json TEST_ELF
  --exact sq8_s3_range::tests::source_ownership_native_witness --ignored --nocapture
  --test-threads=1

Admission opens metadata only, constructs no S3 client and creates no report.
The canary mocks SOURCE metadata and payload, uses a real loopback HTTP server and
the native one-attempt HTTP transport, verifies conditional range headers, and
checks wrong ETag/range, corrupted body and truncation. It also refuses wrong\nHEAD length or ETag before submitting any data GET. It makes no AWS requests.
The witness first performs one explicit native SOURCE HEAD on the same reader,
checks its length and ETag against the pins, and only then snapshots transport
counters. Locked object_store uses this connector for IMDS token PUT, role GET
and credential GET; those credential calls share the transport counters.
The HEAD resolves credentials into that reader's existing credential cache.
The report records the preflight transport totals separately, without assuming
a fixed count or subtracting a guessed credential allowance. The data-GET delta
must still be exactly one; a credential refresh inside that interval fails closed.
This preflight reads no SOURCE payload and does not enable application caching.
The witness then makes one conditional range GET with OneAttemptS3, http1_only,
concurrency 1, cache off and no retry. It runs the existing F256 collector,
crossing-record preparation and full original-unit verifier, with a no-op callback.
The root must enforce CPU1, 512 MiB memory, no swap, TasksMax128, RuntimeMaxSec120,
and run no concurrent tests in that process.

## Observation and interpretation

Only scalar observations are taken before consuming incoming Bytes. The observer
does not clone, slice or retain the incoming Bytes, and uses no unsafe APIs.
It records length and is_unique for each of the first 256 nonempty conversions,
pre-normalization Vec capacity and whether the visible address was preserved.
Chunks and bytes, including empty chunks, are counted in bounded aggregates.
The fallback has no Bytes-to-Vec conversion: its conversion observation is
explicitly unavailable. Prefix coalescing and subsequent appends are separately
counted. Capacity normalization and crossing-record copies are separate.
The component copy sum must equal the actual collector copy counter; crossing
copies must equal the existing owned-copy counter; observed length must equal
the fully authenticated range length.

Address equality is not allocation identity. An offset unique view can move its
visible bytes within an existing allocation. Visible length and is_unique do not
bound the backend owner allocation or RSS. The collector's normalized payload
bound does not establish a whole-backend memory bound. This debug ELF makes no
timing, latency or production performance claim.

Reports are compact JSON, capped at 64 KiB, written with create_new only after
header, ETag, length, original-unit hashes, copy ledger and physical GET counters
succeed. They contain pins and observations, never payload bytes. A disk write
failure can leave an incomplete new report; only successful test exit admits it.
Do not reuse or overwrite that destination on failure.
