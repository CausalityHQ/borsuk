The original-raw S3 Vectors SDK source at `ab9243bf7174d087061f80dd7852e0afe8ffecec` is qualified for source and authentication correctness. This delivery provides no real-service, quality, performance or competitor-win result. Root owns integration and any later service admission.

The clean checkout differs from `1ec2d07f414865f32f6369c00aa93a69ed2c7755` only in `crates/borsuk/src/bin/check_s3_vectors_baseline.rs`. Scale uses `borsuk-s3-vectors-scale-raw-config-v2`: original unnormalized f32LE rows with implicit zero-based ordinal IDs, without ID headers. The frozen workload is 1M rows, D1024, 1,000 queries, cosine, k10; corpus SHA256 is `1a491c060ec8b668b4983042dc1ac3971456327f41f28fd6fd2449df24cf3e38`. The 100k behavior is unchanged. Input pins were inspected in source; this documentation task did not reread dataset payloads or recompute truth.

The full independent review concerned the earlier `b813378f3303b201d750f28f3c0d7d6093d8e96a` candidate. It identified an upload-descriptor authentication gap despite that candidate's passing gates. The repaired source fully authenticates and rewinds the exact upload descriptor before the first PutVectors, retaining identity, EOF and post-readback checks. Its replacement regression swaps same-length finite corpus bytes at GetIndex and requires zero PutVectors, ListVectors, GetVectors and QueryVectors, no successful publication receipt, and successful cleanup. Additional checks reject rehashed unselected NaN/zero tail rows and compare transmitted query f32 bits for nonunit values, signed zero and positive/negative subnormals. These SDK checks use a loopback fixture; they do not establish live AWS behavior. No new independent review was commissioned after repair.

Original SSM `c49e7aba-af35-4a47-9ab9-43922ccd4f75`, unit `borsuk-raw-sdk-gates-a0003.service`, closed with exit 0. Retained evidence in `/mnt/borsuk-workspace/evidence/s3vectors-original-raw-gates-a0003/` records:

- Debug and release SDK binary tests: 16 passed, 0 failed each.
- Release CLI build: exit 0.
- Workspace/all-targets SDK-feature Clippy with correctness and suspicious denied: exit 0; other warnings remain.
- Actual `scripts/check_rust_test_build.sh` with `BORSUK_TEST_BUILD_COMMAND` unset and both jobs settings at 4: exit 0.

The first gate, `4523feb8-7193-4878-a72b-2ca0c38fa344`, exited 101 because the sparse checkout lacked `crates/borsuk-fma/Cargo.toml`. Its a0001 evidence is retained as environment-invalid, without an algorithm verdict. The a0002 gates passed all five stages with 15 debug/release tests on b813378; that result was superseded by the review-required repair and a0003 qualification.

The source, release ELF and full-review SHA256 values, exact gate commands, history and verification receipts are recorded in [verification.json](verification.json). The existing publication packet at `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261016/persistent-rust-workspace-a0001/s3vectors-raw-qualified-a0001/` was uploaded create-only by SSM `ae3fd624-b9d9-4739-b3a3-27f6c6610fe3`, exit 0. Its retained local hash manifest verifies, and all six remote object sizes/ETags match the local packet. The source archive contains only the changed SDK binary source; the packet contains the ELF hash receipt, not the ELF.

Actual original-raw 1M admission, a separate real S3 Vectors canary with bit-exact readback, authorized import/measurement/cleanup, and matched quality/performance/cost evidence remain outstanding. Historical normalized-input results cannot establish a matched win for this raw arm.

Root should integrate these two documents at `docs/research/performance-architecture-20260930/s3vectors-original-raw-input-20261016/`. This worker creates no Git commit, merge or push, changes no source/root checkout, and runs no builds, tests or S3 Vectors calls. The EBS deadline remains 2026-10-16 17:32 UTC.
