# Retained ordinary-S3 library smoke: functional pass

The original `native_s3_conditional_fence_and_gc_smoke` passed on ordinary AWS S3:
**1 passed, 0 failed, 13 filtered; 4.05 s; maximum RSS 22,608 KiB; swaps 0**.
This accepts the tiny conditional-publication, callable compaction/GC, and
subsequent coordinated-open/query path. It does not establish production
readiness or a performance win. Machine-readable evidence is in
[verification.json](verification.json).

The test inserted application IDs 1 and 2 as two-dimensional vectors, compacted
to generation 2, and left ID 99 as a pending insertion. A write-fence cycle
advanced the head epoch by two; S3 refused an update using the old conditional
version with `Precondition`. Callable GC deleted four objects, including a
known orphan. A coordinated open in the **same process** then returned sorted
IDs `[1, 2, 99]`, preserving the live compacted and pending data.

The test used only its dedicated namespace in
`borsuk-bench-453182569524-euc1`:
`research/native-library-check/gc/20261016-70383e2bf02e7b5acf095cda01c82981/`.
After the query, test teardown removed that namespace. Independently recorded
CLI listings show `KeyCount: 0` before execution and after cleanup, with the
final listing untruncated. The empty namespace is a teardown result, not a claim
that GC deleted live index data.

The product goal requires **automatic OR callable in-process compaction/GC**.
The exported `two_bit_compaction::compact_two_bit_index` and
`two_bit_gc::collect_two_bit_garbage` satisfy the callable API requirement and
were exercised here without a separate maintenance instance. Root's retained
`root-reconciliation.txt` supersedes the audit's invented AND requirement;
this proof does not justify building an automatic owner/daemon.

The successful run used the existing qualified, unshimmed test-build artifact
`two_bit_application_ids-9edd18c87a3ea383`, explicitly named in
`s3vectors-original-raw-gates-a0003/testbuild.log:6575`. No fresh compilation
occurred for this successful attempt. The executed and frozen artifact SHA-256 is
`380a9cc82990e0b9e75764b5a17b2d76f9cb81909d7803573f48fadd93d6e317`.
Source was `ab9243bf7174d087061f80dd7852e0afe8ffecec` at
`/mnt/borsuk-workspace/release-next-source`; root identifies canonical
`37a23e8a2` as code-equivalent. The only path changed since
`1ec2d07f414865f32f6369c00aa93a69ed2c7755` was the SDK benchmark binary
`crates/borsuk/src/bin/check_s3_vectors_baseline.rs`; library and test sources
were unchanged. The six source hashes in the closure's `source.sha256` and
the four recorded runtime-library hashes were rechecked successfully.

Earlier attempt `library-real-s3-smoke-a0001` was **environment-invalid before
native execution**: compilation exited 101 because linker `cc` was missing.
Its initial package installation exited 100 on a stale header-package HTTP 404.
After refreshing indexes, a plan requiring one upgrade was refused before
installation. These records establish neither a fresh compiler pass nor a new
source defect.

Original execution was guarded by 8 GiB RAM, zero swap allowance, CPU 8 with
100% CPU quota, 256 tasks, and a 180 s outer runtime limit; the native command
had a 120 s timeout with a 5 s kill grace. These are execution guards, not scale
or performance measurements.

This proof does not cover full logical update/delete CRUD, process-crash or
restart recovery, ANN quality/recall, scale, matched latency/throughput, or a
total-dollar win against S3 Vectors or Turbopuffer. Reopening here happened in
the original process. The two-dimensional, three-live-ID query is a functional
oracle only.

Original receipts and durable evidence:

- Unit `borsuk-library-real-s3-smoke-a0002`, invocation
  `b93f43a89c5a46a9afede98072079056`.
- Dispatch SSM `230455d4-f21b-4115-91dd-c91362b76dcf`; full terminal result SSM
  `7440b6e1-c60d-44e2-a112-6c7f33b3416f`: Success, response code 0.
- Closure SSM `76db18b3-d756-4e57-ad57-c43deab90b5a`: Success, response code 0;
  root uploaded evidence create-only, with terminal last.
- Closure prefix:
  `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261016/persistent-rust-workspace-a0001/library-real-s3-closure-a0001/`.
  Its `evidence.tar.gz` SHA-256 is
  `7b7116aa90a7445765e14239468a83625027574e2b772aeec144743bb129fbcb`;
  `binary.sha256`, `source.sha256`, `terminal.json`, and the frozen
  `two_bit_application_ids` accompany it.
- Retained smoke files:
  `/mnt/borsuk-workspace/evidence/library-real-s3-smoke-a0002/`
  (`run.sh`, `native.*`, inventory, namespace listings, qualification,
  source delta, binary hash, caller identity, and terminal record).

This documentation was prepared by reading retained evidence and source on
EC2. No build or test was rerun and no source was changed. Root owns canonical
Git integration under
`docs/research/performance-architecture-20260930/library-real-s3-smoke-20261016/`.
