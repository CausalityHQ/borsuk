# EC2 handoff review reconciliation

Status: source review complete; native runtime gates pending. No paid launch performed.

Review: consultation 01a0b780c7ef4d0c, completed; exact source c3e52c8 and qualified reducer afb70da.

Accepted: reuse both closed local runs; no corpus preparation, derivation, index build or compiler rerun. Three parity arms require completed-native-reduction-config-v4. Expected identity and bound-input rows include phase and must be checked against authenticated configs, receipts and executables on EC2 before reduction. Require exact prefix parity, 314/320 hits and zero underfill separately. Component provenance remains an external prerequisite.

Namespace decision: do not import 13 retained objects into another namespace. Publisher source parses retained_prefix and destination_prefix separately, uses one namespace-prefixed OneAttemptS3 store, and returns the new destination metadata_prefix. Use original retained-source namespace with fresh disjoint semantic/rebound-full1000-a0001 destination. Source objects remain immutable; the fresh metadata/head are create-only. This avoids a duplicate 5,472,467,000-byte transfer/write. Native whole-body authentication and original/current SQ8 identity checks remain required. Actual destination root and metadata prefix are bound only from the native publication receipt.

Baseline backend kind is lowercase s3; publisher backend kind is uppercase S3. Baseline generation_prefix is receipt metadata_prefix, not destination head prefix. The reducer binary recovered from the original compiler archive is 1,871,136 bytes, SHA256 03aca9d786119e15762add1f2f0d9331fc3c697b73214b98c8072a4b1477de4f. Original compilation verified; original compiler campaign closure was invalid and is not relabelled.

Pending: source-bound execution envelope and transport roster; EC2 archive/manifest verification, component provenance, two closed reductions, native retained publication, new S3 panel32, its reduction and three-arm parity; separate real-S3 disposable canary; then frozen full1000 cold measurement. All runtime/native/fixture work is causality EC2 only. No new performance claim.
