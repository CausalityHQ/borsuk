# Canonical-input S3 Vectors preparation a0001

This fresh packet is preparation only. No real payload, truth-equivalence,
service, credential, resource, performance or billing admission is established.
Historical packets retain their original identities and dispositions.

The runner retains:
`check_s3_vectors_baseline <admit|prepare-scale-canary|publish|measure|reduce|cleanup> CONFIG CONFIG_SHA256`.
Only `borsuk-s3-vectors-canonical-config-v3` is accepted. Old raw schemas and
implicit physical-row mappings are refused. Full mode is `scale-measurement`;
`scale-canary` is an ID-plumbing fixture with no quality/performance claim.

The full config binds the current generation manifest digest, canonical
geometry/digest, physical-order ID digest, logical-order export digest, exact
query/truth artifacts, new run/resource names, account, region, host,
`service_mode: "ENHANCED"`, `cleanup_owner` and `runtime_seconds` (1..7200).
The runtime field records the external controller budget; this runner does
not time out or drop command futures. The root must enforce hard wall/memory
bounds and separately qualify live timeout, drain and reconciliation behavior. Paths are absolute; artifacts contain
`path`, `bytes`, and `sha256`. State directories must already exist.
No legacy migration or packet reuse is provided.

`canonical_input` contains `rows`, `generation_manifest`, `canonical`,
`order`, `logical_export`, `verifier_executable_sha256`, and two proofs:
`export` and `recover_order`, each with `receipt` and `terminal` artifacts.
Full `corpus` must be exactly `logical_export`, including its path.
Canary corpus must be the authenticated first 257 logical IDs, never the
first 257 physical records. Small synthetic source manifests are accepted
only in canary mode and cannot qualify a full measurement.

Use the source-matched native verifier twice with fresh output/receipt paths:

```text
verify_canonical_f32 --recover-order CANONICAL 4104000000 CANONICAL_SHA 1000000 1024 ORDER_SHA NEW_ORDER NEW_ORDER_RECEIPT 1048576
verify_canonical_f32 CANONICAL 4104000000 CANONICAL_SHA 1000000 1024 EXPECTED_LOGICAL_EXPORT_SHA NEW_EXPORT NEW_EXPORT_RECEIPT 1048576
```

Take the full hashes from `protocol.json`. The expected export digest is a
falsifiable expectation, not observed proof. Do not substitute a newly
observed digest on mismatch.

Verifier receipts are deliberately provisional, including the final filename.
A `.pending` receipt is never valid. The root must observe the original
process returning zero after final receipt directory sync, retain that exact
job's evidence, and freeze an authenticated terminal attestation:

```json
{
  "schema": "borsuk-canonical-verifier-exit-v1",
  "operation": "export",
  "original_job": "EXACT_ORIGINAL_JOB_HANDLE",
  "exit_code": 0,
  "receipt_sha256": "SHA256_OF_FINAL_RECEIPT_BYTES",
  "source_sha256": "SHA256_OF_verify_canonical_f32.rs",
  "executable_sha256": "SHA256_OF_EXACT_VERIFIER_ELF"
}
```

For the order operation use `operation: "recover-order"`. Config SHA256
authenticates both terminal files and both native receipts. Attestations
are root-supplied evidence, not a process result reconstructed by this
runner; never fabricate one from the presence of a provisional receipt.
All receipt fields are decoded strictly. The source hash must equal the
verifier source embedded in this runner, and both receipts/terminals must
match the frozen verifier ELF hash. Admission reauthenticates the generation
manifest, canonical stream, complete in-range ID permutation, recovered order
and entire logical export. Every service key is the six-digit logical ID.
Existing complete enumeration and bit-exact readback checks remain.

## Live path remains unqualified

Cargo.lock remains pinned to aws-sdk-s3vectors 1.21.0 and aws-config 1.8.14.
The typed Index model omits indexMode, but the runner's existing raw GetIndex
capture may observe it. CreateIndex has no indexMode field; fresh-bucket service
defaults and actual observed mode require separate live qualification.
Typed field absence does not establish that an SDK upgrade is necessary.
Service::new rejects live commands before credentials or transport for this
preparation slice. Captured index identities must contain
`indexMode: "ENHANCED"`; omission or mismatch is invalid. Mock responses test
that check only. No dependency change or actual service probe was performed.

## Root-owned next gates

1. Qualify this exact native source, lockfile, toolchain and both release ELFs.
2. Authenticate all real inputs/export and run exhaustive f64 cosine top10 for
   queries 0..7 with logical-ID tie breaking, bounded by 600 seconds, four CPUs,
   4 GiB, swap0. A timeout is execution INVALID. Eight rows cannot prove the
   entire original truth equivalent after normalization rounding.
3. After separate authorization and live-path qualification, run a disposable
   257-row/two-query real canary, including complete readback, seal/reduction,
   cleanup and independent absence verification. Synthetic truth proves
   plumbing only.
4. Freeze one new full1000 paired protocol from the exact current BORSUK and
   S3 source. One attempt, Q1, ordered identical query bytes, k10/cosine,
   IDs+distances, no vectors/metadata/filter, zero query delay and zero
   artificial idle. Provider cache remains uncontrolled. Align dispatch-to-
   validated-result clocks and response semantics; report initialization,
   first-query latency, nearest-rank p90/p95, whole-pass QPS and attempts.
   Seal all 1000 responses before opening truth. No resume/partial reduction.
   Publication retains eight in flight and 250 ms PUT spacing.

The metric is recall against the frozen original fixture truth; exact
canonical truth equivalence remains unproven. No matched vendor win follows
from this preparation.

Root must approve a numeric total lifecycle spend envelope before mutation:
ingestion, request attempts, complete verification GET/LIST, retained storage,
queries/processing/returned data, compute, EBS, network/NAT/cross-region traffic,
construction, maintenance/rebuilds, recovery, overlapping generations and
cleanup. SDK payload counts are not invoices. Reserve cleanup time and retain
original job/resource receipts. Turbopuffer requires its own authorized
credentials, pinned native contract, colocated region and equally qualified
readback, retries, clocks and cost envelope.
