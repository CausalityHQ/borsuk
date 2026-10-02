# Bounded publication exact-source qualification

Candidate cb63b58ffd9de57ca78aacfb2e7db67fc3644955 incorporates da56bcb080f559c81886e4e603226bd5e5415564 plus the reviewed upload-metadata reserve. Exact 399 Rust/Cargo identities and the three-file delta are pinned in native-source-manifest.json. Source is **UNVERIFIED**; the manifest is admission authority, not completed assurance. Root main retains the previously qualified native source until this gate passes.

The original local job stopped under sustained host PSI before tests. Preserve that stop; no local full suite or retry. One new interruptible remote qualification will run the unchanged frozen candidate in a fresh target directory.

## Required gates

Seven serial stages: exact semantic publication parity fixture; remaining affected two_bit library tests excluding that fixture; sq8_s3_range library tests; graph publication integration test; locked HTTP release; locked workspace/all-target Clippy correctness and suspicious denied; real workspace test-build script with its test override unset. The first four stages must execute nonzero tests. Require exact commands, start/end and zero exits, source-before/after equality, binary identity, complete resource and artifact receipts. No full workspace test execution claim.

## Resources and ownership

Use existing Spot lifecycle, AWS causality profile, c7i.2xlarge in eu-central-1, Ubuntu 24.04 x86_64. Service MemoryMax 8 GiB, MemorySwapMax 0, CPUQuota 200%, TasksMax 512; Cargo jobs1, test threads1 and existing two-thread runtime settings. Test/service/machine ceilings 7200/7260/9000 seconds; maximum Spot price $0.50/hour, compute estimate cap $1.25 plus $0.15 EBS/S3 allowance. Those are prospective qualification safety/cost limits, not measured product requirements.

Freeze controller commit and code map, manifest pointer and config before launch. Package the exact candidate in an isolated clean bundle; no remote Git or model work. Fsync every ACKed instance ID, retain the original controller handle, monitor terminal/infrastructure only, terminate and wait for all owned IDs before authenticating every terminal artifact. Interruptions and failed stages remain FAIL; no automatic replacement or duplicate run.

## Decision

Integrate native code only after all seven actual gates pass on the exact source and required receipts authenticate. Any failure returns to the same source worker with its concrete failing layer. The source-derived memory counterexample becomes evidence only when executed; allocation models are not RSS. No ANN quality, cold latency, vendor win, scale or lifecycle-cost claim follows from compilation assurance. Synchronous local validation remains uncancellable during its scan and assumes immutable trusted staging.
