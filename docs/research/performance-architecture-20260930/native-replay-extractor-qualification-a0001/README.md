# Rust closed-archive extractor qualification

Exact source: `5660b97d7c153e265cd96688113217717d61bd61`.
Frozen protocol: `70dce6735bc830e4adf243fce148aa13f8fc194a`.
Execution: 2026-10-10/11, causality EC2 Spot, eu-central-1c,
`i-04944188da589804a`. The instance was terminated immediately after
the final stage; AWS termination wait exited 0 and its owned root volume
`vol-0255887ef805bf359` was deleted.

The utility authenticates the complete compressed archive, manifest and
authority, verifies every regular member, and extracts exactly twelve
required replay members into a new private directory. It checks retained
file/directory identities and durability. It rejects unsafe paths, links,
duplicate members/JSON fields, malformed headers, hidden tar/gzip tails,
corruption and changed inputs. It is an experiment utility; ANN library
scoring, routing and storage code are unchanged.

## Verified gates

- Real success/nonzero/timeout platform canaries: native/manager exits
  0/0, 1/1 and 124/124, respectively; all owned cgroups drained.
- Exactly seven compiled and executed extractor tests, all passed.
- Locked release build; retained ELF is 944544 bytes with SHA-256
  `2525dfcedfb8b13b553e83466d96aa04b90f0f3a5fda78e19784a282a98f40f5`.
- Locked workspace/all-target Clippy correctness and suspicious gates.
- Actual environment-unset `scripts/check_rust_test_build.sh`, jobs=1.
- The same release ELF authenticated the historical archive's 758 members
  and the local archive's 806 members, extracting twelve from each.

All six serial stages exited 0, with authenticated retained bodies and
the same 463-file source map before/after/root setup. The native map has
436 files. Compiler units used CPU 0-1, 200% quota, 8 GiB RAM, zero swap
and PID limit 512; archive units used CPU 0, 100%, 1 GiB, zero swap and
PID limit 128. OOM and OOM-kill counters were zero. The workspace test
build reached its 8 GiB cap and recorded 2284 memory-limit events; this
is retained, not represented as absence of memory pressure.

`qualification.json` records exact commands, original SSM command IDs,
resource peaks, source identities and lifecycle. Each stage directory
contains its original body hash manifest and selected receipts. Complete
original logs, manifests and the release ELF are retained create-only at:
`s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261010/native-replay-extractor-qualification-a0001/`.

## Scope and next gate

This proves Rust compilation, platform mechanics and authenticated archive
inventory extraction. It does not prove the original experiment's semantic
closure, ANN quality, recall, cold latency or superiority to a vendor.

Next: use Rust to bind/reduce the original completed replay against its
exact request/truth bytes; establish retained-generation S3 serving parity;
then run the frozen full-1000 cold experiment after actual-input admission
and a separate S3 canary. Preserve both historical archives and their
original dispositions. No corpus/ground-truth regeneration is needed.
