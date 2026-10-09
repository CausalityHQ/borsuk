# Composition canary R4 root review — NOT ACCEPTED

No runtime, fixture, ANN or paid compute was run for this review.
The immutable source plan has SHA256 f5d46840c47f40a9ffce9741dddb124c279ddd05b0b654ef556fad61ebe63fae. Preserve it as the unaccepted proposal.

## Required corrections and retained constraints before implementation

1. Use one boot and an After=cloud-final coordinator. No reboot fallback.
   Existing closed AMI evidence proves the direct CLI service and the zero case;
   the devbox socket shim is a different environment. Capture/assert the new target.
2. Bind private canary-owned cloud and run trees at /var/lib/cloud and /run/cloud-init,
   and private configuration at /etc/cloud/cloud.cfg.d. Extra YAML alone cannot isolate
   status_wrapper's global status/result writes. Reset only copied state, replace only
   copied scripts. Admit bounded tree sizes/types/links before copying; preserve original
   boot records. A missing binding or failed isolation must prevent fixture execution.
3. service-stop.sh literally writes terminal.json, final.exit, bootstrap-manager.json
   and bootstrap-manager.put.json under /mnt/borsuk-scale1m. Bind a separate minimal
   case root there for both ExecStart and ExecStopPost; backing directories must be
   outside the target. Never move or overwrite the original boot's root records.
   Capture actual script exit, CLI process exit, and systemd stop-hook values separately.
4. Respect the committed local3000/external3600+120 envelope: setup900, transport900,
   wrapper120, manager cases180 total, evidence/cleanup600, scheduling margin300.
   R4's wrapper300 contradicts that envelope. Charge mkfs, cgroup/IAM checks, original
   finish uploads, copying, stopping, and final publication explicitly. Deadline guards
   refuse admission rather than extend a cap. Timing projections are not measurements.
5. d4 initializes local_stop_epoch=boot+14400 and shutdown+240. Inject explicit
   source-bound overrides to an earlier root-frozen started+3000 before work. State
   exactly which original segments are unchanged and pin all added hooks; do not
   claim the whole prefix is byte-identical. Root watcher remains independent.
6. Use the staged exact38-byte permission probe (SHA6111cbc9f504be3dc43c96dc450f2d30d319ab20ccdcf0acf94d65a518072fc9).
   Instance-role GET must return those bytes; overwrite and delete must be explicitly
   denied, followed by unchanged GET. A nonexistent-key delete is insufficient here.

## Scope retained

One disposable Spot instance; pinned SDK/17 opaque transports/fresh volume admission,
actual cgroups/cleanup, actual qualified ELF usage errors, and disclosed Bash fixtures.
No preparation, derivation, ANN, truth decoding, recall or performance measurement.
No fake NATIVE_CHAIN_CLOSED. OWN1/OWN2/OWN3 and historical receipts remain unchanged.
Root owns actual launch pins, cost admission, binding, termination and both volume-deletion
proofs. Canary source and replay still require review and real target execution.
