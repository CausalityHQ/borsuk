# Bootstrap review reconciliation

Reviewed source: `92bbe8fc582b903dd003dd24ae2d98e469dd6a57`. Dual review `4f337907fa054712` completed: engineering `201fcbebf0954cdd`, research `662e6d1d61c142df`. Both full reports are retained. This document admits no launch or scientific result.

## Required source repairs

- Arm exact-instance bounded termination cleanup before filesystem or metadata errors can exit the watcher. A late watcher still attempts termination and reports its cost bound as unproven.
- Bound scratch-binding requests and manager-proof waiting by absolute deadlines, including acceptance after a request completes.
- Authenticate EC2 LaunchTime and use the earlier start time. Preserve the original response and discrepancy.
- Keep native chain/bootstrap final exit distinct from cloud-init manager exit. Prospective mapping is final 0 to cloud manager 0/success, and final 2 or 3 to cloud manager 1/exit-code. Actual target cases 0, 2, and 3 remain mandatory canary evidence; this mapping is not yet runtime-qualified.
- Require exactly one scratch-binding JSON value. Publish collection status only after closure and manager validation, with manifest verification explicitly left to independent replay.

The same worker is repairing the source. Historical snapshots and original receipts remain unchanged. The independent replay verifier is a separate pending file; source review alone does not qualify it.

## Scoped infrastructure change

The instance profile uses `borsuk-bench-role`, which has `AmazonS3FullAccess`. Root caller is the distinct IAM user `borsuk-codex-automation`.

Root installed inline policy `BorsukScaleInputsImmutable20261009` after confirming it did not previously exist. Its exact readback matches the proposed document. It denies instance-role object mutation only under these prefixes:

- `research/semantic-router/20261009/actual1m-scale-chain-canary-a0001/inputs/`
- `research/semantic-router/20261009/actual1m-scale-chain-a0001/inputs/`

IAM simulation checked all 12 action/resource combinations: PutObject and DeleteObject denied for both new input prefixes; GetObject allowed; evidence and the historical control prefix remain allowed. The simulation is IAM evidence, not a real target S3-access test. No instance was launched. Existing attached policies and campaign objects were not changed.

Policy installation/readback unit `run-p2943688-i665534567.service` exited 0. Readback SHA256: `38ef94327e8ff0c67622474cc9bc4d9d1d88351eb0fe1d6fbe4e5e3427fba903`. Simulation unit `run-p2912224-i665498366.service` exited 0; the subsequent per-resource assertion also passed before installation. Both used CPU1/AllowedCPUs0/256MiB/noSwap/pids128/120s.

Before a canary: freeze exact sources/support and resource limits; authenticate fresh launch and volume responses; prove target exit mapping, actual staging permissions, cleanup, and scratch deletion. Before measurement: pass independent closed replay and the separate canary, then freeze the actual experiment. No recall, performance, or competitor win is established here.
