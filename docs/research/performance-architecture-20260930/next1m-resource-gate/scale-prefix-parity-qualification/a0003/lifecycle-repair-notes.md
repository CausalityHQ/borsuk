# Pending qualification collector repair

Original a0003 remains closure INVALID (96). Native six stages exited 0; 86 tests passed. No historical disposition is overwritten.

The original collector queried ControlGroup after systemd unloaded the completed transient unit; it then stopped a nonexistent unit and recorded 96. The pending collector uses the exact cgroup path saved by the payload, rejects any unexpected path, stops the unit if that cgroup still exists, and admits an already absent cgroup only when both launcher and payload final exits are zero. Existing drain checks and bounded manager operations remain.

Pending: independent review and causality EC2 disposable success/failure/remaining-process lifecycle smoke. This source-only draft is not runtime-qualified and authorizes no launch. No Cargo rerun is needed merely to validate collector behavior; preserve the existing compiler receipts.
