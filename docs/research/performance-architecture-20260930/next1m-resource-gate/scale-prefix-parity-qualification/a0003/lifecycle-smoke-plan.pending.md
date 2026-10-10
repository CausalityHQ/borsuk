# Pending disposable causality EC2 lifecycle smoke

Purpose: qualify the collector alone, not repeat Cargo, ANN, corpus or vendor measurements. Await exact review reconciliation before freezing script/request.

Use causality eu-central-1 Spot, same Ubuntu AMI as a0003, root encrypted gp3/DeleteOnTermination; one instance, no data volumes. Root-side finite watchdog, instance-side finite shutdown timer; create-only launch claim and S3 evidence prefix. Preregister small compute/ancillary ceiling before launch. Sequential owned units only, no shared-unit controls. Each unit has exact CPU1/256MiB/swap0/PID128/KillMode=control-group and finite runtime/stop bounds. Shell dummy payloads only; no toolchain install.

Mandatory cases against source-bound collector: successful payload whose unit unloads before collection; nonzero payload with original exit retained; owned live descendant stopped and exact cgroup proved empty/absent; missing or malformed saved cgroup path refuses; missing or nonzero launcher/final exit refuses success; cgroup disappearing between observation and stop must not be falsely accepted from stop status alone. Manager/read/drain failures stay INVALID. No broad process killing, no overlapping cases, stop batch on unproven cleanup.

Preserve collector hash, exact properties/argv, observed statuses and cgroup events/path presence, case-original and collector exits, file checksums and per-case timestamps. Terminal evidence uploaded create-only; root terminates/waits same instance and verifies exact root VolumeId absent before collecting. Success is lifecycle mechanics only, no compiler rerun/performance/cold/quality claim. Original a0003 remains INVALID96.

Status: plan-only, no executable fixture, no launch authorization implied, no paid job started.
