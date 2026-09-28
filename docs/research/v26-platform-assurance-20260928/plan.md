# Historical V26 platform contract

Base1643c471. Latest full workspace library1687pass/6ignored, WAL29pass, portablePQ4 17pass, then historicalV26 85pass2fail. These runner fixtures unwrap success on x86 despite immutable V26 fused scorer being explicitly AArch64-only. Trace both local runners to select/score functions atlib.rs1623/1696/1757; do not substitute portable SSSE3 into historical numerical methodology.

Both fixture tests still execute the real runner. On non-AArch64 assert exact unavailable error and no evidence artifact; quality fixture still corrupts the authenticated query identity and verifies rejection/no output on every architecture. On AArch64 retain all original128/32evidence rows, fixed depth/warmup/claim assertions. No production code, benchmark format, default or historical artifact changes; no test ignored/skipped. Rename tests to describe platform contract.

Gate: borsuk-v26 library87cases first; only on success one full locked workspace/all-targets gate on same source/worker. One causality eu-central-1 c7i.2xlarge Spot, fourjobs,1800swall/1500stests/$.30compute estimate cap excludingEBS/S3. Authenticate terminal and changed source, terminate immediately; no overlapping job, localCargo or benchmark, no automatic replacement. ARM positive runner execution remains unmeasured on this x86 worker; do not claim ARM or performance qualification.
