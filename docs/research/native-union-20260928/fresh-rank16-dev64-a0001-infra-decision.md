# Fresh development a0001: infrastructure failure before ANN

2026-09-29. Original Spot `i-0bb37dc0648c62d34` ended with authenticated
terminal `failed`, phase `measure`, exit1, after77s; the instance is terminated.
Seven closed artifacts matched S3 SHA256. The wrapper reached the retained
`two_bit_plan_demo` but it exited before a reference or quality artifact.
The source path downloaded only `generation/manifest.json`, while
`TwoBitGeneration::open` requires `generation/plane/manifest.json` and seven
other local metadata files before any query; those files were absent. No
fresh ANN recall, HTTP latency, QPS or vendor result exists from a0001.
Spot compute$0.0038 is an estimate excluding EBS/S3. The nested native log
was not in the terminal artifact roster, so the exact native error text is
unavailable; the deterministic missing-file precondition establishes the
execution defect. The same source/reference binary succeeded in the prior
qualified campaign with the full metadata roster.

Repair for a **new unique attempt**: fetch and SHA-check the same ten
generation artifacts from the previously qualified current1M configuration,
including canonical source bytes required for publication. Sync native logs
even on failure. Retain source, query/GT, root, scorer, binary, R@10>=95%,
HTTP p90<444ms and offered8QPS gates exactly; no architectural change or
threshold adjustment. Do not relaunch a0001 or call it a scientific KILL.
