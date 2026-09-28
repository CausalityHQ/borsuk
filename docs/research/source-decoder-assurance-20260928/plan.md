# Physical source decoder fixture repair

Base10814d81. Latest full workspace gate passed library1687/0/6ignored, then V36 prefix integration target62pass/1fail. Projection fixture writes a two-row physical Parquet row group but requests decoder maximum1row. Shared admission validates the physical decoder allocation before reading; expected rejection is correct. Every caller of project_v36_prefix_source_resident_file uses this same bounded scanner.

Fixture now asserts that the original two-row group is rejected under a one-row cap, then writes one-row groups with existing Parquet WriterProperties and preserves scalar projection/replay and one-row emitted block assertions. No production change or relaxed decoder admission.

Gate: first cargo test --locked -p borsuk --test v36_prefix_dataset --jobs4 (63 checks). Only on success run cargo test --locked --workspace --all-targets --jobs4 once on the same frozen-source active worker. AWS causality eu-central-1 c7i.2xlarge Spot, fourjobs,1800swall/1500stests/$.30compute estimate cap excludingEBS/S3. Preserve authenticated terminal and terminate immediately; no overlapping gate, localCargo, performance measurement or automatic replacement.

First launch supervisor misclassified EC2 initial eventual-consistency NotFound and terminated the newly created instance; independently confirmed terminated before retry. No test terminal was produced; discarded execution. Manual diagnosed retry handles only initial InvalidInstanceID.NotFound during the first60seconds as visibility delay, within the same original monitor; no overlapping job or automatic interruption replacement.
