# Native authority assurance repair

Base: be3e922d. Ten failed retired f32 native-v2 fixtures used D16, below the current bounded SQ8 D64 eligibility. Move their coverage to the current authority; do not restore retired defaults. Preserve binary identity, sole visibility authority, reload, WAL overlay, tombstones, latest wins, and compaction checks.

The 1/10/100-run test keeps its varied vectors and requires exact expected top-ten identity across partitioning and compaction, finite nonnegative distances within the squared-L2 SQ8 half-step error bound plus f32 rounding allowance. Per-run scales change during compaction; bit equality is not a SQ8 contract.

The initial check passed fourteen cases and rejected the 17th simultaneous delta segment in the hundred-flush case. Retain all hundred flushes; at capacity rejection require zero new PUTs, unchanged authority and durable pending WAL, then callable compaction must drain that WAL and restore bounded headroom. Never increase the admission limit.

Gate: all 15 index::native_bounded_ checks on one frozen-source AWS causality eu-central-1 c7i.2xlarge Spot worker, four Cargo jobs. No local Cargo, full-suite rerun, performance measurement, or new research arm. 30-minute worker cap, 20-minute test timeout, $.30 estimated compute cap excluding EBS/S3. Preserve authenticated terminal artifacts; terminate immediately at completion. Discard interrupted checks, no automatic replacement or overlapping job.
