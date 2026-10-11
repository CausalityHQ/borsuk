**BLOCK — two concrete defects remain in the a0002 collection and timeout paths.**

1. **A successful run exceeds the collector’s 64-file limit.** Each successful native stage produces 51 artifacts and therefore 51 PUT receipts. [run-data-gates.sh:46](/data/target/borsuk-source-neighborhood-data-gates-a0002.pending/run-data-gates.sh:46) copies both stages’ receipts into `batch`: **102 receipt files alone**, before batch logs and resource evidence. [collect-closed-data-stage.sh:15](/data/target/borsuk-source-neighborhood-data-gates-a0002.pending/collect-closed-data-stage.sh:15) rejects that inventory before downloading anything.

   **Minimal fix:** preregister a separate bounded batch allowance covering the full inventory, retaining the byte limits, exact manifest checks and opaque collection. This mismatch is carried into a0002; it prevents complete acceptance even when both native stages succeed.

2. **Timeout classification overrides known evidence failures.** [run-data-stage.sh:52](/data/target/borsuk-source-neighborhood-data-gates-a0002.pending/run-data-stage.sh:52) assigns `INCOMPLETE` whenever `native==124`, including when `slice_ok=false` or drainage is unproved. Moreover, [native-data-inner.sh:47](/data/target/borsuk-source-neighborhood-data-gates-a0002.pending/native-data-inner.sh:47) exits on timeout before validating the collected peaks and resource receipts. A timeout accompanied by missing receipts or a failed environment check therefore receives the wrong disposition under the new protocol.

   **Minimal fix:** select `INCOMPLETE` only for an established safety timeout with valid resource receipts and confirmed drainage; give evidence/environment failures `INVALID` precedence. Preserve the original exits.

I read the full protocol delta and historical release repairs/reviews, verified all 15 draft hashes and the original package manifest, and confirmed byte-identical configs and pending transport. The changed memory settings/readback predicates consistently use 6 GiB; no concealed 1 GiB acceptance guard remains in the reviewed wrappers. Identity changes and nominal timer/cost arithmetic are consistent, with actual termination still separately required.

No files changed; no experiment scripts, tests, data parsing, cloud actions, children or consultations ran. This review makes no Rust, algorithm or native qualification claim.
