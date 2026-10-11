**BLOCK this exact a0002 draft: one collection blocker.**

A successful two-stage run exceeds the collector’s **64-object cap**:

- The stage scripts emit **51 artifact PUT receipts per successful stage**. [Receipt creation](/data/target/borsuk-source-neighborhood-data-gates-a0002.pending/run-data-stage.sh:80).
- Batch closeout copies both stages’ receipts: **102 files before hydration logs, original exits, drains and other batch evidence**. [Batch copying](/data/target/borsuk-source-neighborhood-data-gates-a0002.pending/run-data-gates.sh:46).
- The collector rejects that inventory before downloading anything. Thus native success cannot satisfy required complete evidence collection. [Collector cap](/data/target/borsuk-source-neighborhood-data-gates-a0002.pending/collect-closed-data-stage.sh:15).

**Minimal repair:** preregister a batch-specific object cap covering the complete emitted inventory, preserve the byte/checksum/deadline requirements, and refresh affected pins. Retain all receipt bodies. The historical 31-file hydration failure did not exercise this successful-run inventory.

Otherwise, the inspected delta is coherent: NEW manifest and OLD package pins verify; both configs and pending transport are byte-identical; configured 6 GiB limits match; MemoryHigh is unset with `max` checks; observed/modelled 1 GiB acceptance predicates are removed. Timer arithmetic reserves request-action grace, and `$0.69 ≤ $0.75` is correctly described as prospective rather than actual cost evidence.

Read-only static review; no scripts, native runs, cloud actions, children, consultations or geometry parsing. Runtime feasibility and actual termination remain unproved.
