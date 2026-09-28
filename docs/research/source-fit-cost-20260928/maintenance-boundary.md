# Source fitting and compaction boundary

Inspected e6c2e97; source facts, not performance measurements.

`canonical_source::merge_canonical` streams the base in existing physical order, skips all mutated IDs, then appends sorted live puts. It preserves the relative order of unaffected survivors. It does not sort the entire base by logical ID. Deletes can shift physical page boundaries; appended updates have no semantic assignment.

`two_bit_compaction` constructs order 0..rows for the merged source, builds SQ8 with the surviving signed IDs, then rebuilds generation metadata in that identity physical order. It calls neither source fitter. The correctness checks establish mutation identity, authentication, publication fencing and restart behavior. They do not establish retained ANN quality or bounded local rewrite cost under updates.

The flat and hierarchical source fitter measurement is therefore separate from compaction. A successful source fitter does not repair appended-update locality, license adding global refits to each compaction, or establish an incremental lifecycle cost win. Before a production claim, freeze an update workload (insert/update/delete proportions and batch sizes), measure returned recall and retrieval cost before/after compaction, and charge canonical reads, source/SQ8/metadata writes, scratch, pinned generations and GC requests. Existing same-host exclusive maintenance safety also remains the supported boundary.

The architecture decision must explicitly choose whether current full rewrite cost is acceptable at its declared mutation rate, or require a bounded local partition rewrite design. No new maintenance algorithm or default is introduced in this experiment.
