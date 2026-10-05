# Prospective native graph-packing diagnostic

Status: preregistered method; implementation, compilation and execution pending. This adds precise rules to the historical proposal in `packing-plan.md`. It does not alter the closed fine-SQ8 experiment or its serving layout.

## Intervention and freeze

Use only authenticated original roots and graph bodies to pack each dataset's original 16-row groups. Preserve graph IDs, PQ codes, nomination, scoring and all original row bytes. This diagnostic computes a permutation and range cover; it does not rewrite SQ8 or measure recall.

Each stored directed edge in the last graph layer contributes one to the unordered group-pair weight. Reciprocal edges therefore contribute twice. Ignore upper-layer and intra-group edges; retain base-layer repair edges. Start a pack with the lowest unassigned full group. Add the unassigned full group with greatest summed affinity to all current pack members, ties by old group ID; zero affinity chooses the lowest remaining ID. Reset scores between packs. Capacity is 42 groups. Exclude a short tail group from all seed/candidate choices and append it once at the end, consuming one pack slot; start a new pack if the preceding pack is full. At N=673, the first pack holds 42 full groups and the one-row tail forms a second pack. Do not tune this rule using either panel.

Authenticate graph descriptors and their complete layout identities through the original roots. Bind both roots, graph descriptors/identities, geometry, algorithm rules, configuration and permutation hashes in a new seal. Validate both bijections and inverse round trips. Sync both permutations, seal and parent directory before opening the shared historical nomination prefix.

The historical prefix is 1,665,668 bytes, SHA256 `7abcf7830e10d214999b26fb499cebf925e16b8a88a931ddb9f981ad3d28d025`; its original seal SHA256 is `374cbd0e8700c85c2b4229468c3a9fa20283deb969b661386fb0078024b9de62`. The containing trace is longer. Securely open a regular descriptor, limit reads below buffering to exactly prefix length, authenticate those bytes and parse the same bytes. Never read the suffix or probe whole-file EOF. Do not open requests, truth, SQ8 records, canonical rows or PQ code bodies.

## Necessary locality gate

Require exactly 64 distinct ordinals for each of ReLAION and CoHere, FIRST100k D768, from the frozen prefix. Preserve every nominee and its group membership. Translate selected groups through the permutation and invoke the exact smallest-gap cover with explicit cap 32, not the old cap 256. Charge every bridged row at 780 bytes and clip only the final short group.

Every one of 128 plans must fit at most 32 ranges and 16 MiB. More than 32 touched packs alone is not grounds for rejection: the exact bridge cover decides. Evaluate all 128 covers before issuing a completed REJECT; report per-query ranges/bytes, per-panel fit counts and maxima. Any completed plan above 16 MiB is REJECT. Survival establishes only physical locality for these consumed panels, not recall, remote requests, latency, QPS or vendor parity.

A concrete cap fixture uses 500 groups spaced every three groups: cover cost is 12,330,240 bytes at 256 ranges but 17,921,280 bytes at 32. The latter must reject. Independently verify the arithmetic in the native test.

## Resource and execution authority

Proposed diagnostic ceiling: CPU1, memory 256 MiB, swap zero, 300 seconds; process datasets sequentially. Admit encoded graph, decoded capacities, validation scratch, both aggregation directions, permutations, scores, cover/prefix/output workspace and runtime reserve before their allocations. Bound operations and poll the deadline. A deterministic score scan avoids an unbounded stale-entry heap. If the admitted peak does not fit, report INVALID with required bytes; do not raise the cap or classify resource failure as locality rejection.

Success-looking output is insufficient. Accept survival or rejection only when the exact binary/config/report/permutation/seal bindings match the original supervisor's collected exit and closed resource/drain/cleanup receipt. Authentication, admission, deadline, OOM, interrupted execution or persistence failure is INVALID. Existing outputs must never be overwritten.

## Native qualification and next gate

Qualify the exact source externally before the real diagnostic: affected fine-SQ8 module including the new nested packing tests; graph module; complete affected binary; release binary; workspace Clippy denying correctness/suspicious; actual workspace test compilation with the build shim unset and compiler jobs one. Require every mandatory new test by exact log name. The old `fine_sq8_groups::tests` filter excludes the new nested diagnostic module and cannot establish this qualification.

If locality survives, a separate frozen implementation may stream-copy complete groups. Independently verify row-byte and ID parity and unchanged nominees, then run returned-recall gates. Current CoHere returned quality depends on incidental fetched rows: preserving nominees does not prove that repacking preserves recall. Require mean recall@100 at least 98% and p05 hits at least 95 on each panel before fresh-panel and physical-S3 tests. Product total requests must include metadata/recovery and attempts, not payload ranges alone.
