# Startup admission review of 65183e22

Candidate `65183e2223fff58a5bbfb53a6de452e459ed7429` remains unverified.
No compiler, native correctness, recall or performance result is implied.

## Required guard-order repair

`OverlapIndex::open` calls `Prototype::open_for_source_probes` before calculating
and checking its aggregate overlap startup peak. The router therefore preloads
while the parsed overlap root is already live, before aggregate admission.

`paired_overlap` opens both indexes before calculating their combined resident,
preload, evaluator and query coexistence budget. Individually admitted indexes
can allocate their combined startup payload before the pair rejects it.

Authenticate bounded metadata and compute the aggregate allocation requirements
before heavy router/directory/cell bodies are opened or loaded. For the pair,
admit both startup peaks and resident/evaluator/query coexistence before opening
either index. Bind metadata to the same authenticated roots and revalidate it
when materializing the indexes. Account for metadata parsing itself.

Preserve the original candidate and make a separate source repair. Include a
falsifier where each index fits individually but the pair does not: rejection
must precede heavy body reads. Also cover insufficient single-index admission,
metadata tampering and missing bodies. Keep scientific routing, replica quotas,
scoring and quality gates unchanged.

This is source inspection, not a measured RSS violation. Whole-unit remote
cgroup verification remains necessary after compilation.
