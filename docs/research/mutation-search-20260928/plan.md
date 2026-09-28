# Root-bound incremental mutation search

Base23f3dcb0. Add a single library search method taking an opaque authenticated
TwoBitMutationSnapshot. Require exact base-root/dimension binding and charge its
retained payload in already_pinned_bytes. Keep the generation query semaphore
held through planning, conditional reads, masked base scoring and delta merge.

Reuse common unadmitted search core beneath the existing admitted wrappers.
Snapshot excludes all replaced/deleted base IDs before top-k. Score every bounded
pending put in normalized cosine squared-L2 units and merge with the masked SQ8
base shortlist in a bounded worst-first std BinaryHeap, then sort score/ID. New
hit type has logical ID/score only: pending rows have no base physical ordinal.
Base SQ8 is approximate; pending normalized FP32 scoring does not certify global
exact cosine ranking or ANN recall. Up to k visible rows; k can exceed base N
when inserts exist. No compensating GETs or complete base scan/hydration.

Admit extra heap/output scratch across max_active_queries against the loader's
existing full payload model, not just per-query bytes. Bound capacity by fetched
row maximum plus pending put count. Preserve base I/O failure/charge stats and
report delta rows/puts scored and immutable mutation revision/digest separately.
Query accounting excludes earlier snapshot recovery, which remains lifecycle I/O.

Use existing real local HTTP fixture and same authenticated generation: mutate,
recover, search, update/delete again, recover/search and pinned old snapshot;
assert IDs/order, exact GET/byte counts, no extra fetch, root/admission failures
before GET and existing wrong-ETag/corruption failure checks. Missing-method red,
focused original Spark green plus publication/ID regression. No cloud/full suite,
new consultation, parameter sweep or quality/vendor claim. Next compaction/GC.
