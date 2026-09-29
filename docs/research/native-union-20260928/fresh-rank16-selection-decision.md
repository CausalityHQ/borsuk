# ReLAION rank16 provisional panel: identity construction PASS, lineage pending

2026-09-29. Preregistration and selector source were frozen and fast forward
pushed at `e1f13539` **before** selecting any IDs. The ID-only selector then
produced `fresh-rank16-provisional-ids.json`, SHA256
`a8bd97d6e6468e715c4b26d9df8c400f649e5428b2a85f151307f20cb030230c`.
It contains exactly1,000 unique IDs, ordinals0–999, from registered source
objects rank16–31. Ordinals0–63 are reserved for development;64–999 remain
prospective. No query embedding, GT, ANN result, old rejected seal, or quality
body was opened.

All32 source objects were full-SHA-verified against the pinned registry again
during selection. The selector reproduced3,579,759 original unique physical
IDs,3,578,530 candidate unique physical IDs, and5,724 shared IDs. It excluded
the entire original physical ID universe before computing blind hash ranks.
An independent ID-column pass confirmed zero selected IDs in original rank0–15
and all1,000 rank/row-offset locators against the candidate objects. Selector
digest recomputation, ordinal order, unique IDs, and source rank bounds passed.
Raw source scratch occupies about11GB locally; no cloud instance or paid job
was started for this selection.

**Decision:** construction PASS; fresh qualification HOLD. The authenticated
source-code scan found no direct raw ReLAION shard import outside the V36
builder/tests across35 original archives and149 exact Git source trees, but
indirect runtime query inputs and the later nonversioned history still need a
selected-ID lineage audit. `prior_query_audit_pass` remains false in the panel.
If an old query reused any selected ID, or lineage cannot be established, reject
the entire1,000-ID panel before vector/GT or quality access. Never drop a
colliding row and reuse the remainder.

Next: close the selected-ID historical source/input audit, then AWS Spot exact
f64 GT10/GT100 construction and sealed query bodies with the unchanged V36
first1M source. Only a separately frozen native HTTP gate may assess mean
recall@10 >=95%, report recall@100, cold p90 and offered8 QPS. No vendor win,
fresh recall, or cold latency is measured by this decision.
