# V116 untouched ReLAION-1M validation preregistration

Status: frozen before reading validation query or GT values for this method.
The source corpus and SQ8 body are the same authenticated 1M artifacts used
by V114/V115. The V115 PQ64 router sections were built only from source and
layout; their manifest SHA-256 is
`d558a77443d6a1a50b9b3d01e821f134b1cc0992aa8bcb7ef3dc9ed2941221fe`.
Use the V36 **validation-query** and **validation-gt100** objects, with
SHA-256 `869e225181f7d01a972d8faa144eaff4838c7c1f8f7c0c55091487e234f0bd5e`
and `bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871`.
They have 1,000 queries and 100 exact neighbors each. Do not use query or GT
rows in training or parameter selection.

The candidate uses the frozen V115 source-only router: 1,024 regions,
512 PQ64 nominees, exact local SQ8 top-100 primary, weighted physical
interval planning, at most 32 GETs and 16,777,216 returned bytes, then the
Rust scalar SQ8 returned scorer. The paired control uses the same source
planes, the V109 ranked-page admission rule at the same GET/byte caps, and
**the same Rust returned scorer**. Generate both plans without GT, seal
their requests and replay output, then fetch GT for an independent reducer.
Check Rust/Python nominee-set parity before ranking the control's pages;
any mismatch fails the cell and is recorded. A changed scorer or a query
trained special case requires a new source-bound campaign.

Report both arms' Recall@100, p05 hits, sub-90 query count, maximum actual
planned GETs/bytes, and per-query paired wins/ties/losses. Qualify this
offline quality gate only if the candidate returns at least 99,000/100,000
hits, at least the paired control's total hits and p05 hits, p05 at least 90,
and no more sub-90 queries than control; any physical cap violation fails.
These thresholds continue the development gate; they are not selected from
validation results. Historical V75 measured 99.272% Recall@100 on this
split, but it is a different reader/configuration and is context, not a
matched control. A V116 pass does not establish live S3 latency, cost,
untouched cross-corpus quality or a production release.

Run one source-archive-bound Causality `c7i.12xlarge` Spot attempt with a
7,200-second science wall cap and no On-Demand fallback. Upload each phase,
terminal and resource log to an immutable attempt prefix; hash every input
and output. On interruption discard the incomplete cell and restart under
a new attempt. Before terminal, inspect only the terminal marker and
infrastructure. Terminate the instance immediately after terminal. If the
candidate fails, use per-query paired evidence to identify routing, planning
or scoring loss and change that layer generically before another quality run.
