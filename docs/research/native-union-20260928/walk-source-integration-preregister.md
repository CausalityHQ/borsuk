# Actual planner integration of the surviving source roster

One shared query change, following verified ReLAION development GO28cf7669.
No source fit, layout, scorer or graph traversal change. Actual plan_inner now
uses existing evaluated unit lists to nominate primary+min(158,pagecount-1)
pages per distinct graph using partial two-bit source maxima. Deduplicate unit
scoring across both graphs and reuse global page maxima for physical admission.
Seed128/walk1272/union318/source-code rows<=min(rows,81408)/32GET/16773120B.
One graph when both graph identities match; one-page case scores primary units
without requesting zero additional graph pages. Clip final units at row count.
No second full-page source pass. No corpus-sized per-query arrays or maps.

Sorted-vector unit memo<=2544 entries; pergraph maxima<=1272 entries; global
maxima<=2544 entries. Vectors have explicit bounded capacity or exact-size
collect. At source ranking peak, unit/score/global/local/selected/ranked payload
plus two retained walks is below256KiB; graph walk temporaries are released
before source ranking. The existing perquery1MiB planner allowance covers this
phase and the previous bounded graph/traces phase. Codec scratch remains separate.
Runtime allocator overhead and actual RSS need cold serving measurement; this
is a payload bound, not100M RSS or generation pin qualification. Shared planner
slots and total query admission remain unchanged. Persistent metadata unchanged.

AWS intended assertion RED on new source-cutoff/duplicate regression, then exact
frozen function/planner transform; GREEN source tests and existing generation
integration target, covering actual ordinary/diagnostic parity and admission.
New second regression covers one graph,257 partial rows,one row,malformed walks
before scoring and nonfinite callback rejection. One full workspace all-targets
assurance required because actual core changes; release new plan_demo and HTTP.
Expected previous2689passes plus2new core regressions plus2already-qualified
research-bin tests=2693actual top-level passes/0fail/26ignored;146Cargo targets
including one benchmark with12smokes. Independently match old target roster
plus one new binary target, not raw interleaved nested stdout summary counts.
No corpus queries in engineering; no repeated full assurance in later science.

One causality c7g2xSpot eu1c,4200s worker/3900s check/fourjobsCPU0–3/10GiB
zeroSwap/80GiB encrypted disposable EBS; quote<=.30/hour,compute cap$.35 plus
$.10EBS/S3. Frozen source/unique reservation/client token<=64/shared lock/active
worker guards. Required final artifacts regardless reported success; terminal
sync and immediate actual termination. No automatic replacement. Whileactive
observe original session and terminal/infrastructure only, never incomplete logs.
Independent exactsource/RED/GREEN/full target roster/binary/cgroup/termination.

After authority: actual planner must reproduce authenticated ReLAION replay
ranges/nomination IDs on the exact old source-v3 root; unchanged retained old
binary supplies matched control. Qualify CoHere dev0–63 with source-v3 recipe
and unchanged control6347 versus exhaustive6351, fixed gates mean98/p0595/
exhaustivegap<=32/nonregression/physical caps. Only if quality passes, measure
actual object-native incoming cold HTTP with retained original control.
No fresh, scale, vendor or maintenance claim from development or assurance.
