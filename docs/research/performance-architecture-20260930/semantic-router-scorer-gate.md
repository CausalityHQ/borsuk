# Semantic router: actual scorer development gate

This refines the executable adapter before any new scorer corpus execution.
The frozen nomination, panel, identities and limits in
`semantic-router-falsifier.md` remain authoritative. The coverage pass is a
ceiling, not returned recall or an end-to-end performance result.

## Shared scorer and routing geometry

Use `RotatedTwoBitCodec` with the authenticated historical mean and seed.
Prepare it from the original f32 request, as the existing generation does;
use `cosine_vector` only for graph/semantic discovery and final SQ8 ranking.
Reuse the generation's exact source unit maxima, page completion, score/ID
ties and `choose_budgeted_pages_sparse`; reuse
`returned_sq8::rank_returned_ranges` for final ranking. No cached SQ8 scores,
truth, or historical returned IDs may select source units or pages.

The production graph path requires 159 candidate pages per walk at this
100k geometry. Every frozen semantic query nominates fewer pages (maximum
154 ReLAION, 120 CoHere), so a literal call would reject otherwise valid
semantic geometry. The diagnostic adapter therefore takes an explicit page
limit: 159 for control, the actual nominated page-closure count for candidate.
The production path retains its original limit and rejection behavior. This
changes routing admission only; source scoring, completion, ordering and SQ8
physical selection are shared. Never pad with unrelated pages to reach 159.

For candidate, recompute the fixed root rule and retain every unit in the
selected leaves. Use one walk; its seed is the lowest nominated physical page.
Add that page's missing units only to satisfy the existing complete-seed-page
invariant. These units are within the already nominated page closure; charge
and report them separately. All other missing units on nominated pages are
scored by the unchanged completion mechanism. No page outside this closure
is nominated. Report original semantic-unit coverage separately from these
seed additions and page completion.

## Physical budget before scoring

Plan the whole nominated source-page closure using the existing native
`cover_pages` rule, with 200-byte source records, 256-row physical pages,
128 GETs, 64MiB and at most 16 concurrent reads. Charge bridge gaps and the
clipped last page. Make the score-record callback reject any row outside the
planned cover. Fully authenticating an offline object is validation only;
it does not turn full-object hydration into a bounded serving query.

After source nomination, enforce the unchanged 32-GET/16,773,120-byte SQ8
planner before reading or scoring SQ8 ranges. Combined source/SQ8 caps remain
160 GETs and 83,881,984 bytes. Root/selected-leaf reads and membership preload
are additional, with the existing router caps. Logical planned ranges are not
measured physical S3 requests; offline read/CPU timings are not cold HTTP.

## Paired execution and terminal decision

After exact-source affected tests, workspace Clippy and full workspace test
compilation pass, run ReLAION FIRST100k D768 cosine consumed ordinals 0–63
first. Rerun unchanged two-graph discovery contemporaneously with the same
original query, centroids, graph bodies, source records and final SQ8 object.
Authenticate and match the closed control's discovery, plan, ordered returned
IDs and scorer identities before treating the new control as equivalent.
Recorded walks may diagnose a mismatch but cannot replace fresh discovery.

Alternate control/candidate order by ordinal. Return top100 with the native
kernel and report R10/R100, per-query tails, semantic discovery/closure,
source-selection, fetched and returned losses. Truth enters only after plans
and returned IDs exist. Mean returned R10 must be at least 95%, all 64 calls
successful and all identity/budget gates satisfied. Preserve a failure and
name its stage; do not silently expand caps or repeat to obtain a pass.
Only ReLAION survival earns CoHere with the same adapter and fixed policy.
Only both returned-quality survivors earn object-native cold HTTP and offered
8-QPS/saturation/cost measurements. No vendor win or 1M/100M qualification
follows from this local gate.

Run the local scorer once per dataset in a fresh bounded service: 512MiB
memory, zero swap, two CPUs (0–1), 32 tasks and 900 seconds. Record exact
source/binary identities, command, terminal status, cgroup counters and RSS.
The 512MiB bound includes offline authenticated 100k reference objects and
is not a production query RSS claim. Do not overlap scorer jobs or Cargo.
