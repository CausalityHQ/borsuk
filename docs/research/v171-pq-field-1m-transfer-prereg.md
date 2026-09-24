# V171 direct PQ field: paired 1M transfer gate

## Decision and baseline

Test whether the V170 direct PQ64 score field transfers from the reused
ReLAION-100k D768 development-1000 screen to the **already used**
ReLAION-1M D768 validation-1000 cohort. Freeze the same score, candidate
units, neighbor-rank ablation, mandatory-primary priority and 32-row
interval optimizer. The benchmark's per-query caps come from the closed
V154/V155 cached sparse baseline; they are experimental matching limits,
not dataset-specific production defaults. Production recall and resource
targets remain caller inputs under the V169 design.

The strongest measured 1M operating point is V155 cached sparse:
99,567/100,000 exact-source GT100 hits (99.567%), p05 98,
11,134,007,040 planned bytes and 22,126 planned GETs over 1,000 queries.
The same-layout V164 context is 99,553 exact-source hits, 99,199 SQ8-only
hits, 14,568,253,440 bytes and 16,776 GETs. All historical results stay
tied to their own archived revisions; the new cell must replay the V155
baseline IDs and resources on the identical cohort before comparison.

## Frozen inputs and row spaces

Use V116 validation requests
`c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9`
and frozen SQ8 primary rosters
`3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960`.
The old V63 physical order is
`32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b`;
the V164 new physical order is
`5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f`.
The complete V164 terminal is
`daa4025093ddef883358a200751972b9d953cd53be80681b9055677c3c7793c7`
and binds the relaid 780,000,000-byte SQ8 object
`aecf0f2704f44906f411a74ab81b36e5e05f81bab35f4c70558e88acbc4d05c9`.
The V115 source router manifest is
`d558a77443d6a1a50b9b3d01e821f134b1cc0992aa8bcb7ef3dc9ed2941221fe`;
it binds the old physical PQ64 books/codes to V36 source and V63/V70
generation identities. Authenticate both permutations, their common source
identity and the closed V164 layout seal before planning. After the
feasibility phase, directly check stable IDs against the relaid SQ8 object
and source before returned scoring. No row ordinal may be silently
reinterpreted between the old router and new SQ8 object.

The used validation truth is
`bf0fb0c934c986d05282e3d1c63dc351c553976ea05bfcab0cd3f06d2979e871`.
The V155 complete terminal is
`784097f577f11bd49468473e43b1ba06642bf107ecde06a8b0b0ce09b0cd9cdb`;
its replay is
`a4f9e39e674ce22fc65ee82836731a22e2c094f7267439888b45f9320c3cb78f`.
The V154 GT-blind plan artifact
`ab9bac04c32445a85928dd8978468b1c40d937da2f89960e71d8d21d132ea4d1`
provides each query's V155 sparse GET and byte ceiling. Verify those
charges against the closed V155 replay after the plan is sealed.

## GT-blind feasibility first

Run a separate lightweight primary-only feasibility cell before the scored
cell. For mandatory unit positions, the exact minimum charged units under
a GET cap is the count of unique primary units plus the smallest physical
gaps that must be bridged to reduce disconnected primary runs to that cap.
Authenticate the V154 per-query resource limits and report all 1,000
minimum charges without downloading the PQ code plane, SQ8 object, source
vectors or truth. An infeasible result ends this **per-query matched
profile**, not the general architecture; a later cross-query allocation
rule would require its own preregistration.

For each query, map its old physical 512 nominees and 100 primaries through
the V63 old-order and V164 new-order permutations. Candidate units are the
nominee 32-row units and their immediate physical neighbors. Rank them by
minimum direct PQ64 ADC score, and independently by the V170 neighbor-rank
control. Force every primary unit. Plan each arm with the V154/V155 sparse
per-query byte and GET caps, as well as the global 16,777,216-byte and
32-GET caps. Authenticate and upload both GT-blind plans and their seal
before downloading source vectors or truth. If any query's mandatory
primary units cannot fit its paired caps, stop at this resource
feasibility result and do not download or open source/truth.

## Returned-quality gate

On feasible plans, score the fetched V164 SQ8 ranges and retain top 512
IDs. Union them with the 512 nominee stable IDs, then rerank source vectors
with V155's cosine normalization, float64 dot product and stable-ID tie
order to return top 100. Seal both arms' returned IDs before opening GT.
After source-only scoring, download the closed V155 replay, evidence and
truth; independently recount V155's exact-source and SQ8-only hits,
and the two new arms' exact-source/SQ8-only hits and physical coverage.
Report per-query and aggregate bytes/GETs, same-ceiling direct/control
paired differences, and equal-actual-resource points separately.

Classify **baseline-competitive** only if the direct arm reaches at least
99,567 exact-source GT100 hits, p05 at least 98, total planned bytes at
most 11,134,007,040 and GETs at most 22,126, with every primary covered.
Classify **transfer-pass but not baseline-competitive** if it reaches the
V164 preregistered transfer floor of 99,400 exact-source hits and p05 97,
but misses any competitiveness condition. Otherwise kill this frozen
feature/admission combination. A direct-versus-neighbor gain is reported
with 10,000 PCG64(171) paired bootstrap resamples, but does not substitute
for the V155 quality and resource gate. This cohort is already used;
passing it licenses a fresh real-query holdout and live-S3 qualification,
not a production default or a general recall guarantee.

## Execution and recovery

Run one immutable pushed-source archive on Causality Spot with a hard wall
cap. Upload terminal-listed evidence and hashes on success or failure;
stream and rehash only after the terminal marker. Discard an interrupted
measurement cell and start a new attempt ID. Terminate benchmark compute
immediately after the terminal. Do not run a full suite or 1M data build
on the swap-pressured devbox.
