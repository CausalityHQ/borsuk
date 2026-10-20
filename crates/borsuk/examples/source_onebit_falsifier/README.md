# Offline one-bit SOURCE falsifier

A diagnostic example only; no library, format, default or serving API changes.
Compare current two-bit SOURCE with one fixed one-bit estimator on identical
recorded nominations. This does not replay a planner or establish cold latency.

Build: cargo build --locked -p borsuk --release --example source_onebit_falsifier

CLI:
source_onebit_falsifier --self-check
source_onebit_falsifier --admit AUTHORITY AUTHORITY_SHA NEW_REPORT MEMORY_BYTES
source_onebit_falsifier AUTHORITY AUTHORITY_SHA NEW_REPORT MEMORY_BYTES

Root alone runs admission and measurements. Admission authenticates inputs and
encodes the nominated union; it emits no rankings. Run a separate synthetic CLI
smoke before the scientific run. NEW_REPORT is create-new; an existing report is
never overwritten. Exit 2 means INVALID; no success report is written before all
checks pass. Keep inputs immutable through process exit and use an external
<=5 minute, <=8 GiB, <=2 worker cgroup (this binary uses one worker). Preserve
failed attempts. OS caches are not a cold benchmark.

Authority: borsuk-source-onebit-falsifier-authority-v2, bounded to 8 MiB (8,388,608 bytes).
Report: borsuk-source-onebit-falsifier-v2. Old authority markers are rejected;
there is no compatibility mode. Mandatory candidate_shortlist_rows is an integer
in 1..4096, chosen globally by root before execution.
The finite cap admits wider cohorts, including 1,563,062-byte authorities;
authority bytes still cost 96x in the working model (at most 768 MiB).
The caller memory cap must admit that cost plus all other modeled allocations
before any request, truth, canonical or plane payload is read.
Add truth:{path,bytes,sha256,k}; use a NEW authenticated authority, never edit the
historical file. Queries require increasing ordinals, unique nominated
32-row units in their original order (unsorted accepted), including a partial last unit. Diagnostic truth k is bounded to 1..256 before payload. Rows/dimensions/budgets are generic.
The pinned generation manifest binds plane and canonical descriptors. Local
canonical.bin length and canonical_row_digests.bin length/SHA are checked;
every nominated canonical record (LE i64 ID + D LE f32) is authenticated by its
root-bound row digest before encoding. The entire canonical blob is not hashed.
Plane open uses the existing authenticated reader and score API. Requests and
truth are exact-length SHA-bound files. Truth is ordinal-major LE u64 logical
IDs; IDs must fit i64. No walks or baselines are synthesized.

If queries[].baseline_top256:[{row,score_bits}] is supplied by root authority,
the entire recomputed two-bit top256 order and f64 score bits must match or the
run is INVALID. Without this field, baseline reproduction is explicitly
unavailable: recomputed current-two-bit retention is still reported, but parity
with an historical baseline is NOT established.

Estimator: normalize x and q for cosine about the origin; zero-pad to M=next
power of two, apply the fixed SplitMix64 signed diagonal (seed 0x5241424954510001)
then normalized Walsh-Hadamard. Store signs (>0 is bit1, zero bit0), LSB-first,
and f64 factor norm(Tx)/L1(Tx). Score is sum(sign(Tx)*Tq)*factor; q remains full
precision. Scores are not clamped. This is an asymmetric estimator, not Hamming.
The transform is structured NON-HAAR HEURISTIC, with NO RaBitQ unbiasedness or
error-bound claim. No seed, centroid or transform tuning; the authority pins the candidate shortlist.

Derivation checked against RaBitQ v1 sections 3.1-3.2, the ratio
<quantized_x,q>/<quantized_x,x>, and the official origin-centered IP factors:
https://arxiv.org/html/2405.12497v1
https://github.com/VectorDB-NTU/RaBitQ-Library
https://raw.githubusercontent.com/VectorDB-NTU/RaBitQ-Library/main/include/rabitqlib/core/detail/rabitq_impl.hpp
The official factor uses centered binary coordinates +/-0.5; the two scale
factors cancel to the same signed-dot/L1 ratio on unit vectors. No upstream
code is copied and no new dependency is added.

Memory is admitted before payload: conservative full-population one-bit/ID/map/
sidecar/score-vector capacities plus authority*96, request/truth duplication,
padded transform scratch, three existing two-bit query scratch allowances,
96 MiB control allowance, queries*(2*min(rows,256)+min(rows,candidate_shortlist_rows))*4096 report allowance, and the authenticated plane reader's full
records/mean/sidecar working model. The nominated union is encoded once; its
code is ceil(M/8) bytes and factor eight bytes per row. Report logical code and
factor bytes separately from resident plane bytes and all measured capacities.
External cgroup bounds allocator/runtime and filesystem cache not in the
explicit model. Report process CPU and peak RSS plus encode/score wall times.
Ranking uses descending f64 total_cmp score, then ascending physical row.
The two-bit top256 and one-bit top256 controls remain fixed. One-bit scores
and sort are computed once; candidate_topk takes the first min(S,N) nominated
rows from that same order. onebit_score_sort_seconds charges this shared work
once, including sorting; truth calculations are outside score/sort timers.
Reports retain all three row/ID/score/score_bits lists, original nomination
order, nominees, truth and explicit planner flags. candidate_truth_retained
contains truth IDs in candidate_topk, in truth-file order; candidate_truth_gained
and candidate_truth_lost compare it with baseline_truth_retained. Existing
onebit_top256 and truth_retained/lost/gained fields retain their control meaning.
All three retained ranking lists are charged with checked arithmetic before
payload reads; the two full score-vector capacities are also charged.

Synthetic tests cover independent dense f64 Hadamard oracle across padded and
non-padded dimensions, same-sign/different-direction collision, full precision
query sensitivity, zero/nonfinite/extreme coordinates, bad widths/tail bits,
overflow, memory model, tie order, baseline mismatch and file/output guards.
No real data was opened by the implementation worker. Quality, runtime and
working-set feasibility remain UNVERIFIED until root runs actual admission
and the preregistered paired falsifier. Existing negatives/Pareto results
are unaffected.

Upstream physical-row invariant (not independently proved by this wrapper):
canonical_source.rs:57-169 iterates physical SQ8 records and reads
order[physical] (or ordinal ID), then writes that SQ8 ID and cosine-normalized
raw row. two_bit_source.rs::build_inner:209-280 uses the same physical SQ8 order
and raw-row mapping for plane records. two_bit_build.rs:2434-2475 is the later
canonical-generation preparation: it checks each canonical/SQ8 ID pair and
both full payload hashes before copying canonical records and sealing the new
row-digest binding; it does not rebuild the plane.
The generation binds both artifacts under one authenticated root. Root must
verify the producer/preparation receipt (fp32-serving-real-prep-a0001) before
real quality execution. This standalone reader authenticates selected bytes;
it does not independently reconstruct their correspondence.

Additional synthetic checks cover S256 prefix/score-bit equality, S512 extra
truth, N<S and tailed rows/dimensions, malformed/missing S in both modes, old
schema rejection, report overflow and model-minus-one rejection before payload.

Opt-in fixed-center diagnostic (v3 authority/report)
--------------------------------------------------
The unchanged CLI accepts authority schema borsuk-source-onebit-falsifier-authority-v3
only with centroid_mode:"fixed" and center:{path,bytes,sha256}. The absolute regular
file is exactly D little-endian f64 values (D*8 bytes), authenticated before score
or request/canonical/plane payload. Values and transformed values must be finite.
V2 is explicitly origin-only; center injection is rejected. Every v3 input x/q
may be any finite nonzero f32 vector; normalization in f64
matches the immutable origin control. The fixed center need not have unit norm.

For x,q normalized about the origin, r=x-c, z=sign(T r), and
alpha=||r||^2/L1(T r), estimate q.c+r.c+alpha*z.T(q-c).
By linearity of T this equals q.c+[r.c-alpha*z.Tc]+alpha*z.Tq.
The bracketed additive factor depends on the row and cannot be discarded.
For r=0, alpha and additive are zero, so the score is q.c.
The same signed Hadamard seed remains NON-HAAR; no theory/speed/IO claim follows.
The official centroid-dependent IP factors are described at
https://vectordb-ntu.github.io/RaBitQ-Library/rabitq/estimator/
and https://arxiv.org/html/2405.12497v1 .

All existing origin candidate/control lists retain their arithmetic and names.
V3 adds centered_topk and centered_truth_retained/lost/gained (relative to the
two-bit baseline), plus separate code/factor/additive bytes and scoring time.
It charges an extra packed code and two f64 factors per full-population row,
one full Hit vector, center file+decoded center (16D), transformed center (8M),
and queries*min(rows,S)*4096 report allowance before payload. Ranking capacities
are reserved before payload. V3 still emits the original fixed 256 controls.
All center dimensions, paths, lengths and SHA pins are root authority.

This example does not prepare or infer a center from nominees or queries.
Root owns full-corpus center preparation, its provenance receipt, and execution.
No native --prepare-center command is included in this example; the root must
provide a separately verified corpus preparation seam. Synthetic qualification
alone leaves real quality, runtime and memory feasibility UNVERIFIED.

Experimental portable centered byte LUT
--------------------------------------
A newly authenticated v3 authority may set experimental_scorer to
"centered-byte-lut-v1". Omit the field for scalar-only controls; unknown, null,
or non-v3 selectors are rejected before payload. Root/center/query/body
authentication remains mandatory. One 256-entry f64 signed-sum table per
packed byte is prepared per query and reused for all nominated rows, followed
by the unchanged qc + additive + dot * alpha finalizer. Byte grouping changes
floating-point association: score bits, ties and rankings may change.
There is no bit-exact, Haar-theory, cold-latency or physical-IO claim.
lut_topk and lut_truth_retained/lost/gained are separate from scalar controls.
lut_table_prepare_seconds and lut_score_sort_seconds charge preparation and
scoring/sorting separately. The checked pre-payload model charges actual LUT
capacity (rejecting unexpected allocator capacity), a full extra Hit vector,
and queries*min(rows,S)*4096 retained report bytes.
The ignored fixed_work_scalar_lut primitive is root-owned: same frozen query,
codes and factors, 256 distinct 32-row units, counts 1/7/32, eight alternating
repetitions, nonlinear consumed fingerprints; no adaptive timing threshold.
The implementation worker compiles this primitive but does not run it.
