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
