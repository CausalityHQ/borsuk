# Semantic router: bounded development falsifier

Protocol frozen before any corpus fitting/query examination. Construction is
owned by the one-file builder plan. This document authorizes no cloud launch;
actual builder assurance, authenticated runtime implementation and controller
qualification remain prerequisites. No production architecture is selected.

## Question and intervention

Does query-blind semantic grouping of exact, unnormalized FP16 unit means,
with whole-leaf admission and bounded query-boundary expansion, preserve useful
returned recall while avoiding full centroid hydration? This differs from
V149 consecutive physical groups and V150 nearest-unit truncation. It can still
fail the closed coarse-summary discovery mechanism; the test must expose that
failure instead of hiding it behind a new file format.

Use fixed 64-unit maximum leaves and the builder's deterministic squared-
Euclidean fit. Root prototypes are unweighted finite f32 leaf means. Rank roots
by squared Euclidean query distance using f64 arithmetic over the original f32
query and prototypes, ties by leaf ID. Admit the first eight leaves, or all if
fewer. Admit subsequent leaves in that order only while their distance is at
most 1.15 times the eighth distance, stopping at sixteen total leaves. A zero
eighth distance admits only zero-distance additions. This is a nomination
heuristic, not a certified nearest-neighbor bound. No learned query thresholds,
GT-driven expansion, replication, normalization or parameter sweep.

Retain ALL units in each admitted leaf. The initial geometry falsifier measures
exact truth-row coverage of their original 32-row units and of the physical
256-row page closure. Do not truncate to nearest 256 units: that would reintroduce
the V150 discovery loss. Subsequent scoring must use the unchanged two-bit/SQ8
source scorer, logical-ID handling and budget selection, with discovery,
page-closure, local selection and quantization losses reported separately.

## Inputs and fixed panel

FIRST100k D768 cosine, ReLAION first then CoHere, consumed development query
ordinals 0–63 from source-completion-http-config.json. Use its immutable
requests/truth/order and generation identities. Root/centroid authorities derive
from closed source-completion-http/a0001 terminal
bb027f633bce5293ba09b7568506b6f7d53cdb8a85480fcd98669644a8d7bbcd.
ReLAION root 577a9c874005b6b29c65f62ce2f525ee2634662ff2b4097ff267759766351d3b;
CoHere root 9430551e73a3ca09aed910a8d6502fafa0f5ec836a53c2fab6ab88a8547b4521.
Each centroid body is 4,800,032 bytes; authenticate body SHA from that terminal.
Truth is an evaluator input only, never fitting or nomination input. Check the
query/truth ordinal mapping explicitly. No generalization claim on this panel.

## Admission and decision

Construction: <=100k rows/D768/3125 units, <=8MiB centroid input, <=128MiB
modeled payload allocations. Any real fit additionally needs parent-enforced
memory/swap/CPU/wall limits and recorded RSS; modeled admission is not RSS.

Router: root <=1MiB; <=16 leaf range GETs, <=2MiB verified leaf payload,
no full-router hydration in the candidate query path. Charge membership metadata
and any preload explicitly. Source reads keep <=128 GETs, <=64MiB, <=16 parallel;
SQ8 keeps <=32 GETs and <=16,773,120 bytes; combined <=160 GETs and
<=83,881,984 bytes. These are the current paged-source engineering envelope,
not a claim of meeting the older 32-GET/16MiB stretch envelope. Router I/O is
additional and must be reported in total physical requests/bytes.

First cheap rejection: mean truth@10 coverage of admitted physical-page closure
below 95% kills this nomination policy on the panel. A pass is only a recall
ceiling. Report unit coverage and recall@100 separately. A survivor earns actual
returned mean R10>=95%, all calls successful, exact source/scorer identity and
bounded bytes/GETs; report R100 and per-query/tail quality without converting the
old 98% R100 stretch diagnostic into a universal product KILL. Rerun unchanged
control contemporaneously. No closed historical latency is a matched control.

Only returned-quality survival earns process-cold and namespace-cold HTTP,
8-QPS offered/saturation/cost, unopened 1M queries and larger-scale gates.
A failure terminates this arm and names discovery, closure, selection or scoring
as the bottleneck. Do not rerun merely to find favorable noise or silently expand
caps. No 100M or incremental-maintenance qualification follows from this test.

## Query-space binding before evaluation

Independent review identified ambiguity in “original f32 query.” No corpus query
has been evaluated for this arm. Use the existing `sq8_source::cosine_vector`
semantics: cast coordinates to f32, sum their squares in f64 in coordinate order,
reject a nonfinite or nonpositive sum; retain coordinates if |sum−1|<=1e-6,
otherwise divide each by the f64 square root and cast back to f32. Widen these
coordinates and the manifest prototypes (also rounded to f32 after JSON parsing)
to f64 for root-distance arithmetic. Never normalize the unit means or the
leaf prototypes. Record this query-space rule with the result.

Report distinct original physical pages per leaf and selected page-union counts.
A high closure coverage obtained by fetching most of the corpus is not a serving
success. Root/leaf and later source/SQ8 limits still apply. The 100k JSON root
format is intentionally bounded to this falsifier; 1M would require a separate
scalable root layout and measured gate, not extrapolation from this artifact.
