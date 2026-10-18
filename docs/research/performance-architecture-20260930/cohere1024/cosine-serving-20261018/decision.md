# Native cosine serving increment
Qualified source e5293d37576a8e1eee92f487bb5b80a543363952 changes immutable
SQ8 and pending FP32 puts to consistent norm-correct cosine distance.
Shared ordinary/direct/ObjectNative serving uses the existing cosine kernel,
with traced top-k, unchanged authentication, exclusion validation, ordered drain,
split regrouping, generation pins and allocation structure. Empty-base mutations
use the same metric. Low-level L2 APIs remain unchanged. Encoding is unchanged.

181 scoped native tests passed, two existing tests ignored. Release native runner
and replay example, workspace correctness+suspicious Clippy, and actual shim-unset
workspace compilation (162 executable targets) passed. Both full reviews were
reconciled. Separate L2/cosine references preserve the bridge counterexample;
split tests now use decoded norms, varied scores and an independent oracle;
the tail test checks actual full-return serving while keeping exact top10 checks.
The overbroad interrupted attempt and all failed test/build attempts are preserved.
28 historical diagnostic failures were observed before interruption; a representative
fails on a missing source_layout fixture field before scoring. Exact-parent
full-suite execution remains unverified: this is not an all-tests-passed claim.

Actual live-S3 native Full1000 input admission and independent native result reader
passed on exact frozen Cohere1M/D1024/cosine/k10 development inputs. Serving produced
9746/10000 hits (97.46%) versus retained L2-control9737/10000 (97.37%).
Every returned ID/physical ordinal/scorebit matches the frozen offline candidate,
confirming19 gains/10 losses with no underfill. Every plan and logical charge
matches the retained input: SOURCE97797GET/24255289344B, SQ830014GET/9974177024B.
Physical billed requests/wire bytes are not established by these logical counters.

The admission timing is unpaired, not a causal speed comparison: total query
wall158.000s, serial reciprocalQPS6.329, observed p95=323.058ms and native observed
process peak103309312B. Preserve these observations; do not hide them or reject
the quality gain solely on this unpaired efficiency result. No matched latency,
concurrent capacity, fully-cold or S3 Vectors/Turbopuffer win is claimed.
The first32 A/B/B/A screen was not started: an overly strict source-clean check
rejected only untracked evidence, followed by a pre-dispatch glue syntax error.
The fixed measurement start window passed. No algorithm-negative disposition.

Retain this pre-release functional+quality increment. Production default freeze,
separate staging/canary and paired runtime confirmation remain open.
Next is the unchanged preregistered first32 A/B/B/A in a fresh bounded Spot window,
reusing qualified binaries and authenticated assets without rebuilding.
Requantization can change post-compaction scores: its reopened representation
is checked against its own independent scalar cosine oracle, not false bit parity.
