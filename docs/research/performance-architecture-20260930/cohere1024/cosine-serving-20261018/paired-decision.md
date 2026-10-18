# Cosine serving: full paired confirmation

Exact qualified e529 source (main c1 docs descendant) was reused without a Rust rebuild.
Fresh bounded Causality Spot; retained EBS; unchanged source/assets and actual Full1000 admission.
Fresh real-S3 q0 canary passed SDK/native reader/exact bits/scratch cleanup.
First32 ABBA was valid but mixed latency (wall ratios1.301/0.732).
Separately frozen Full1000 ABBA passed all native runs, strict readers, exact rank/scorebit/plan/charge checks and per-cell S3 checkpoints.

Cohere1M D1024 cosine k10, reused development1000 query cohort:

| Cell | Hits/10000 | Query wall s | p95 ms | Serial reciprocal QPS | CPU s |
|---|---:|---:|---:|---:|---:|
| A1 | 9737 | 136.857 | 226.269 | 7.307 | 99.342 |
| B1 | 9746 | 133.089 | 222.658 | 7.514 | 98.709 |
| B2 | 9746 | 130.980 | 191.000 | 7.635 | 98.585 |
| A2 | 9737 | 181.692 | 501.362 | 5.504 | 98.388 |

Retain the consistent-cosine functional and quality increment:97.37% ->97.46%,19 gained/10 lost hits,zero underfill,exact offline/native parity.
Both arms have logical SOURCE97797 GET/24255289344B and SQ830014 GET/9974177024B per1000; not billed/wire counters.
Candidate CPU ratios0.9936/1.0020 show no material added CPU here.
Wall ratios0.9725/0.7209 and p95 ratios0.9840/0.3810 are observations, not a robust causal speed claim: final control p99=1261.980ms versus candidate343.326ms with nearly unchanged CPU.
Source cache off/fresh processes; backend and OS cache unknown. No blind holdout, fully-cold or matched vendor claim.
All cells passed frozen validity checks; efficiency uncertainty does not make valid measurements invalid. Preserve individual observations and quality/Pareto gain.

Existing qualification:181 scoped tests passed/2ignored,release BIN+example,workspace Clippy and real shim-unset162-target test compilation. No unchanged Rust gates repeated.
The28 earlier broad diagnostic failures/exact-parent full-suite baseline remain documented and unverified; one reproduced fixture lacks source_layout before scoring. Production readiness remains open.

Next: select one causal reduction in SOURCE/planning/SQ8 work from measured profiles and run its cheapest matched recall falsifier. Resolve current-format fixture failures before production freeze; matched S3 Vectors/Turbopuffer lifecycle/quality/latency trials remain open.
