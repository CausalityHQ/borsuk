# ARM SHA startup intervention preregistration, 2026-09-29

One causal change: sha2 0.10.9 asm feature enabled only on aarch64, activating
runtime-detected SHA instructions with software fallback. Cargo.lock adds
sha2-asm0.6.4; fixed SHA256 abc/million-a known answers augment existing source
authentication/tamper checks. No serving layout, scorer, metadata transfer,
query panel, identity check or memory admission change.

Control: independently verified startup-profile/a0001, source217a33df/archive
4a8437ff664adbbd4cf8fb6e09382ac7dc923f886615a8a0be6e7990ab6cf6c0, reuse its authenticated
ARM executable and compiled-source/boundary receipts. Candidate native source
must equal that control for the original four compiled files; only the declared
Cargo feature/lock and new source test differ. Toolchain/environment differences
must be recorded; unknown equality is not claimed. No source patch during build.

Same c7g.2xlarge Spot host/region/roots and worker. Four ordered blocks ABBA,
control0,candidate1,candidate2,control3. Each uses the original six interleaved
[ReLAION,CoHere]*3 starts:24starts, six per arm per dataset. FIRST1M D768 cosine
metadata only, no query split or ANN queries. No application cache, S3 cache
uncontrolled; paired host and ordering reduce time drift but do not control S3
cache. Report all samples, median per arm/dataset and signed deltas. Do not turn
these exploratory small samples into population percentile confidence claims.

Build qualification: locked release, existing staging/generation/HTTP tests
plus source KAT/tamper tests; ARM feature-tree selects asm, x86 tree does not;
CPU SHA capability recorded. Build10GiB swap0 four CPUs2400s; profiling8GiB swap0
four CPUs4GiB AS600s; total machine hard shutdown3600s;80GB encrypted gp3 deleted.
Spot max$0.30/hour plus$0.15 EBS/S3 allowance, estimates not billed cost. On
interruption discard cell/campaign, preserve terminal evidence, terminate owned
compute; no automatic replacement or second controller. Observe terminal/health
only until closed; independent verifier authenticates every closed artifact,
archive, compiled sources, code/config, original control, resources and shutdown.

Diagnostic gate: both datasets show median authenticated load/decode and median
namespace readiness lower than same-host control; all identity/resource/cleanup
checks pass. Otherwise preserve no-improvement or infrastructure FAIL and locate
the remaining phase. No recall, first-query, throughput or vendor gate awarded
from startup-only evidence. Next after survival: fixed quality parity and true
cold first incoming-query end-to-end protocol. No retroactive change to old gates.
