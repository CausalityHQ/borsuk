# Cold SOURCE score overlap: full1000 proposal, 2026-10-19

Propose integrating the exact qualified generic overlap implementation from
cd8cac6bbda4066f7277114ac3b9706c75129785 onto
708af9b470ec3af3fe997cf4f3f03876839fe909. This worker returns a proposal;
the parent retains integration, scientific decisions and push authority.

The closed corrected attempt cold-source-overlap-full1000-a0002 supports matched
quality latency on this cold application-cache fixture: 1M vectors, D1024, k=10,
1000 frozen queries, two repetitions per arm, order A1/B1/B2/A2. Each cell returns
9902/10000 recall hits. The root independently verified all 4000 returned ID,
f64 score-bit, ordinal, plan, trace, shortlist and charge records against the
frozen reference. This worker inspected closed JSON metadata only.

| Metric | B/A |
| --- | ---: |
| Pooled query wall | 0.9398647543352614 |
| Query process CPU | 0.987991007205771 |
| Paired p95 B1/A1 | 0.9441846533984232 |
| Paired p95 B2/A2 | 0.9358104724240023 |
| Pooled p90 | 0.9444544079299695 |

Both preregistered paired p95 ratios are <=0.95; every closed report gate passes.
The CPU metric above is query process CPU; whole-process and external child CPU
ratios are separately recomputed in verification.json. Each measured cell charges
169024 GETs and 51077441536 verified bytes. The root verified physical GET attempts
equal logical charges, with zero retry GETs and transport/stream failures. These
are measured request/verified-byte quantities, not vendor billed bytes or cost.

Serving configuration remains cache0/Q1/semantic64/Offload2/FP32 shortlist16.
The change scores original mandatory flat SOURCE units on the existing two auth
workers after each complete response authenticates while later reads are pending.
Replay preserves mandatory-unit order and exact f64 bits. SQ8 begins only after
all SOURCE reads succeed; ordered errors, full drain, leases and cancellation
remain covered by the reused focused native qualification. The library algorithm
is mathematically unchanged and contains no new dataset specialization.

Exactly seven crates paths are copied without editing Rust. The complete crates
tree is ab73789e2b2ee642f893123d8d2e96a90c26e77c, identical to the qualified
candidate; Cargo.toml/Cargo.lock/toolchain entries also match. The qualified
runner is 67055f6d7d1f4911f7a024768d6c87d2ee93fa59e629173e097c4566ce6c9d0b;
baseline reader is 11f70589d210a102d4808358fc416bfd2c2cb80b2f993b56243f6504e7b85dfb;
candidate reader is fa16dccadc44f974a019f8613122fc5aa88f6f2ac2d5b469d4200fe9f922073e.
Qualification reuses closed affected/native, workspace Clippy, real shim-unset
workspace test-build and release evidence. No compiler or native reruns occurred.
Broad historical fixture failures remain UNCLASSIFIED and their exact-parent
baseline UNVERIFIED; this is not a full-suite pass.

Simulated storage-engineer objection: overlapping work could hide extra memory,
weaken authentication or change billed transport. Source inspection confirms the
callback follows complete-response/page authentication, retains existing workers
and leases, and reserves score storage through the shared memory model. Closed
startup models are 469203215 bytes for A and 471434981 for B (2231766 extra).
Physical request parity is root-verified; wire/unread/billed-byte attribution and
provider cache eviction are unproven. Application SOURCE cache is zero, each cell
uses a fresh process, and none of this establishes provider-level cold storage.

Simulated evaluator objection: a second attempt or repeated fixture could erase a
failure or imply generalization. The prior full1000-a0001 remains execution INVALID
because the baseline was assigned the wrong reader; a0002 changes reader binding,
not the algorithm. The earlier 68-query screen's both-p95 FAIL is immutable
(ratios 0.880421 and 1.004069 at its recorded precision). Full1000 empirical
percentiles describe this fixed query set and two repeats, without a population,
vendor win, distribution-generalization or production-readiness claim. These
simulated objections are reasoning checks, not expert testimony or extra review.

report.json and protocol.json preserve the closed report and measurement addendum
byte for byte. verification.json embeds the independently confirmed parent receipt
(SHA256 52ac5cca2a1013f2f83835d7e943d5ce1f3d730867975743171ef21477fa7df3)
and records metadata hashes, exact source equality and check scope.
qualification.json retains the original gate and unclassified-failure receipts.

The corrected packet at 58e0b92e11558a25a591be0a435fdf3f617db704, its original f917
packet and INVALID receipts are preserved; the parent integrates that packet
separately. This proposal owns only seven copied crates files and these five docs.
Unowned trees and missing historical objects remain intact. No payload reads,
new resources, experiments, compiler/native runs, S3 operations, children,
consultations, push or merge were performed. Hold for the parent's disposition.

## Root acceptance

Root independently accepted the exact qualified crates tree and closed full1000 result. Integrate this generic cold SOURCE overlap slice; preserve the earlier screen failure and initial reader-binding INVALID attempt. Public scoring mathematics, layout and dataset-independent interfaces remain unchanged. This acceptance does not freeze all production defaults or establish full-suite correctness, lifecycle readiness or a vendor win.

Next falsifier: compare the existing narrower routing breadth with this 64-leaf high-quality incumbent under the same cold overlap implementation. Preregister the recall/latency Pareto gate and verify the closed narrower reference first; retain the 99.02% point. No new experiment is authorized by this evidence file itself.
