# Read-only post-success stage analysis

Consultation 62e5b8323d35479c; codex/gpt-6.1-sol xhigh; completed exit 0. Source evidence ab67d540; no experiment or source change. Counterfactual timings below are estimates, not measurements.

**GO: reuse the authenticated generation root already fetched during authority-head validation.** Remove its second HEAD/GET during metadata staging; retain width8 and all existing validation.

At `ab67d540`, I authenticated all24 closed artifact bodies and checked the relevant native source against the qualified manifest. No files or experiments changed.

Each candidate beats **both** actual controls:

| Candidate | p90 reduction vs control0 / control3 | p95 reduction vs control0 / control3 |
|---|---:|---:|
| Cell1 | 23.16% / 7.19% | 27.99% / 6.88% |
| Cell2 | 24.78% / 9.15% | 29.15% / 8.38% |

Control p90 drift is **103.399ms**. Backend/cloud cache state is unknown; that variability prevents attributing the entire observed improvement to the deleted wave.

The following are same-row **mean milliseconds**, reduced from the closed records. Nested rows are included in their parent.

| Measured span | Control0 | Candidate1 | Candidate2 | Control3 |
|---|---:|---:|---:|---:|
| Whole cold response | 500.92 | 423.28 | 413.58 | 451.06 |
| Authority-head read, including lazy credentials | 80.78 | 81.12 | 79.11 | 80.05 |
| Metadata staging | 124.14 | 94.59 | 92.09 | 119.80 |
| ↳ Redundant root wave | 33.21 | **33.49** | **32.49** | 33.74 |
| ↳ Child waves combined | 90.90 | 61.08 | 59.58 | 86.04 |
| Source HEAD / router HEAD | 11.03 / 9.58 | 9.60 / 9.68 | 9.38 / 9.97 | 9.71 / 9.66 |
| Decode | 8.77 | 8.88 | 8.91 | 8.93 |
| Spawn/init/connect-poll residual | 11.11 | 11.21 | 11.40 | 11.42 |
| Router discovery | 44.12 | 43.25 | 40.24 | 42.77 |
| SOURCE fetch/authentication/scoring | 89.45 | 76.29 | 74.77 | 78.02 |
| Planning | 4.48 | 4.42 | 4.47 | 4.45 |
| SQ8 fetch/authentication/scoring | 116.01 | 82.77 | 81.74 | 84.75 |
| Cleanup after response—excluded from cold | 4.48 | 4.60 | 4.57 | 4.69 |

Candidate redundant-root HEAD / GET-header / stream means are **9.17 / 23.22 / 0.98ms** and **8.75 / 22.71 / 0.90ms**. Writes are nested inside stream timing. Across concurrent metadata objects, summed HEAD/GET/stream work overlaps and cannot be added to staging. Credentials, spawn versus polling, and scoring versus fetch are not independently measured.

The causal opportunity is explicit in [head validation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_store.rs:242): it authenticates `manifest.json`, discards those bytes, and [remote open](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:779) fetches the same digest-bound root again.

1. **Smallest intervention:** carry that bounded, authenticated root body through the existing head→generation→staging flow. Seed the scratch manifest from it, recheck digest/length at the seed boundary, then run the existing child admission and decoding. Apply the shared mechanism to HTTP and coordinated index opening; no global cache, credentials reuse, layout change or router/scorer change. Record local reuse explicitly: zero additional root HEAD/GET, with copy/authentication time accounted separately.

2. **Gain ceiling:** deleting the redundant wave exposes **33.49 / 32.49ms mean opportunity**, before replacement work or contention. Subtracting each row’s wave and recomputing p90 gives an **ideal counterfactual** of **426.040 / 420.749ms**—35.808 / 31.347ms below current p90. These are projections, not measured candidate outcomes or universal floors.

3. **Fastest falsifier:** extend the existing `RecordedStore` semantic fixture to reject a second root GET or any root HEAD. Require successful open and unchanged ordered results, exactly one root GET across head+open, and rejection of corrupted/oversized seeds before child GETs. Preserve source/leaf ETag and geometry failures, memory admission and scratch cleanup. Charge any retained root allocation, including published-head constructors and coordinated opens.

4. **Prospective decision:** after affected Rust compilation, required locked Clippy, complete test-build and release qualification, freeze one new matched ABBA protocol: **current width8 control versus width8+root-reuse candidate**, same64 queries, immutable publication,8 offeredQPS/eight owners and resource limits. Collect all four declared cells; require all256 successes, ≥95%R10, exact parity, dispatch/identity/resource/cleanup gates. **KILL this intervention** for correctness/admission failure or failure of either candidate to improve both p90 and p95 against both actual bracketing controls. Report444ms attainment separately.

Current candidate p90 still misses the recorded **turbopuffer444ms context by8.096–17.848ms**. AWS subsecond directional context is attained, but matched AWS and turbopuffer latency, quality, sustained capacity and lifecycle-dollar comparisons remain absent. CoHere’s91.25%R10 failure also remains unchanged. No physical-S3, cost or saturation conclusion follows.

