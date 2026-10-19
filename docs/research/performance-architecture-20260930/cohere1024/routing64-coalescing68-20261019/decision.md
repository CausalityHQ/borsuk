# Canonical range coalescing on leaf64: latency negative, GET tradeoff retained

One configuration change on qualified Rust 2906256b: canonical FP32 coalescing enabled with 1MiB byte cap, versus disabled with 65,664-byte cap. Both retain shortlist16, canonical GET16/parallel16, semantic64, Q1, SOURCE and SQ8 policy, generation and frozen corpus/query/truth.

A1/B1/B2/A2 used the same preregistered 68-query diagnostic panel. Real input admission, separate real S3 canaries, native execution, strict replay, scratch cleanup and original scalar oracle passed. The root independently authenticated complete output hashes/EOF, expected headers/config pins, returned IDs/f64 bits/physical ordinals, shortlist, upstream plans/traces/charges, truth counts and resource sums.

All four arms scored 674/680. Canonical GETs per arm fell 1088 to467; total GETs fell11678 to11057 (5.32%). Total verified bytes rose3481321088 to3531738728 (1.45%). A1/B1/B2/A2 p95 was497.196/483.390/486.293/452.806ms. Both preregistered paired 5% p95 improvement gates failed. Aggregate B/A query wall0.975809 and CPU0.995243 do not establish a latency win.

Disposition: VALID_NEGATIVE_ON_PANEL for the latency hypothesis. Retain the GET/byte Pareto observation, preserve default behavior and thresholds, and do not repeat this arm merely to chase a passing threshold. Panel percentiles are not population estimates. No provider-cache eviction, holdout, production-readiness or competitor win claim.

## Annotation erratum

Original wrapper reports incorrectly inherited upstream_plan_change_is_intervention=true. The actual intervention was canonical coalescing only; the frozen controller explicitly asserted upstream SOURCE/SQ8 plan, trace and charge equality for every query. The root recorded this error before inspecting results. Original reports and archive remain unchanged. verification.json records applicable value false and the original note. This correction changes no measurements, protocol or scientific gate.

## Immutable evidence

S3 prefix: research/semantic-router/20261019/routing64-coalescing68-a0001/
Original evidence.tar.gz:7521797 bytes, SHA256 dc1fe981cf7dd5857340d3f4cdad9ea38dc9bf253d76d630d7afed7cf6b75476.
Supplement root-independent-verification.json:5448 bytes, SHA256 c2ae49a1883282bdaacb3acaaef13e6a3e045c7587f1e11cecba14635c842da2; create-only upload and checksum HEAD passed.
Independent root recount SSM4f0cbf26-d334-481b-a0ff-70a67815dd1d exit0; supplement publication SSMf07a21c8-19b3-4e23-9a8f-9a9dc5be8652 exit0.

Next distinct hypothesis: generic direct SQ8 closure followed by existing canonical FP32 refinement, bypassing SOURCE nomination while measuring scan CPU/byte/quality tradeoffs. Closed geometry rejects256MiB/32GET;128GET fits all1000 closed traces below256MiB. This is a prospective envelope, not measured performance or actual-input admission.
