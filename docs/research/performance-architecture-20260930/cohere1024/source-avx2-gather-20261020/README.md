# AVX2 full-table gather: retain original SOURCE scorer

## Decision

NEGATIVE. Keep the original 256-entry per-byte SOURCE scorer. The qualified AVX2 candidate ff98dc3b96e099745630cc966aa1e9466b382c6d is retained as research evidence and is not integrated into production.

The root ran one same-executable, fixed-work primitive on Causality AMD EC2 with CPU0/100%, 2GiB RAM, swap disabled, both compiler/benchmark locks and a 900s outer limit. It completed exit0 in45.745s. Each of27 dimension/batch cases used256 distinct units, at least524288 rows per arm/repetition and8 alternating repetitions. Untimed exact score-bit checks, AVX2 backend verification and paired fingerprints passed. Counts1/7 use portable tails; count32 uses the gather candidate against an independent copy of the original blocked method.

| Dimension | Full32 candidate/original elapsed |
|---:|---:|
| 1 | 1.0342 |
| 2 | 1.0335 |
| 3 | 1.0323 |
| 7 | 1.0677 |
| 257 | 1.4756 |
| 768 | 1.5441 |
| 1000 | 1.5726 |
| 1024 | 1.5725 |
| 1025 | 1.5701 |

A ratio above1 is slower. D1024 took57.25% more kernel time; D768 took54.41% more. Portable-tail measurements were near parity. No cold ANN run or matching-recipe control rebuild was justified for this candidate. This is not a recall, population-tail-latency or competitor result.

## Qualification and limits

Affected hardware-boundary/scorer checks, the four required generation/admission checks, scalar-control compilation, locked workspace all-target Clippy correctness+suspicious, real override-unset workspace test-build jobs6, and final release lib/runner/replay builds passed on the frozen two-file patch. Original failed attempts and reviewed binaries are retained.

The safe helper uses the existing borsuk-fma hardware boundary; core forbid(unsafe_code), public BORSUK API, layouts and dataset independence were preserved. Logical scratch2191B is not a physical stack/RSS maximum. Final emitted stack reservations did not shrink after the source-level copy correction; see archived final-memory-evidence.json.

The candidate runner was built together with the replay example; the historical control was built bin-only. Ten transitive feature sets and26 dev packages differ. Therefore those two runner artifacts are not an authenticated causal cold pair. The same-executable primitive above is unaffected.

## Next gate

Stop this gather arm; do not continue LUT-arity/SIMD sweeps without new evidence. Reconcile existing routing and byte-reduction results before a new hypothesis. The earlier deterministic high-bit box bound eliminated0 of35640 full32 units on authenticated first8; do not rebuild its split layout.

One fresh read-only contextless research consultation f54464c7e65340b8 is collecting falsifiable byte-reduction alternatives. Its answer is not implementation evidence. Relevant primary-source starting points include [SPANN](https://arxiv.org/abs/2111.08566), [SPFresh](https://arxiv.org/abs/2410.14452), [SOAR](https://arxiv.org/abs/2404.00774), and [RaBitQ](https://arxiv.org/abs/2405.12497). SSD/RAM results do not establish S3 performance. Choose one bounded native offline falsifier before any new layout or paid cold measurement.

## Evidence

See publication.json for the create-only S3 archive with checksum HEAD verification. It retains source bundle, source/review/qualification/codegen artifacts, original receipts and all root primitive outputs. Main crates remain unchanged. Current production quality/performance and vendor comparison limits are unchanged.
