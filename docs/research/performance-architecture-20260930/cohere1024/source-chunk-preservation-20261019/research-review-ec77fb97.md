What this change does: For flat SOURCE with cache off and `Offload2`, it retains normalized chunks and copies distinct crossing rows. It authenticates each complete physical range before scoring early walked units, while retaining row access for later planner completion.

**Verdict: suitable for bounded F256 correctness work; hold performance qualification.** I found one counterexample to the stated memory guarantee and two material coverage gaps. I did not establish an ANN result regression.

All references below are to the inspected EC2 tree, including permitted test additions.

1. **[P1 — qualification blocker] The `2B` guarantee excludes transient oversized backing owners.**  
   Locations: `sq8_s3_range.rs:1039`, `sq8_s3_range.rs:1044`; existing example at `sq8_s3_range.rs:5393`; payload reservation at `two_bit_generation.rs:382`.

   A valid 10-byte SOURCE response can arrive as the final 10 bytes of a uniquely owned 16 MiB `Bytes` allocation. `Vec::from(chunk)` recovers that allocation; normalization then allocates the bounded 10-byte replacement while the 16 MiB owner remains alive until assignment. Admission checks visible length, so they admit this case.

   This follows from the locked `bytes 1.12.0` implementation inspected on EC2: unique conversion preserves capacity and can backshift; opaque-owner conversion copies before dropping its owner. The existing oversized-slice test verifies **final capacity and copy counts**, not simultaneous ownership.

   **Smallest resolution:** state the guarantee as bounded normalized collector storage, then separately establish and charge the transport-owner envelope. An unconditional bound for arbitrary `ObjectStore` implementations needs an enforceable ownership contract. Another visible-length check cannot supply it.

   **Falsifier:** track live backing capacity through delivery, conversion, coalescing and failure, using oversized unique, offset, shared and opaque owners. Start accounting before the producer delivers the chunk. This is a counterexample to the hard-bound claim, potentially inherited from the transport boundary, rather than proof of a newly introduced regression.

2. **[P2 — qualification evidence blocker] The new boundary test cannot reliably detect wrong-row reconstruction.**  
   Locations: `two_bit_generation.rs:10011`, `two_bit_generation.rs:10037`, `two_bit_generation.rs:10075`; reconstruction at `sq8_s3_range.rs:819` and `sq8_s3_range.rs:904`.

   The new segmented test gives every row the same encoded record and constructs its range at offset zero. Copying a neighboring record can therefore preserve its score assertions and tie-based plan. The older signed-zero test exercises the contiguous callback.

   **Smallest falsifier:** strengthen this existing test with distinguishable valid records, signed-zero cases, nonzero physical range starts, and multiple fragment boundaries inside one row. Assert exact reconstructed bytes as well as score bits. Force a crossing row into later completion, assert that it was absent from `SourceScores`, and compare nomination order, evaluated-unit order and final plans through `verified_source_segment_record`.

   Without this, the tests provide useful coverage of dimensions and tails but weak evidence for row identity across the new representation.

3. **[P2 — qualification evidence blocker] Canonical FP32 lifetime tests do not exercise segmented SOURCE.**  
   Locations: `two_bit_generation.rs:14207`, `two_bit_generation.rs:14216`, `two_bit_generation.rs:14479`.

   These tests open the generation with default inline authentication. They cover canonical-read cancellation and slot retention, but do not compose those checks with the new segmented callback path.

   Static inspection supports the intended lifetime: the outer query permit acquired at `two_bit_generation.rs:4300` encloses canonical fetching. Worker jobs retain their byte lease and compute slot through authentication and callback execution at `sq8_s3_range.rs:247`. That is promising code evidence, not execution evidence for their composition.

   **Smallest falsifier:** parameterize the existing ordinary FP32 case with `Offload2`. Hold canonical reads, verify that another query cannot acquire the slot, cancel, then verify worker drain, byte-window restoration, pin retention and recovery.

The remaining inspected mechanics look coherent:

- `PageAuthority` checks exact aggregate length, original ordered unit digests and the clipped tail.
- Crossing records are retained without eager bridge scoring. Later completion still reaches the borrowed-row fallback.
- `drain_ordered` collects every outcome before sorting; failure selection then takes the earliest plan-ordered error.
- Outcome-vector growth is explicitly charged with factor three, and runtime, runner and strict reader share the authentication reservation helpers.
- I found no layout, default, schema, quantization or dataset-specific production change in this diff.

I used four simulated perspectives as hypotheses, not consulted testimony:

| Perspective | Objection or prediction |
|---|---|
| Systems | F256 trades fewer coalescing copies for more descriptors and row lookups. Actual chunk ownership determines the benefit. |
| Security | Full authentication before callbacks is sound; allocating crossing records before authentication still requires a defensible resource envelope. |
| Retrieval | Score parity on a supplied roster cannot prove that original walks selected the same roster or performed the same early and late work. |
| Skeptical operator | Final retained capacities do not prove peak memory, and separate cancellation tests do not prove the combined serving path. |

Compared with the contiguous baseline, F256 is most promising for unique, exact-capacity responses containing **65–256 nonempty chunks**. At 257 chunks it coalesces. For shared chunks, a larger threshold can incur more conversion copies before eventually coalescing. Directly retaining `Bytes` avoids some conversions but preserves oversized-owner risk; fixed slabs bound owned storage while introducing copying. No performance ranking is established.

The clocks use separate scopes without observed subtraction or reset, but `collector_copy` explicitly includes normalization, allocation and descriptor work (`sq8_s3_range.rs:1058`). Report it as collector processing CPU, not isolated memcpy CPU. The offline harness correctly declares `planner_replayed:false` and `actual_wire_replay:false`; its roster cannot establish original planner replay, early-versus-late CPU work or cold latency.

Freeze verification succeeded: exact base and diff hash matched, and in-memory reconstruction reproduced all three supplied production hashes. Current differences were confined to the test additions. Final verification receipt: SSM `985d4640-6fe1-49fa-8b3d-582f11dfd2f0`, Success/0.

Before performance qualification, require F256-bound native checks, actual producer→strict-reader conservation, Clippy and workspace test compilation. The reported F64 passes remain historical evidence.

Not checked: builds, tests, fixtures/raw payloads or performance; no files edited or experiments started.  
Risk: transient owner memory remains unbounded by the stated generic `2B` model; F256 qualification is pending.
