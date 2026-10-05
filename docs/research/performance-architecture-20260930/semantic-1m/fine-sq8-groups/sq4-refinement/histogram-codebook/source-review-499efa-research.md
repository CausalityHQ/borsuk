# Histogram SQ4 research source review

Source: `499efa02ee9513f34740b9bf11d96fedf77e2e4c`. Read-only review; native execution remains unverified. Consultation `3ab0057f21b14d7e`, effective provider/model `codex/gpt-6.1-sol`, collected exit 0.

Reviewed immutable candidate `499efa02ee9513f34740b9bf11d96fedf77e2e4c`. **Two additional P2 defects need correction before freezing.** I excluded the three pending corrections from findings. All locations below refer to that commit.

1. **Failed transcoding leaves unaccounted partial output.**  
   [Payload writes](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:4309) occur before `output.bytes` advances at line 4318. A later group-authentication failure, operation-cap failure, write failure, or final EOF failure can therefore leave a nonempty payload while [INVALID accounting](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:2774) excludes its bytes. The same partial-write omission exists in `Outputs::publish`.

   **Minimal fix:** distinguish written/unsealed bytes from durably published bytes; account successful chunks and recover partial length from the owned file descriptor on error. Add one check that corrupts a later second-pass group after an earlier group has been written, asserting retained partial-byte accounting and unopened requests/truth. This affects failed-attempt resource evidence, rather than successful ranking.

2. **Terminal-size admission happens after the complete measurement.**  
   The [terminal details](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:4990) embed the freeze descriptor and both truth descriptors. [`secure_open`](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:1845) accepts paths through 4096 bytes, but the [8 KiB terminal check](/home/rb/worktrees/borsuk-histogram-sq4-native-codebook/crates/borsuk/src/fine_sq8_groups.rs:5059) runs after all scoring and truth evaluation. Three legal 3 KiB ASCII paths exceed the reserve before any other JSON fields are counted.

   **Minimal fix:** admit a bounded terminal representation using the actual descriptors and future freeze path before transcoding. Reject excessive metadata there. Add a fixture showing this rejection precedes source-body reads. The present outcome correctly becomes INVALID, but only after avoidable work.

The remaining requested checks produced no additional concrete defect:

- **DP and cancellation:** the recurrence enumerates contiguous partitions; ascending predecessors and strict `<` implement the declared computed-f64 tie policy. Integer prefixes avoid prefix-subtraction rounding. Negative costs receive bounded handling before clamping. This supports the specified computed-cost search, without establishing real-arithmetic optimality.
- **SSE:** the actual allowance is  
  `1.52587890625e-5 × uniform_SSE + 1.8189894035458565e-12 × max(source_energy, 1)`.  
  Literal f32 reconstructions feed both SSE measurements. This is an explicit distortion tolerance; it does not imply recall.
- **Authentication and order:** both passes authenticate groups, whole-source SHA, IDs, finite source norms and exact EOF. Books bind source, coefficients, histogram and trainer. Both generations precede requests; all 128 results and the closure are authenticated before truth. Quality failure does not stop late validation.
- **Resources and scoring:** the outer admission includes training workspace and both retained books. Histograms and maps are dropped at the intended stages. Packed scoring follows the declared sequential arithmetic without SQ8 expansion. I found no additional definite compile error; compilation remains unverified.

The research choice is coherent as a distinct experimental arm. Its optimization mechanism is established [1D clustering prior art](https://arxiv.org/abs/1701.07204). The unresolved assumption is whether reducing corpus reconstruction SSE recovers the required ranking loss: approximately **76% for ReLAION and 90% for CoHere**. Histograms cannot establish directional query-error behavior. [TurboQuant](https://arxiv.org/abs/2504.19874) explicitly motivates residual correction because MSE-optimal quantization can bias inner-product estimation; [multi-bit RaBitQ](https://arxiv.org/abs/2409.09913) offers another mechanism to qualify if this arm fails. Neither paper establishes BORSUK recall or vendor parity.

No files were edited. No Cargo, rustc, native/corpus execution, AWS operations, large historical payload reads, or children were launched.

