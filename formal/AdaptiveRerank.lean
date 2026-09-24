import Std

/-!
Conditional score-interval facts for a generic quantized-first, exact-source
reranker. The implementation must authenticate candidate IDs, quantized score
errors, exact fallback scores and tie behavior. No theorem below supplies
those premises or proves unseen recall, hardware latency or charged memory.
-/

namespace Borsuk.AdaptiveRerank

def soundInterval (exact approx error : Nat → Int) (row : Nat) : Prop :=
  0 ≤ error row ∧
  approx row ≤ exact row + error row ∧
  exact row ≤ approx row + error row

theorem definitely_below_threshold
    (exact approx error : Nat → Int) (row : Nat) (threshold : Int)
    (sound : soundInterval exact approx error row)
    (below : approx row + error row < threshold) :
    exact row < threshold := by
  rcases sound with ⟨_, _, upper⟩
  omega

theorem definitely_above_threshold
    (exact approx error : Nat → Int) (row : Nat) (threshold : Int)
    (sound : soundInterval exact approx error row)
    (above : threshold + error row ≤ approx row) :
    threshold ≤ exact row := by
  rcases sound with ⟨_, lower, _⟩
  omega

/-! Any row whose score interval lies wholly below the threshold is
strictly outranked by each certified witness whose interval lies above it.
If the witnesses are `k` distinct candidate rows, a correct exact top-`k`
sort cannot select that row. The cardinality and sort refinement are
separate premises for a production certificate. -/
theorem pruned_row_below_every_witness
    (exact approx error : Nat → Int) (threshold : Int)
    (witnesses : List Nat) (row : Nat)
    (soundRow : soundInterval exact approx error row)
    (soundWitness : ∀ witness ∈ witnesses,
      soundInterval exact approx error witness)
    (pruned : approx row + error row < threshold)
    (certified : ∀ witness ∈ witnesses,
      threshold + error witness ≤ approx witness) :
    ∀ witness ∈ witnesses, exact row < exact witness := by
  intro witness belongs
  have rowBelow := definitely_below_threshold exact approx error
    row threshold soundRow pruned
  have witnessAbove := definitely_above_threshold exact approx error
    witness threshold (soundWitness witness belongs)
    (certified witness belongs)
  omega

def fp16TierPayloadBytes
    (rows dimensions generations metadataBytesPerRow : Nat) : Nat :=
  rows * (2 * dimensions + metadataBytesPerRow) * generations

theorem payload_scales_with_rows
    (rows dimensions generations metadataBytesPerRow : Nat) :
    fp16TierPayloadBytes rows dimensions generations metadataBytesPerRow =
      rows * ((2 * dimensions + metadataBytesPerRow) * generations) := by
  simp [fp16TierPayloadBytes, Nat.mul_assoc]

theorem hundred_million_fp16_payload_examples :
    fp16TierPayloadBytes 100_000_000 96 2 8 = 40_000_000_000 ∧
    fp16TierPayloadBytes 100_000_000 768 2 8 = 308_800_000_000 := by
  decide

/-! SHA-256 digest-table payload for a full authenticated float32 source
plane with an embedded 8-byte source ID per row. This counts 32 bytes per
verification block and a 64-byte format
header per generation. It excludes Vec overhead, page cache and I/O. The
block size is a resource policy input, not a vector-count quality switch. -/
def sourceTierPayloadBytes
    (rows dimensions generations : Nat) : Nat :=
  generations * (64 + rows * (8 + 4 * dimensions))

def sourceDigestBytes
    (rows dimensions generations blockBytes : Nat) : Nat :=
  generations * (32 * ((64 + rows * (8 + 4 * dimensions) + blockBytes - 1) / blockBytes))

theorem hundred_million_source_payload_examples :
    sourceTierPayloadBytes 100_000_000 96 2 = 78_400_000_128 ∧
    sourceTierPayloadBytes 100_000_000 768 2 = 616_000_000_128 := by
  decide

theorem hundred_million_source_digest_examples :
    sourceDigestBytes 100_000_000 96 2 65_536 = 38_281_280 ∧
    sourceDigestBytes 100_000_000 768 2 65_536 = 300_781_312 ∧
    sourceDigestBytes 100_000_000 768 2 4_096 = 4_812_500_032 := by
  decide

/-! A finite-cohort capture ceiling: an authenticated roster certificate and
candidate-only return refinement can rule out an aggregate target, regardless
of the scorer. The V124 number is an external artifact premise, not a Lean
derivation from Parquet or evidence JSONL. -/
theorem target_impossible_below_capture
    (returned captured target : Nat)
    (restricted : returned ≤ captured)
    (captureBelowTarget : captured < target) :
    returned < target := by
  omega

theorem relaion_v124_nominee_only_below_99_percent
    (returned captured : Nat)
    (restricted : returned ≤ captured)
    (sealedCapture : captured = 96_849) :
    returned < 99_000 := by
  exact target_impossible_below_capture returned captured 99_000
    restricted (by omega)

end Borsuk.AdaptiveRerank
