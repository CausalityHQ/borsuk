import Std

/-!
A finite-cohort score-fidelity budget for the sealed 1M source-range
diagnostic. Each pair is (source-score hit, compressed-score hit) at one
ordered truth position. The lists and hit bits are authenticated inputs,
not values inferred by Lean from Parquet, Python or S3. This proves an
implication about one fixed cohort, not unseen-query recall or latency.
-/

namespace Borsuk.SourceRange

def sourceHits (pairs : List (Bool × Bool)) : Nat :=
  (pairs.filter fun pair => pair.1).length

def compressedHits (pairs : List (Bool × Bool)) : Nat :=
  (pairs.filter fun pair => pair.2).length

def lostHits (pairs : List (Bool × Bool)) : Nat :=
  (pairs.filter fun pair => pair.1 && !pair.2).length

theorem source_hits_le_compressed_plus_lost (pairs : List (Bool × Bool)) :
    sourceHits pairs ≤ compressedHits pairs + lostHits pairs := by
  induction pairs with
  | nil => simp [sourceHits, compressedHits, lostHits]
  | cons head tail ih =>
      rcases head with ⟨source, compressed⟩
      cases source <;> cases compressed <;>
        simp [sourceHits, compressedHits, lostHits] at * <;> omega

theorem gt100_gate_of_bounded_source_loss
    (pairs : List (Bool × Bool))
    (_cohort : pairs.length = 100000)
    (source_count : sourceHits pairs = 98920)
    (loss : lostHits pairs ≤ 769) :
    98151 ≤ compressedHits pairs := by
  have accounting := source_hits_le_compressed_plus_lost pairs
  omega

theorem gt10_gate_of_bounded_source_loss
    (pairs : List (Bool × Bool))
    (_cohort : pairs.length = 10000)
    (source_count : sourceHits pairs = 9956)
    (loss : lostHits pairs ≤ 28) :
    9928 ≤ compressedHits pairs := by
  have accounting := source_hits_le_compressed_plus_lost pairs
  omega

/-! The p05 and sub-90 conditions concern the per-query distribution.
Aggregate hit-loss bounds alone do not establish them. -/

theorem fixed_gate_of_fidelity_certificates
    (gt100 gt10 : List (Bool × Bool)) (p05 sub90 : Nat)
    (gt100_cohort : gt100.length = 100000)
    (gt10_cohort : gt10.length = 10000)
    (gt100_source : sourceHits gt100 = 98920)
    (gt10_source : sourceHits gt10 = 9956)
    (gt100_loss : lostHits gt100 ≤ 769)
    (gt10_loss : lostHits gt10 ≤ 28)
    (p05_certificate : 90 ≤ p05)
    (sub90_certificate : sub90 ≤ 49) :
    98151 ≤ compressedHits gt100 ∧
    9928 ≤ compressedHits gt10 ∧
    90 ≤ p05 ∧ sub90 ≤ 49 := by
  exact ⟨
    gt100_gate_of_bounded_source_loss gt100 gt100_cohort gt100_source gt100_loss,
    gt10_gate_of_bounded_source_loss gt10 gt10_cohort gt10_source gt10_loss,
    p05_certificate,
    sub90_certificate,
  ⟩

end Borsuk.SourceRange
