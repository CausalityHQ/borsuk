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

/-! A per-query certificate can discharge the lower-tail gate without
postulating its observed value. It must bind the 100 paired truth positions
for that query and count the actual lost hits. -/

theorem query_stays_at_least_ninety
    (pairs : List (Bool × Bool))
    (_truth_positions : pairs.length = 100)
    (loss_budget : lostHits pairs + 90 ≤ sourceHits pairs) :
    90 ≤ compressedHits pairs := by
  have accounting := source_hits_le_compressed_plus_lost pairs
  omega

/-! A 96-byte sign record has 752 bits and one binary16 scale. The same
arithmetic applies to PQ96, though its scorer has different CPU work.
Headers, IDs, authentication data and source vectors are not row bytes. -/

def signRecordBytes : Nat := 94 + 2

def packedCodeBytes (rows : Nat) : Nat := signRecordBytes * rows

def groupedCodeBytes (pages rows : Nat) : Nat :=
  4 + 4 * pages + packedCodeBytes rows

theorem sign_record_is_ninety_six : signRecordBytes = 96 := by decide

theorem one_hundred_million_code_bytes :
    packedCodeBytes 100000000 = 9600000000 := by decide

theorem code_row_budget
    (pages rows : Nat)
    (within_code_payload_budget : groupedCodeBytes pages rows ≤ 16777216) :
    rows ≤ 174762 := by
  simp only [groupedCodeBytes, packedCodeBytes, signRecordBytes] at within_code_payload_budget
  omega

theorem code_row_budget_exact_remainder :
    96 * 174762 + 64 = 16777216 := by decide

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
