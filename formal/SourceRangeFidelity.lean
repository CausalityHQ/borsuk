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

/-! The 100k sign96 screen uses a paired source arm. A bound of 300 lost
truth positions certifies its aggregate fidelity relationship. The
lower-tail and resource gates remain separate premises. -/

theorem hundred_thousand_screen_of_bounded_loss
    (pairs : List (Bool × Bool))
    (_cohort : pairs.length = 100000)
    (source_floor : 98000 ≤ sourceHits pairs)
    (loss : lostHits pairs ≤ 300) :
    97700 ≤ compressedHits pairs ∧
    sourceHits pairs ≤ compressedHits pairs + 300 := by
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

def signByteTableEntries : Nat := 94 * 256

def signByteLookups (rows : Nat) : Nat := 94 * rows

theorem sign_byte_table_size : signByteTableEntries = 24064 := by decide

theorem code_payload_lookup_budget
    (pages rows : Nat)
    (within_code_payload_budget : groupedCodeBytes pages rows ≤ 16777216) :
    signByteLookups rows ≤ 16427628 := by
  have row_bound := code_row_budget pages rows within_code_payload_budget
  simp only [signByteLookups]
  omega

/-! Uniform per-row score-error bounds also bound a page's minimum score.
This supports a non-recall-based premise for page-order certificates. -/

theorem min_score_error
    (trueA routedA trueB routedB error : Int)
    (firstUpper : routedA ≤ trueA + error)
    (firstLower : trueA ≤ routedA + error)
    (secondUpper : routedB ≤ trueB + error)
    (secondLower : trueB ≤ routedB + error) :
    min routedA routedB ≤ min trueA trueB + error ∧
    min trueA trueB ≤ min routedA routedB + error := by
  omega

def reducePageMins (state : Int × Int) (rows : List (Int × Int)) : Int × Int :=
  rows.foldl (fun current row =>
    (min current.1 row.1, min current.2 row.2)) state

theorem page_min_error_of_row_errors
    (state : Int × Int) (rows : List (Int × Int)) (error : Int)
    (stateUpper : state.2 ≤ state.1 + error)
    (stateLower : state.1 ≤ state.2 + error)
    (rowErrors : ∀ row ∈ rows,
      row.2 ≤ row.1 + error ∧ row.1 ≤ row.2 + error) :
    (reducePageMins state rows).2 ≤ (reducePageMins state rows).1 + error ∧
    (reducePageMins state rows).1 ≤ (reducePageMins state rows).2 + error := by
  induction rows generalizing state with
  | nil => simpa [reducePageMins] using And.intro stateUpper stateLower
  | cons row rest ih =>
      have rowError := rowErrors row (by simp)
      have restErrors : ∀ next ∈ rest,
          next.2 ≤ next.1 + error ∧ next.1 ≤ next.2 + error := by
        intro next inRest
        exact rowErrors next (by simp [inRest])
      have nextError := min_score_error state.1 state.2 row.1 row.2 error
        stateUpper stateLower rowError.1 rowError.2
      simpa [reducePageMins, List.foldl_cons] using
        ih (min state.1 row.1, min state.2 row.2)
          nextError.1 nextError.2 restErrors

theorem separated_page_minima_keep_order
    (stateA stateB : Int × Int)
    (rowsA rowsB : List (Int × Int)) (error : Int)
    (stateAUpper : stateA.2 ≤ stateA.1 + error)
    (stateALower : stateA.1 ≤ stateA.2 + error)
    (stateBUpper : stateB.2 ≤ stateB.1 + error)
    (stateBLower : stateB.1 ≤ stateB.2 + error)
    (rowsAErrors : ∀ row ∈ rowsA,
      row.2 ≤ row.1 + error ∧ row.1 ≤ row.2 + error)
    (rowsBErrors : ∀ row ∈ rowsB,
      row.2 ≤ row.1 + error ∧ row.1 ≤ row.2 + error)
    (gap : (reducePageMins stateA rowsA).1 + 2 * error <
      (reducePageMins stateB rowsB).1) :
    (reducePageMins stateA rowsA).2 <
      (reducePageMins stateB rowsB).2 := by
  have a := page_min_error_of_row_errors stateA rowsA error
    stateAUpper stateALower rowsAErrors
  have b := page_min_error_of_row_errors stateB rowsB error
    stateBUpper stateBLower rowsBErrors
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
