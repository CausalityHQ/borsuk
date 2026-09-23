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

/-! A 96-byte PQ record performs 96 table lookups. These are exact
algorithmic counts, not wall-clock bounds. The 16-MiB premise concerns
the code payload and is distinct from the 16-MiB data-range wave. -/

def pqTableLookups (rows : Nat) : Nat := 96 * rows

theorem hundred_million_pq_full_scan_lookups :
    pqTableLookups 100000000 = 9600000000 := by decide

theorem pq_code_payload_lookup_budget
    (pages rows : Nat)
    (within_code_payload_budget : groupedCodeBytes pages rows ≤ 16777216) :
    pqTableLookups rows ≤ 16777152 := by
  have row_bound := code_row_budget pages rows within_code_payload_budget
  simp only [pqTableLookups]
  omega

/-! The historical rotated two-bit record uses 192 packed bytes, one
float32 scale and one float32 norm. Its 100M-row plane is larger than
the 96-byte variants; a streaming reader need not hold that plane in RAM.
The coordinate count measures decode/score work, not CPU time. -/

def twoBitRecordBytes : Nat := 192 + 4 + 4

def twoBitGroupBytes (pages rows : Nat) : Nat :=
  4 + 4 * pages + twoBitRecordBytes * rows

def twoBitCoordinateWork (rows : Nat) : Nat := 768 * rows

theorem two_bit_record_is_two_hundred : twoBitRecordBytes = 200 := by decide

theorem hundred_million_two_bit_code_bytes :
    twoBitRecordBytes * 100000000 = 20000000000 := by decide

theorem hundred_million_two_bit_coordinate_work :
    twoBitCoordinateWork 100000000 = 76800000000 := by decide

theorem two_bit_code_payload_row_budget
    (pages rows : Nat)
    (budget : twoBitGroupBytes pages rows ≤ 16777216) :
    rows ≤ 83886 := by
  simp only [twoBitGroupBytes, twoBitRecordBytes] at budget
  omega

/-! The proposed page-local format has no group header in its code object.
Its selected page payloads are exactly 200 bytes per row; bytes of pages
bridged into a physical GET must also occur in `rowCounts`. -/

def selectedPageCodeBytes (rowCounts : List Nat) : Nat :=
  (rowCounts.map (fun rows => 200 * rows)).sum

theorem selected_page_code_bytes_exact (rowCounts : List Nat) :
    selectedPageCodeBytes rowCounts = 200 * rowCounts.sum := by
  induction rowCounts with
  | nil => simp [selectedPageCodeBytes]
  | cons rows rest ih =>
      simp only [selectedPageCodeBytes, List.map_cons, List.sum_cons]
      simp only [selectedPageCodeBytes] at ih
      omega

theorem page_local_code_wave_row_budget
    (rowCounts : List Nat)
    (budget : selectedPageCodeBytes rowCounts ≤ 16777216) :
    rowCounts.sum ≤ 83886 := by
  rw [selected_page_code_bytes_exact] at budget
  omega

theorem page_local_code_wave_coordinate_work_budget
    (rowCounts : List Nat)
    (budget : selectedPageCodeBytes rowCounts ≤ 16777216) :
    twoBitCoordinateWork rowCounts.sum ≤ 64424448 := by
  have rows := page_local_code_wave_row_budget rowCounts budget
  simp only [twoBitCoordinateWork]
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

/-! A threshold can represent the score boundary of a fixed page quota.
Each page carries an authenticated number of truth positions it owns.
If every true page score is separated from the threshold by more than the
error allowance, approximate scores make exactly the same admission
decisions. The premise concerns *all* pages competing for admission; it
is not established by an aggregate error percentile. -/

structure ThresholdPage where
  trueScore : Int
  routedScore : Int
  truthCount : Nat

def sourceAdmitted (pages : List ThresholdPage) (threshold : Int) :
    List ThresholdPage :=
  pages.filter (fun page => page.trueScore < threshold)

def routedAdmitted (pages : List ThresholdPage) (threshold : Int) :
    List ThresholdPage :=
  pages.filter (fun page => page.routedScore < threshold)

def admittedTruthHits (pages : List ThresholdPage) : Nat :=
  (pages.map ThresholdPage.truthCount).sum

theorem threshold_admission_and_hits_stable
    (pages : List ThresholdPage) (threshold error : Int)
    (scoreErrors : ∀ page ∈ pages,
      page.routedScore ≤ page.trueScore + error ∧
      page.trueScore ≤ page.routedScore + error)
    (boundaryMargins : ∀ page ∈ pages,
      page.trueScore + error < threshold ∨
      threshold + error ≤ page.trueScore) :
    routedAdmitted pages threshold = sourceAdmitted pages threshold ∧
    admittedTruthHits (routedAdmitted pages threshold) =
      admittedTruthHits (sourceAdmitted pages threshold) := by
  have same : routedAdmitted pages threshold =
      sourceAdmitted pages threshold := by
    unfold routedAdmitted sourceAdmitted
    apply List.filter_congr
    intro page member
    have bounds := scoreErrors page member
    have margin := boundaryMargins page member
    rcases margin with inside | outside
    · have routedInside : page.routedScore < threshold := by omega
      have sourceInside : page.trueScore < threshold := by omega
      simp [routedInside, sourceInside]
    · have routedOutside : ¬ page.routedScore < threshold := by omega
      have sourceOutside : ¬ page.trueScore < threshold := by omega
      simp [routedOutside, sourceOutside]
  exact ⟨same, congrArg admittedTruthHits same⟩

/-! When some pages lie near the threshold, charge every possible lost
truth position to that boundary band. This is a conservative bound for a
fixed score threshold. A byte-constrained greedy planner requires an
additional refinement or a separately certified admitted-page list. -/

def sourceThresholdContribution (page : ThresholdPage) (threshold : Int) : Nat :=
  if page.trueScore < threshold then page.truthCount else 0

def routedThresholdContribution (page : ThresholdPage) (threshold : Int) : Nat :=
  if page.routedScore < threshold then page.truthCount else 0

def boundaryBandContribution (page : ThresholdPage) (threshold error : Int) : Nat :=
  if threshold ≤ page.trueScore + error ∧ page.trueScore < threshold
  then page.truthCount else 0

theorem per_page_loss_charged_to_boundary
    (page : ThresholdPage) (threshold error : Int)
    (upperError : page.routedScore ≤ page.trueScore + error) :
    sourceThresholdContribution page threshold ≤
      routedThresholdContribution page threshold +
      boundaryBandContribution page threshold error := by
  by_cases sourceInside : page.trueScore < threshold
  · by_cases routedInside : page.routedScore < threshold
    · simp [sourceThresholdContribution, routedThresholdContribution,
        boundaryBandContribution, sourceInside, routedInside]
    · have inBand : threshold ≤ page.trueScore + error := by omega
      simp [sourceThresholdContribution, routedThresholdContribution,
        boundaryBandContribution, sourceInside, routedInside, inBand]
  · simp [sourceThresholdContribution, sourceInside]

def sourceThresholdHits (pages : List ThresholdPage) (threshold : Int) : Nat :=
  (pages.map (fun page => sourceThresholdContribution page threshold)).sum

def routedThresholdHits (pages : List ThresholdPage) (threshold : Int) : Nat :=
  (pages.map (fun page => routedThresholdContribution page threshold)).sum

def boundaryBandHits (pages : List ThresholdPage) (threshold error : Int) : Nat :=
  (pages.map (fun page => boundaryBandContribution page threshold error)).sum

theorem threshold_recall_loss_bounded_by_band
    (pages : List ThresholdPage) (threshold error : Int)
    (scoreUpper : ∀ page ∈ pages,
      page.routedScore ≤ page.trueScore + error) :
    sourceThresholdHits pages threshold ≤
      routedThresholdHits pages threshold +
      boundaryBandHits pages threshold error := by
  induction pages with
  | nil => simp [sourceThresholdHits, routedThresholdHits, boundaryBandHits]
  | cons page rest ih =>
      have head := per_page_loss_charged_to_boundary page threshold error
        (scoreUpper page (by simp))
      have tail : ∀ next ∈ rest,
          next.routedScore ≤ next.trueScore + error := by
        intro next member
        exact scoreUpper next (by simp [member])
      have tailBound := ih tail
      simp only [sourceThresholdHits, routedThresholdHits, boundaryBandHits,
        List.map_cons, List.sum_cons] at *
      omega

theorem one_million_gt100_gate_of_boundary_certificate
    (pages : List ThresholdPage) (threshold error : Int)
    (scoreUpper : ∀ page ∈ pages,
      page.routedScore ≤ page.trueScore + error)
    (sourceHits : sourceThresholdHits pages threshold = 98920)
    (boundaryHits : boundaryBandHits pages threshold error ≤ 769) :
    98151 ≤ routedThresholdHits pages threshold := by
  have bound := threshold_recall_loss_bounded_by_band
    pages threshold error scoreUpper
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
