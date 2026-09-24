import Std

/-!
Conditional bounds for the V140/V143 primary-first page admission policy.
`cover` must return the actual coalesced physical GET count and byte charge
for a selected roster. This model does not prove that the Rust implementation
refines `cover`, that centroid scores rank useful pages, empirical recall,
charged memory, or hardware/S3 latency.
-/

namespace Borsuk.BudgetedPageAdmission

structure Plan where
  selected : List Nat
  gets : Nat
  bytes : Nat
  deriving Repr

def admit (cover : List Nat → Nat × Nat) (maxGets maxBytes : Nat)
    (state : Plan) (page : Nat) : Plan :=
  let proposal := state.selected ++ [page]
  let charge := cover proposal
  if charge.1 ≤ maxGets ∧ charge.2 ≤ maxBytes then
    { selected := proposal, gets := charge.1, bytes := charge.2 }
  else state

theorem admit_bounds (cover : List Nat → Nat × Nat)
    (maxGets maxBytes : Nat) (state : Plan) (page : Nat)
    (hGets : state.gets ≤ maxGets)
    (hBytes : state.bytes ≤ maxBytes) :
    (admit cover maxGets maxBytes state page).gets ≤ maxGets ∧
    (admit cover maxGets maxBytes state page).bytes ≤ maxBytes := by
  by_cases fits : (cover (state.selected ++ [page])).1 ≤ maxGets ∧
      (cover (state.selected ++ [page])).2 ≤ maxBytes
  · simp [admit, fits]
  · simp [admit, fits, hGets, hBytes]

theorem fold_bounds (cover : List Nat → Nat × Nat)
    (maxGets maxBytes : Nat) (ranked : List Nat) (state : Plan)
    (hGets : state.gets ≤ maxGets)
    (hBytes : state.bytes ≤ maxBytes) :
    (ranked.foldl (admit cover maxGets maxBytes) state).gets ≤ maxGets ∧
    (ranked.foldl (admit cover maxGets maxBytes) state).bytes ≤ maxBytes := by
  induction ranked generalizing state with
  | nil => exact ⟨hGets, hBytes⟩
  | cons page rest ih =>
      simp only [List.foldl_cons]
      obtain ⟨nextGets, nextBytes⟩ :=
        admit_bounds cover maxGets maxBytes state page hGets hBytes
      exact ih (admit cover maxGets maxBytes state page) nextGets nextBytes

theorem production_wave_bounds (cover : List Nat → Nat × Nat)
    (ranked : List Nat) :
    (ranked.foldl (admit cover 32 16777216) ⟨[], 0, 0⟩).gets ≤ 32 ∧
    (ranked.foldl (admit cover 32 16777216) ⟨[], 0, 0⟩).bytes ≤ 16777216 := by
  exact fold_bounds cover 32 16777216 ranked ⟨[], 0, 0⟩ (by simp) (by simp)

def pageBytes (dimensions pageRows : Nat) : Nat :=
  pageRows * (dimensions + 12)

theorem current_full_page_geometry :
    pageBytes 96 256 = 27648 ∧
    pageBytes 768 256 = 199680 ∧
    16777216 / pageBytes 96 256 = 606 ∧
    16777216 / pageBytes 768 256 = 84 := by
  decide

/-! Payload only: 64 PQ64 bytes/row, two f32 page summaries per
256-row page (`D/32` bytes/row), one f16 32-row unit centroid
(`2D/32` bytes/row), and 16 source-ID map bytes/row. This omits
headers, final-page rounding, hierarchy, allocator, cache and scratch.
The coefficient is a resource policy parameter; it contains no N switch. -/
def residentBytes (rows generations bytesPerRow : Nat) : Nat :=
  generations * rows * bytesPerRow

def currentBytesPerRow (dimensions : Nat) : Nat :=
  64 + dimensions / 32 + 2 * dimensions / 32 + 16

theorem current_bytes_per_row_examples :
    currentBytesPerRow 96 = 89 ∧
    currentBytesPerRow 768 = 152 := by
  decide

theorem resident_rows_monotone (rows₁ rows₂ generations bytesPerRow : Nat)
    (h : rows₁ ≤ rows₂) :
    residentBytes rows₁ generations bytesPerRow ≤
      residentBytes rows₂ generations bytesPerRow := by
  simp only [residentBytes]
  exact Nat.mul_le_mul_right bytesPerRow (Nat.mul_le_mul_left generations h)

theorem resident_recall_budget_monotone (rows generations
    lowerHigherRecallBytes higherRecallBytes : Nat)
    (h : lowerHigherRecallBytes ≤ higherRecallBytes) :
    residentBytes rows generations lowerHigherRecallBytes ≤
      residentBytes rows generations higherRecallBytes := by
  simp only [residentBytes]
  exact Nat.mul_le_mul_left (generations * rows) h

theorem hundred_million_payload_examples :
    residentBytes 100000000 1 (currentBytesPerRow 96) = 8900000000 ∧
    residentBytes 100000000 1 (currentBytesPerRow 768) = 15200000000 ∧
    residentBytes 100000000 2 (currentBytesPerRow 96) = 17800000000 ∧
    residentBytes 100000000 2 (currentBytesPerRow 768) = 30400000000 := by
  decide

end Borsuk.BudgetedPageAdmission
