import Std

/-!
Conditional per-query recall accounting for a complete source roster.
The premises are data obligations, not conclusions about PQ64 or SQ8:
an exact top-k cutoff, a uniform upper SQ8 score-error bound for truth
rows, a nominee threshold no lower than that cutoff minus the error,
and an authenticated count of omitted rows in the expanded score window.
No independence of row events is assumed.
-/

namespace Borsuk.ConditionalRecallWindow

theorem truth_row_in_surrogate_window
    (trueScore surrogateScore exactCutoff nomineeThreshold error : Int)
    (truthWithinCutoff : trueScore ≤ exactCutoff)
    (surrogateUpperError : surrogateScore ≤ trueScore + error)
    (nomineeLowerBound : exactCutoff ≤ nomineeThreshold + error) :
    surrogateScore ≤ nomineeThreshold + 2 * error := by
  omega

structure RowWitness where
  truth : Bool
  selected : Bool
  near : Bool

def truthCount (rows : List RowWitness) : Nat :=
  (rows.filter fun row => row.truth).length

def returnedTruth (rows : List RowWitness) : Nat :=
  (rows.filter fun row => row.truth && row.selected).length

def nearOmissions (rows : List RowWitness) : Nat :=
  (rows.filter fun row => row.near && !row.selected).length

theorem truth_count_le_returned_plus_near
    (rows : List RowWitness)
    (sound : ∀ row ∈ rows,
      row.truth = true → row.selected = false → row.near = true) :
    truthCount rows ≤ returnedTruth rows + nearOmissions rows := by
  induction rows with
  | nil => simp [truthCount, returnedTruth, nearOmissions]
  | cons row rest ih =>
      have tailSound : ∀ next ∈ rest,
          next.truth = true → next.selected = false → next.near = true := by
        intro next inRest isTruth notSelected
        exact sound next (by simp [inRest]) isTruth notSelected
      have tail := ih tailSound
      have headSound : row.truth = true → row.selected = false → row.near = true :=
        sound row (by simp)
      cases row with
      | mk truth selected near =>
        cases truth <;> cases selected <;> cases near <;>
          simp [truthCount, returnedTruth, nearOmissions] at * <;>
          omega

theorem recall_floor_of_near_omission_certificate
    (rows : List RowWitness) (k target : Nat)
    (truthSize : truthCount rows = 100)
    (nearBound : nearOmissions rows ≤ k)
    (targetFits : target + k ≤ 100)
    (sound : ∀ row ∈ rows,
      row.truth = true → row.selected = false → row.near = true) :
    target ≤ returnedTruth rows := by
  have accounting := truth_count_le_returned_plus_near rows sound
  omega

end Borsuk.ConditionalRecallWindow
