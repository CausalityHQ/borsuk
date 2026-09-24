import Std

/-!
Conditional arithmetic model for V151's page-seeded graph search. The model
counts primary seeds and newly evaluated units. It does not assert that the
Rust frontier implements the model or that graph exploration finds useful
pages. Centroid scores, recall, elapsed time and charged RAM need separate
implementation and measurement evidence.
-/

namespace Borsuk.PageSeededGraphBounds

def primarySeedBudget (primaryPages : Nat) : Nat := 8 * primaryPages
def additionalEvaluationBudget (primaryPages : Nat) : Nat := 8 * primaryPages
def exactCandidateBudget (primaryPages : Nat) : Nat := 5 * primaryPages

theorem distinct_evaluations_le_sixteen_per_primary
    (primaryPages seeds discovered : Nat)
    (seedBound : seeds ≤ primarySeedBudget primaryPages)
    (discoveryBound : discovered ≤ additionalEvaluationBudget primaryPages) :
    seeds + discovered ≤ 16 * primaryPages := by
  unfold primarySeedBudget at seedBound
  unfold additionalEvaluationBudget at discoveryBound
  omega

theorem exact_candidate_pages_le_five_per_primary
    (primaryPages additionalPages : Nat)
    (additionalBound : additionalPages ≤ 4 * primaryPages) :
    primaryPages + additionalPages ≤ exactCandidateBudget primaryPages := by
  unfold exactCandidateBudget
  omega

theorem provisional_min_upper_bounds_exact_min
    (exactMin fallback : Nat) (observedDistances : List Nat)
    (lowerBound : ∀ distance ∈ observedDistances, exactMin ≤ distance)
    (fallbackBound : exactMin ≤ fallback) :
    exactMin ≤ observedDistances.foldl min fallback := by
  induction observedDistances generalizing fallback with
  | nil => simpa using fallbackBound
  | cons distance rest inductionHypothesis =>
      simp only [List.foldl_cons]
      apply inductionHypothesis
      · intro value membership
        exact lowerBound value (by simp [membership])
      · have observedBound := lowerBound distance (by simp)
        omega

end Borsuk.PageSeededGraphBounds
