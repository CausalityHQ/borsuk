import Std

/-!
Conditional primary-nominee stability for the V113 one-wave route.

The threshold must separate the 100 true-score primary nominees from every
secondary nominee. Authenticated per-row score bounds and boundary margins
are premises. This file does not derive those bounds from SQ8/PQ bytes or
establish any unseen-query recall, live latency, or measured memory peak.
-/

namespace Borsuk.NomineePrimary

def primary (rows : List Nat) (score : Nat → Int) (threshold : Int) : List Nat :=
  rows.filter (fun row => score row < threshold)

theorem primary_stable_of_boundary_margin
    (rows : List Nat) (trueScore approximateScore error : Nat → Int)
    (threshold : Int)
    (bounds : ∀ row ∈ rows,
      0 ≤ error row ∧
      approximateScore row ≤ trueScore row + error row ∧
      trueScore row ≤ approximateScore row + error row ∧
      (trueScore row + error row < threshold ∨
       threshold + error row ≤ trueScore row)) :
    primary rows trueScore threshold =
      primary rows approximateScore threshold := by
  induction rows with
  | nil => simp [primary]
  | cons row rest inductionHypothesis =>
      have rowBounds := bounds row (by simp)
      have restBounds : ∀ next ∈ rest,
          0 ≤ error next ∧
          approximateScore next ≤ trueScore next + error next ∧
          trueScore next ≤ approximateScore next + error next ∧
          (trueScore next + error next < threshold ∨
           threshold + error next ≤ trueScore next) := by
        intro next inRest
        exact bounds next (by simp [inRest])
      have sameSide :
          (trueScore row < threshold) ↔
            (approximateScore row < threshold) := by
        rcases rowBounds with ⟨nonnegative, upper, lower, inside | outside⟩
        · constructor <;> intro _ <;> omega
        · constructor <;> intro _ <;> omega
      have tailEqual := inductionHypothesis restBounds
      have filterTail :
          rest.filter (fun next => trueScore next < threshold) =
            rest.filter (fun next => approximateScore next < threshold) := by
        simpa [primary] using tailEqual
      simp only [primary, List.filter_cons]
      simp [sameSide, filterTail]

theorem hundred_primary_votes_stable
    (rows : List Nat) (trueScore approximateScore error : Nat → Int)
    (threshold : Int) (pageWeights : List Nat → Nat → Nat)
    (bounds : ∀ row ∈ rows,
      0 ≤ error row ∧
      approximateScore row ≤ trueScore row + error row ∧
      trueScore row ≤ approximateScore row + error row ∧
      (trueScore row + error row < threshold ∨
       threshold + error row ≤ trueScore row))
    (hundred : (primary rows trueScore threshold).length = 100) :
    (primary rows approximateScore threshold).length = 100 ∧
    (∀ page, pageWeights (primary rows trueScore threshold) page =
      pageWeights (primary rows approximateScore threshold) page) := by
  have same := primary_stable_of_boundary_margin rows trueScore
    approximateScore error threshold bounds
  rw [same] at hundred ⊢
  exact ⟨hundred, fun _ => rfl⟩

/-! A fixed deterministic downstream route receives the same primary list
when the authenticated margin premise holds. A fixed truth roster therefore
has identical returned hits. This is a conditional equivalence, not a lower
bound on either route's unknown recall. -/

def returnedHits (truth returned : List Nat) : Nat :=
  (truth.filter (fun row => row ∈ returned)).length

theorem returned_hits_stable_under_fixed_route
    (rows truth : List Nat) (trueScore approximateScore error : Nat → Int)
    (threshold : Int) (route : List Nat → List Nat)
    (bounds : ∀ row ∈ rows,
      0 ≤ error row ∧
      approximateScore row ≤ trueScore row + error row ∧
      trueScore row ≤ approximateScore row + error row ∧
      (trueScore row + error row < threshold ∨
       threshold + error row ≤ trueScore row)) :
    returnedHits truth (route (primary rows trueScore threshold)) =
      returnedHits truth (route (primary rows approximateScore threshold)) := by
  rw [primary_stable_of_boundary_margin rows trueScore approximateScore
    error threshold bounds]

def residentPayloadBytes (rows generations : Nat) : Nat :=
  rows * 16 * generations

theorem resident_payload_exact_for_any_scale (rows generations : Nat) :
    residentPayloadBytes rows generations = rows * (16 * generations) := by
  simp [residentPayloadBytes, Nat.mul_assoc]

theorem hundred_million_two_generation_r16_payload :
    residentPayloadBytes 100_000_000 2 = 3_200_000_000 := by decide

end Borsuk.NomineePrimary
