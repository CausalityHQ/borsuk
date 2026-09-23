import Std

/-!
An exact natural-number model of the OPQ8 group admission loop. The input
ranking and sealed group lengths are parameters; this file does not claim that
Python, floating-point scoring, or S3 reads implement the model.
-/

namespace Borsuk.OPQ8

structure Plan where
  selected : List Nat
  bytes : Nat
  deriving Repr

def admit (length : Nat → Nat) (maxGets maxBytes : Nat)
    (state : Plan) (group : Nat) : Plan :=
  if state.selected.length < maxGets ∧ state.bytes + length group ≤ maxBytes then
    { selected := state.selected ++ [group], bytes := state.bytes + length group }
  else
    state

def plan (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) : Plan :=
  ranked.foldl (admit length maxGets maxBytes) ⟨[], 0⟩

theorem admit_bounds (length : Nat → Nat) (maxGets maxBytes : Nat)
    (state : Plan) (group : Nat)
    (hGets : state.selected.length ≤ maxGets)
    (hBytes : state.bytes ≤ maxBytes) :
    (admit length maxGets maxBytes state group).selected.length ≤ maxGets ∧
    (admit length maxGets maxBytes state group).bytes ≤ maxBytes := by
  by_cases h : state.selected.length < maxGets ∧ state.bytes + length group ≤ maxBytes
  · constructor
    · simpa [admit, h, List.length_append] using
        (show state.selected.length + 1 ≤ maxGets by omega)
    · simp [admit, h]
  · simp only [admit, h]
    exact ⟨hGets, hBytes⟩

theorem fold_bounds (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) (state : Plan)
    (hGets : state.selected.length ≤ maxGets)
    (hBytes : state.bytes ≤ maxBytes) :
    (ranked.foldl (admit length maxGets maxBytes) state).selected.length ≤ maxGets ∧
    (ranked.foldl (admit length maxGets maxBytes) state).bytes ≤ maxBytes := by
  induction ranked generalizing state with
  | nil => exact ⟨hGets, hBytes⟩
  | cons group rest ih =>
      simp only [List.foldl_cons]
      obtain ⟨nextGets, nextBytes⟩ :=
        admit_bounds length maxGets maxBytes state group hGets hBytes
      exact ih (admit length maxGets maxBytes state group) nextGets nextBytes

theorem fold_sublist (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) (state : Plan) :
    (ranked.foldl (admit length maxGets maxBytes) state).selected.Sublist
      (state.selected ++ ranked) := by
  induction ranked generalizing state with
  | nil => simp
  | cons group rest ih =>
      simp only [List.foldl_cons]
      by_cases h : state.selected.length < maxGets ∧
          state.bytes + length group ≤ maxBytes
      · have next := ih (admit length maxGets maxBytes state group)
        simpa [admit, h, List.append_assoc] using next
      · have next := ih (admit length maxGets maxBytes state group)
        have skip : (state.selected ++ rest).Sublist
            (state.selected ++ group :: rest) := by
          exact List.Sublist.append_left (List.Sublist.cons group (by simp)) _
        have next' :
            (rest.foldl (admit length maxGets maxBytes) state).selected.Sublist
              (state.selected ++ rest) := by
          simpa [admit, h] using next
        simpa [admit, h] using next'.trans skip

theorem admit_accounting (length : Nat → Nat) (maxGets maxBytes : Nat)
    (state : Plan) (group : Nat)
    (h : state.bytes = (state.selected.map length).sum) :
    (admit length maxGets maxBytes state group).bytes =
      ((admit length maxGets maxBytes state group).selected.map length).sum := by
  by_cases fits : state.selected.length < maxGets ∧
      state.bytes + length group ≤ maxBytes
  · have target : state.bytes + length group =
        ((state.selected ++ [group]).map length).sum := by
      simp [List.map_append, List.sum_append, h]
    simpa [admit, fits] using target
  · simpa [admit, fits] using h

theorem fold_accounting (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) (state : Plan)
    (h : state.bytes = (state.selected.map length).sum) :
    (ranked.foldl (admit length maxGets maxBytes) state).bytes =
      ((ranked.foldl (admit length maxGets maxBytes) state).selected.map length).sum := by
  induction ranked generalizing state with
  | nil => exact h
  | cons group rest ih =>
      simp only [List.foldl_cons]
      exact ih (admit length maxGets maxBytes state group)
        (admit_accounting length maxGets maxBytes state group h)

theorem plan_bounds (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) :
    (plan length maxGets maxBytes ranked).selected.length ≤ maxGets ∧
    (plan length maxGets maxBytes ranked).bytes ≤ maxBytes := by
  exact fold_bounds length maxGets maxBytes ranked ⟨[], 0⟩
    (by simp) (by simp)

theorem plan_sublist (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) :
    (plan length maxGets maxBytes ranked).selected.Sublist ranked := by
  simpa [plan] using fold_sublist length maxGets maxBytes ranked ⟨[], 0⟩

theorem plan_nodup (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) (h : ranked.Nodup) :
    (plan length maxGets maxBytes ranked).selected.Nodup :=
  (plan_sublist length maxGets maxBytes ranked).nodup h

theorem plan_selection_bound (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) :
    (plan length maxGets maxBytes ranked).selected.length ≤ ranked.length :=
  (plan_sublist length maxGets maxBytes ranked).length_le

theorem plan_accounting (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) :
    (plan length maxGets maxBytes ranked).bytes =
      ((plan length maxGets maxBytes ranked).selected.map length).sum := by
  exact fold_accounting length maxGets maxBytes ranked ⟨[], 0⟩ (by simp)

theorem production_wave_bounds (length : Nat → Nat) (ranked : List Nat) :
    (plan length 32 16777216 ranked).selected.length ≤ 32 ∧
    (plan length 32 16777216 ranked).bytes ≤ 16777216 :=
  plan_bounds length 32 16777216 ranked

/-! Integer scores can represent outward-rounded fixed-point score intervals.
The caller must authenticate and check each interval against the exact
distance calculation; the theorem only proves the implication. -/

theorem margin_preserves_order (trueEarlier trueLater
    routedEarlier routedLater error : Int)
    (earlierUpper : routedEarlier ≤ trueEarlier + error)
    (laterLower : trueLater - error ≤ routedLater)
    (gap : trueEarlier + 2 * error < trueLater) :
    routedEarlier < routedLater := by
  omega

def coveredHits (truthOwners selected : List Nat) : Nat :=
  (truthOwners.filter (fun group => group ∈ selected)).length

theorem certified_recall_lower_bound (truthOwners certified selected : List Nat)
    (positions : certified.Sublist truthOwners)
    (covered : ∀ group ∈ certified, group ∈ selected) :
    certified.length ≤ coveredHits truthOwners selected := by
  have keep : certified.filter (fun group => group ∈ selected) = certified := by
    apply List.filter_eq_self.mpr
    intro group inCertified
    simp [covered group inCertified]
  have sub := positions.filter (fun group => group ∈ selected)
  simpa [coveredHits, keep] using sub.length_le

theorem complete_recall_of_owner_certificate (truthOwners selected : List Nat)
    (h : ∀ group ∈ truthOwners, group ∈ selected) :
    coveredHits truthOwners selected = truthOwners.length := by
  induction truthOwners with
  | nil => simp [coveredHits]
  | cons group rest ih =>
      have head : group ∈ selected := h group (by simp)
      have tail : ∀ x ∈ rest, x ∈ selected := by
        intro x hx
        exact h x (by simp [hx])
      calc
        coveredHits (group :: rest) selected = 1 + coveredHits rest selected := by
          simp [coveredHits, head]
          omega
        _ = (group :: rest).length := by rw [ih tail]; simp; omega

def codePlaneBytes (rows : Nat) : Nat := 8 * rows

theorem two_full_code_planes (rows : Nat) :
    codePlaneBytes rows + codePlaneBytes rows = 16 * rows := by
  simp [codePlaneBytes]
  omega

theorem hundred_million_code_plane :
    codePlaneBytes 100000000 = 800000000 := by decide

theorem hundred_million_two_code_planes :
    codePlaneBytes 100000000 + codePlaneBytes 100000000 = 1600000000 := by
  decide

end Borsuk.OPQ8
