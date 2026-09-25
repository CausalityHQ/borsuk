import Std

/-!
Conditional facts for a GT-blind physical interval planner whose unit
weights come from a fitted source model. The error premises must be
validated on the target query distribution. No theorem here establishes
that PQ scores satisfy them, that Python/Rust implements an optimum, or
that S3 latency or charged RAM meets a target.
-/

namespace Borsuk.PredictedIntervalGuarantees

/-! A conservative modeled-mass threshold becomes an actual recall floor
only when the supplied model-error bound applies to this query and plan. -/
theorem predicted_target_implies_actual_floor
    (predicted actual target error : Nat)
    (modelError : predicted ≤ actual + error)
    (predictedTarget : target + error ≤ predicted) :
    target ≤ actual := by
  omega

/-! If the predicted optimizer is exact over the same feasible-plan set
and both plans have error at most `error`, the selected plan loses at most
`2 * error` true neighbors versus any alternative. The premise includes
candidate omissions in the error; a source fit curve alone does not
certify it for unseen queries. -/
theorem modeled_optimum_with_error_is_near_actual_optimum
    (selectedPredicted selectedActual alternativePredicted
      alternativeActual error : Nat)
    (modeledOptimal : alternativePredicted ≤ selectedPredicted)
    (selectedError : selectedPredicted ≤ selectedActual + error)
    (alternativeError : alternativeActual ≤ alternativePredicted + error) :
    alternativeActual ≤ selectedActual + 2 * error := by
  omega

def pricedScore (mass units gets unitPrice getPrice : Int) : Int :=
  mass - unitPrice * units - getPrice * gets

/-! At fixed nonnegative prices, an exact priced optimum cannot be
strictly worse in modeled mass while also using at least as many units
and GETs. This is conditional on the planner's optimality certificate;
it does not say that a dual price will satisfy hard caps. -/
theorem priced_optimum_is_pareto_undominated
    (selectedMass selectedUnits selectedGets
      alternativeMass alternativeUnits alternativeGets
      unitPrice getPrice : Int)
    (priceNonnegative : 0 ≤ unitPrice ∧ 0 ≤ getPrice)
    (modeledOptimal :
      pricedScore alternativeMass alternativeUnits alternativeGets
        unitPrice getPrice ≤
      pricedScore selectedMass selectedUnits selectedGets
        unitPrice getPrice)
    (alternativeMoreMass : selectedMass < alternativeMass)
    (alternativeNoMoreUnits : alternativeUnits ≤ selectedUnits)
    (alternativeNoMoreGets : alternativeGets ≤ selectedGets) : False := by
  have unitCharge : unitPrice * alternativeUnits ≤
      unitPrice * selectedUnits :=
    Int.mul_le_mul_of_nonneg_left alternativeNoMoreUnits priceNonnegative.1
  have getCharge : getPrice * alternativeGets ≤
      getPrice * selectedGets :=
    Int.mul_le_mul_of_nonneg_left alternativeNoMoreGets priceNonnegative.2
  unfold pricedScore at modeledOptimal
  omega

/-! For plans with identical physical charges, a fixed-price optimum
also maximizes predicted mass. A validated two-sided error bound then
gives the same `2 * error` true-mass comparison. If charges differ,
this conclusion needs an additional bound on their price difference. -/
theorem equal_charge_priced_choice_near_actual_optimum
    (selectedPredicted selectedActual alternativePredicted
      alternativeActual units gets unitPrice getPrice : Int)
    (error : Nat)
    (pricedOptimal :
      pricedScore alternativePredicted units gets unitPrice getPrice ≤
      pricedScore selectedPredicted units gets unitPrice getPrice)
    (selectedError :
      selectedPredicted ≤ selectedActual + (error : Int))
    (alternativeError :
      alternativeActual ≤ alternativePredicted + (error : Int)) :
    alternativeActual ≤ selectedActual + 2 * (error : Int) := by
  unfold pricedScore at pricedOptimal
  omega

/-! A uniform validated error certificate gives every query in the
listed cohort a recall floor, hence every empirical percentile too.
Without the uniform premise, one must measure the tail directly. -/
theorem every_certified_query_meets_target
    {Query : Type} (queries : List Query)
    (predicted actual : Query → Nat) (target error : Nat)
    (certified : ∀ query ∈ queries,
      predicted query ≤ actual query + error ∧
      target + error ≤ predicted query) :
    ∀ query ∈ queries, target ≤ actual query := by
  intro query member
  obtain ⟨modelError, predictedTarget⟩ := certified query member
  exact predicted_target_implies_actual_floor
    (predicted query) (actual query) target error
    modelError predictedTarget

/-! A dense exact interval DP with two open/closed states has this many
state visits in the stated model. This is a work count, not elapsed
time. A sparse or pruned implementation needs its own refinement proof. -/
def hardCapStateVisits (sites getCap unitCap : Nat) : Nat :=
  2 * sites * (getCap + 1) * (unitCap + 1)

theorem hard_cap_dp_work_from_per_state_bound
    (sites getCap unitCap perStateNs setupNs elapsedNs : Nat)
    (implementationBound :
      elapsedNs ≤ setupNs +
        hardCapStateVisits sites getCap unitCap * perStateNs) :
    elapsedNs ≤ setupNs +
      2 * sites * (getCap + 1) * (unitCap + 1) * perStateNs := by
  simpa [hardCapStateVisits] using implementationBound

/-! A hard-cap planner cannot exceed the physical budget if its emitted
interval witness refines its internal counters. The witness/accounting
premise must be checked against the implementation and page geometry. -/
theorem hard_cap_witness_admission
    (plannedUnits plannedGets actualUnits actualGets unitCap getCap : Nat)
    (unitAccounting : actualUnits = plannedUnits)
    (getAccounting : actualGets = plannedGets)
    (unitBound : plannedUnits ≤ unitCap)
    (getBound : plannedGets ≤ getCap) :
    actualUnits ≤ unitCap ∧ actualGets ≤ getCap := by
  omega

/-! A sequential service ceiling follows only from independently
established upper bounds on request, byte and local work terms. -/
theorem bounded_plan_latency
    (gets bytes localNs requestNsPerGet transferNsPerByte
      fixedNs elapsedNs getCap byteCap localCap : Nat)
    (serviceBound : elapsedNs ≤ fixedNs + gets * requestNsPerGet +
      bytes * transferNsPerByte + localNs)
    (getsBound : gets ≤ getCap)
    (bytesBound : bytes ≤ byteCap)
    (localBound : localNs ≤ localCap) :
    elapsedNs ≤ fixedNs + getCap * requestNsPerGet +
      byteCap * transferNsPerByte + localCap := by
  have getCostBound :=
    Nat.mul_le_mul_right requestNsPerGet getsBound
  have byteCostBound :=
    Nat.mul_le_mul_right transferNsPerByte bytesBound
  omega

end Borsuk.PredictedIntervalGuarantees
