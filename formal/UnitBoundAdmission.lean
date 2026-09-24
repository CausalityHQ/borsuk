import Std

/-!
Conditional guarantees for a physical-unit admission policy.

The metric triangle inequality, row-to-center radius, soundness of the
unit-to-row map, and relationship of a witness threshold to exact top-k are
premises. The implementation must refine this model, including floating-point
rounding and final short-page bytes. A request budget can prevent admission
of all units whose bound is promising, in which case `topk_units_contained`
does not apply. No empirical recall, latency, throughput or charged RAM peak
is claimed by this file.
-/

namespace Borsuk.UnitBoundAdmission

/-! `qc` is metric distance from query to unit center, `qx` from query to
row, `xc` from row to center, and `radius` encloses every row in the unit.
Nat subtraction is truncated at zero. The triangle inequality implies a
sound lower bound on every row's distance. -/
theorem center_radius_is_lower_bound
    (qc qx xc radius : Nat)
    (triangle : qc ≤ qx + xc)
    (enclosed : xc ≤ radius) :
    qc - radius ≤ qx := by
  omega

/-! A witness set of k rows with exact score at most `threshold` gives an
upper bound on the true k-th score. If each unadmitted unit has a sound
lower bound strictly above that threshold, no exact top-k row is pruned.
Strictness retains score ties conservatively. This is a certificate, not a
claim that a finite I/O budget can always satisfy the premise. -/
theorem topk_units_contained
    {Row Unit : Type} [DecidableEq Row] [DecidableEq Unit]
    (corpus topk : List Row) (units admitted : List Unit)
    (unitOf : Row → Unit) (distance : Row → Nat)
    (lowerBound : Unit → Nat) (threshold : Nat)
    (topkInCorpus : ∀ row ∈ topk, row ∈ corpus)
    (unitListed : ∀ row ∈ corpus, unitOf row ∈ units)
    (sound : ∀ row ∈ corpus, lowerBound (unitOf row) ≤ distance row)
    (topkBelowWitness : ∀ row ∈ topk, distance row ≤ threshold)
    (prunedBeyondWitness :
      ∀ unit ∈ units, unit ∉ admitted → threshold < lowerBound unit) :
    ∀ row ∈ topk, unitOf row ∈ admitted := by
  intro row hrow
  by_cases present : unitOf row ∈ admitted
  · exact present
  · have known := topkInCorpus row hrow
    have listed := unitListed row known
    have lower := sound row known
    have upper := topkBelowWitness row hrow
    have pruned := prunedBeyondWitness (unitOf row) listed present
    omega

/-! A geometry budget is smooth in unit count and dimensions. The formula is
payload only; it excludes allocation overhead, graph links, page cache,
generation overlap and scratch buffers. -/
def summaryPayloadBytes (units dimensions : Nat) : Nat :=
  units * (2 * dimensions + 4)

theorem summary_payload_monotone_units
    (small large dimensions : Nat) (growth : small ≤ large) :
    summaryPayloadBytes small dimensions ≤ summaryPayloadBytes large dimensions := by
  unfold summaryPayloadBytes
  exact Nat.mul_le_mul_right (2 * dimensions + 4) growth

theorem summary_payload_monotone_dimensions
    (units small large : Nat) (growth : small ≤ large) :
    summaryPayloadBytes units small ≤ summaryPayloadBytes units large := by
  unfold summaryPayloadBytes
  apply Nat.mul_le_mul_left
  omega

/-! For equal-size physical units, admission limited to floor(byte cap /
unit bytes) cannot exceed the cap. A real short final unit requires its
actual charge in the implementation's accounting model. -/
theorem uniform_units_stay_within_byte_cap
    (selected unitBytes maxBytes : Nat)
    (selectedWithinCap : selected ≤ maxBytes / unitBytes) :
    selected * unitBytes ≤ maxBytes := by
  calc
    selected * unitBytes ≤ (maxBytes / unitBytes) * unitBytes :=
      Nat.mul_le_mul_right unitBytes selectedWithinCap
    _ ≤ maxBytes := Nat.div_mul_le_self maxBytes unitBytes

end Borsuk.UnitBoundAdmission
