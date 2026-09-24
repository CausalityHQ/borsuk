import Std

/-!
The arithmetic behind exact interval-budget lattice normalization. `full`
and `tail` are charges in base units, and `factor` divides both. The caller
must establish that its page accounting, final-page rounding and DP states
refine this model. These theorems do not prove quality or latency.
-/

namespace Borsuk.IntervalLatticeNormalization

def charge (fullPages finalPages full tail : Nat) : Nat :=
  fullPages * full + finalPages * tail

theorem charge_factors (fullPages finalPages factor full tail : Nat) :
    charge fullPages finalPages (factor * full) (factor * tail) =
      charge fullPages finalPages full tail * factor := by
  simp only [charge, Nat.add_mul]
  congr 1 <;> ac_rfl

theorem feasible_iff_normalized
    (fullPages finalPages factor full tail budget : Nat)
    (positive : 0 < factor) :
    charge fullPages finalPages (factor * full) (factor * tail) ≤ budget ↔
      charge fullPages finalPages full tail ≤ budget / factor := by
  rw [charge_factors]
  exact (Nat.le_div_iff_mul_le positive).symm

theorem charged_bytes_identical
    (fullPages finalPages factor full tail unitBytes : Nat) :
    charge fullPages finalPages (factor * full) (factor * tail) * unitBytes =
      charge fullPages finalPages full tail * (unitBytes * factor) := by
  rw [charge_factors]
  simp [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]

theorem normalized_charge_respects_byte_cap
    (normalizedCharge unitBytes factor byteCap : Nat)
    (positiveUnit : 0 < unitBytes)
    (positiveFactor : 0 < factor)
    (fits : normalizedCharge ≤ byteCap / (unitBytes * factor)) :
    normalizedCharge * (unitBytes * factor) ≤ byteCap := by
  exact (Nat.le_div_iff_mul_le (Nat.mul_pos positiveUnit positiveFactor)).mp fits

theorem d96_sealed_budget_scale :
    16_777_216 / (32 * 108) = 4_854 ∧
    16_777_216 / (4 * (32 * 108)) = 1_213 ∧
    (32 + 1) * (1_213 + 1) * 368 = 14_742_816 := by
  decide

end Borsuk.IntervalLatticeNormalization
