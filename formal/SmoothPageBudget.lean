import Std

/-!
Conditional byte and GET accounting for V160/V163/V164's 512-row D768 SQ8
page format. A row is 780 bytes, a planner unit is 32 rows, and the 1M
object's final page has 64 rows. The theorems assume authenticated counts
of whole full/final pages in the emitted intervals. They do not prove that
the Python planner emits those counts, that the router captures GT rows,
or that S3 meets any latency target.
-/

namespace Borsuk.SmoothPageBudget

def unitBytes : Nat := 32 * 780
def fullPageBytes : Nat := 512 * 780
def finalPageBytes : Nat := 64 * 780
def capBytes : Nat := 16777216
def chargedUnits (fullPages finalPages : Nat) : Nat :=
  16 * fullPages + 2 * finalPages
def chargedBytes (fullPages finalPages : Nat) : Nat :=
  fullPageBytes * fullPages + finalPageBytes * finalPages

theorem page_geometry :
    unitBytes = 24960 ∧ fullPageBytes = 399360 ∧
    finalPageBytes = 49920 := by
  decide

theorem charged_bytes_are_units (fullPages finalPages : Nat) :
    chargedBytes fullPages finalPages =
      unitBytes * chargedUnits fullPages finalPages := by
  simp [chargedBytes, chargedUnits, unitBytes, fullPageBytes,
    finalPageBytes, Nat.mul_add]
  omega

theorem maximum_units_exact :
    unitBytes * 672 = 16773120 ∧
    capBytes - unitBytes * 672 = 4096 ∧
    capBytes < unitBytes * 673 := by
  decide

theorem charged_units_fit_iff_bytes_fit (fullPages finalPages : Nat) :
    chargedUnits fullPages finalPages ≤ 672 ↔
      chargedBytes fullPages finalPages ≤ capBytes := by
  rw [charged_bytes_are_units]
  simp [unitBytes, capBytes]
  omega

theorem admitted_plan_respects_both_caps
    (gets fullPages finalPages : Nat)
    (getsBound : gets ≤ 32)
    (unitsBound : chargedUnits fullPages finalPages ≤ 672) :
    gets ≤ 32 ∧ chargedBytes fullPages finalPages ≤ capBytes := by
  exact ⟨getsBound,
    (charged_units_fit_iff_bytes_fit fullPages finalPages).mp unitsBound⟩

theorem forty_two_full_pages_fit :
    chargedBytes 42 0 = 16773120 ∧
    chargedBytes 43 0 > capBytes := by
  decide

end Borsuk.SmoothPageBudget
