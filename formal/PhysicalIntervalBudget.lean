import Std

/-!
V63/V70 whole-page SQ8 physical byte accounting. These theorems apply to
an authenticated single contiguous 1M-row object with 780 bytes per row,
256-row full pages, and a 64-row final page. They prove the arithmetic
used by the V110 oracle and V111 planner, conditional on a certified
count of fetched full and short pages. They do not prove the Python DP
refinement, query-time recall, or S3 service latency.
-/

namespace Borsuk.PhysicalIntervalBudget

def unitBytes : Nat := 64 * 780
def fullPageBytes : Nat := 256 * 780
def shortPageBytes : Nat := 64 * 780
def maxBytes : Nat := 16777216
def chargedUnits (fullPages shortPages : Nat) : Nat := 4 * fullPages + shortPages
def chargedBytes (fullPages shortPages : Nat) : Nat :=
  fullPageBytes * fullPages + shortPageBytes * shortPages

theorem page_geometry :
    unitBytes = 49920 ∧ fullPageBytes = 199680 ∧
    shortPageBytes = 49920 := by
  decide

theorem charged_bytes_are_units (fullPages shortPages : Nat) :
    chargedBytes fullPages shortPages =
      unitBytes * chargedUnits fullPages shortPages := by
  simp [chargedBytes, chargedUnits, unitBytes, fullPageBytes, shortPageBytes]
  omega

theorem units_fit_iff_bytes_fit (fullPages shortPages : Nat) :
    chargedUnits fullPages shortPages ≤ 336 ↔
      chargedBytes fullPages shortPages ≤ maxBytes := by
  rw [charged_bytes_are_units]
  simp [unitBytes, maxBytes]
  omega

theorem maximum_units_exact :
    unitBytes * 336 = 16773120 ∧
    maxBytes - unitBytes * 336 = 4096 ∧
    maxBytes < unitBytes * 337 := by
  decide

theorem thirty_two_requests_and_336_units_fit
    (requests fullPages shortPages : Nat)
    (request_cap : requests ≤ 32)
    (unit_cap : chargedUnits fullPages shortPages ≤ 336) :
    requests ≤ 32 ∧ chargedBytes fullPages shortPages ≤ maxBytes := by
  exact ⟨request_cap, (units_fit_iff_bytes_fit fullPages shortPages).mp unit_cap⟩

end Borsuk.PhysicalIntervalBudget
