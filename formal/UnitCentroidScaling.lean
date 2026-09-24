import Std

/-!
Arithmetic model of V146's flat unit-centroid scorer. The format stores f16
centroids, but `decode` keeps f32 coordinates and one f32 squared norm per
unit. The proof assumes the implementation evaluates every unit for every
query and that the arrays contain no allocator padding, page cache or other
serving state. It does not establish measured latency, recall or charged RAM.
-/

namespace Borsuk.UnitCentroidScaling

def units (rows unitRows : Nat) : Nat := (rows + unitRows - 1) / unitRows

def scannedCoordinates (rows unitRows dimensions : Nat) : Nat :=
  units rows unitRows * dimensions

def residentArrayBytes (rows unitRows dimensions : Nat) : Nat :=
  units rows unitRows * (4 * dimensions + 4)

def compactBlobBytes (rows unitRows dimensions : Nat) : Nat :=
  32 + units rows unitRows * (2 * dimensions)

theorem units_monotone (rows₁ rows₂ unitRows : Nat)
    (h : rows₁ ≤ rows₂) :
    units rows₁ unitRows ≤ units rows₂ unitRows := by
  unfold units
  have added := Nat.add_le_add_right h unitRows
  exact Nat.div_le_div_right (Nat.sub_le_sub_right added 1)

theorem scanned_coordinates_monotone (rows₁ rows₂ unitRows dimensions : Nat)
    (h : rows₁ ≤ rows₂) :
    scannedCoordinates rows₁ unitRows dimensions ≤
      scannedCoordinates rows₂ unitRows dimensions := by
  simp only [scannedCoordinates]
  exact Nat.mul_le_mul_right dimensions (units_monotone rows₁ rows₂ unitRows h)

theorem resident_array_bytes_monotone (rows₁ rows₂ unitRows dimensions : Nat)
    (h : rows₁ ≤ rows₂) :
    residentArrayBytes rows₁ unitRows dimensions ≤
      residentArrayBytes rows₂ unitRows dimensions := by
  simp only [residentArrayBytes]
  exact Nat.mul_le_mul_right (4 * dimensions + 4)
    (units_monotone rows₁ rows₂ unitRows h)

theorem frozen_geometry_examples :
    scannedCoordinates 100000 32 96 = 300000 ∧
    scannedCoordinates 1000000 32 768 = 24000000 ∧
    residentArrayBytes 100000 32 96 = 1212500 ∧
    residentArrayBytes 1000000 32 768 = 96125000 ∧
    compactBlobBytes 100000 32 96 = 600032 ∧
    compactBlobBytes 1000000 32 768 = 48000032 := by
  decide

theorem hundred_million_payload_examples :
    scannedCoordinates 100000000 32 96 = 300000000 ∧
    scannedCoordinates 100000000 32 768 = 2400000000 ∧
    residentArrayBytes 100000000 32 96 = 1212500000 ∧
    residentArrayBytes 100000000 32 768 = 9612500000 ∧
    compactBlobBytes 100000000 32 96 = 600000032 ∧
    compactBlobBytes 100000000 32 768 = 4800000032 := by
  decide

/-! Conditional performance model: if a verified implementation and
hardware bound certify fixed overhead plus at most `nsPerCoordinate`
nanoseconds per coordinate, a budget at least that large bounds latency.
The premise must be separately measured or justified for the deployment.
-/
theorem conditional_latency_budget
    (rows unitRows dimensions nsPerCoordinate fixedNs elapsedNs budgetNs : Nat)
    (implementationAndHardwareBound :
      elapsedNs ≤ fixedNs + nsPerCoordinate *
        scannedCoordinates rows unitRows dimensions)
    (budgetCoversBound :
      fixedNs + nsPerCoordinate *
        scannedCoordinates rows unitRows dimensions ≤ budgetNs) :
    elapsedNs ≤ budgetNs := by
  exact Nat.le_trans implementationAndHardwareBound budgetCoversBound

end Borsuk.UnitCentroidScaling
