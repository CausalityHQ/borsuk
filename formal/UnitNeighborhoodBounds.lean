import Std

/-!
Conditional work bounds for a radius-w neighborhood of nominated physical
units. A duplicate-free union can never have more than (2w+1) units per
nominee. The caller must prove the actual unit count satisfies that premise;
this file does not prove empirical recall, I/O cost, latency or charged RAM.
-/

namespace Borsuk.UnitNeighborhoodBounds

def inNeighborhood (base : List Nat) (radius unitCount unit : Nat) : Prop :=
  unit < unitCount ∧
    ∃ nominated ∈ base,
      unit ≤ nominated + radius ∧ nominated ≤ unit + radius

theorem neighborhood_monotone
    (base : List Nat) (narrow wide unitCount unit : Nat)
    (ordered : narrow ≤ wide)
    (included : inNeighborhood base narrow unitCount unit) :
    inNeighborhood base wide unitCount unit := by
  obtain ⟨inRange, nominated, inBase, left, right⟩ := included
  exact ⟨inRange, nominated, inBase, by omega, by omega⟩

def unitCap (nominees radius : Nat) : Nat :=
  nominees * (2 * radius + 1)

def rowCap (nominees radius unitRows : Nat) : Nat :=
  unitCap nominees radius * unitRows

def lookupCap (nominees radius unitRows subspaces : Nat) : Nat :=
  rowCap nominees radius unitRows * subspaces

theorem lookup_bound
    (nominees radius unitRows subspaces units rows lookups : Nat)
    (unitsBound : units ≤ unitCap nominees radius)
    (rowsBound : rows ≤ units * unitRows)
    (lookupsBound : lookups ≤ rows * subspaces) :
    lookups ≤ lookupCap nominees radius unitRows subspaces := by
  have rowsLe : rows ≤ rowCap nominees radius unitRows := by
    exact Nat.le_trans rowsBound
      (Nat.mul_le_mul_right unitRows unitsBound)
  exact Nat.le_trans lookupsBound
    (Nat.mul_le_mul_right subspaces rowsLe)

theorem unit_cap_monotone_radius
    (nominees narrow wide : Nat) (ordered : narrow ≤ wide) :
    unitCap nominees narrow ≤ unitCap nominees wide := by
  unfold unitCap
  exact Nat.mul_le_mul_left nominees (by omega)

theorem frozen_width_eight_geometry :
    unitCap 512 8 = 8704 ∧
    rowCap 512 8 32 = 278528 ∧
    lookupCap 512 8 32 64 = 17825792 := by
  decide

end Borsuk.UnitNeighborhoodBounds
