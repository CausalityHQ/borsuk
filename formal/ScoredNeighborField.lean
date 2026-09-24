import Std

/-!
Conditional work and code-payload bounds for the V168 nominee-neighbor PQ64
field. The Python implementation must supply the authenticated nominee count,
32-row unit geometry, and actual visited-row counters. These theorems do not
prove that PQ scores capture neighbors, that the router finds the nominees,
or that hardware meets a latency target.
-/

namespace Borsuk.ScoredNeighborField

def candidateUnitCap (nominees : Nat) : Nat := 3 * nominees
def candidateRowCap (nominees unitRows : Nat) : Nat :=
  candidateUnitCap nominees * unitRows
def lookupCap (nominees unitRows subspaces : Nat) : Nat :=
  candidateRowCap nominees unitRows * subspaces

theorem candidate_lookup_bound
    (nominees units rows lookups unitRows subspaces : Nat)
    (unitsBound : units ≤ candidateUnitCap nominees)
    (rowsBound : rows ≤ units * unitRows)
    (lookupsBound : lookups ≤ rows * subspaces) :
    lookups ≤ lookupCap nominees unitRows subspaces := by
  have rowsLe : rows ≤ candidateRowCap nominees unitRows := by
    exact Nat.le_trans rowsBound
      (Nat.mul_le_mul_right unitRows unitsBound)
  exact Nat.le_trans lookupsBound
    (Nat.mul_le_mul_right subspaces rowsLe)

theorem frozen_512_nominee_lookup_cap :
    candidateUnitCap 512 = 1536 ∧
    candidateRowCap 512 32 = 49152 ∧
    lookupCap 512 32 64 = 3145728 := by
  decide

def codePayloadBytes (rows codeBytes generations : Nat) : Nat :=
  rows * codeBytes * generations

theorem code_payload_monotone_rows
    (rows₁ rows₂ codeBytes generations : Nat)
    (ordered : rows₁ ≤ rows₂) :
    codePayloadBytes rows₁ codeBytes generations ≤
      codePayloadBytes rows₂ codeBytes generations := by
  exact Nat.mul_le_mul_right generations
    (Nat.mul_le_mul_right codeBytes ordered)

theorem hundred_million_pq64_payload :
    codePayloadBytes 100000000 64 1 = 6400000000 ∧
    codePayloadBytes 100000000 64 2 = 12800000000 := by
  decide

end Borsuk.ScoredNeighborField
