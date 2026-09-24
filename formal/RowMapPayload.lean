import Std

/-!
Payload accounting for the authenticated physical row permutation.
One generation owns two `u32` vectors of `N` entries. This excludes
allocator capacity, artifact bytes, router memory, concurrent queries,
and memory held by other generations. It proves no empirical latency or
maximum supported corpus size.
-/

namespace Borsuk.RowMapPayload

def residentBytes (rows generations : Nat) : Nat :=
  8 * rows * generations

theorem one_generation_payload (rows : Nat) :
    residentBytes rows 1 = 8 * rows := by
  simp [residentBytes]

theorem payload_monotone_in_rows
    (rows1 rows2 generations : Nat) (order : rows1 ≤ rows2) :
    residentBytes rows1 generations ≤ residentBytes rows2 generations := by
  unfold residentBytes
  exact Nat.mul_le_mul_right generations (Nat.mul_le_mul_left 8 order)

theorem payload_monotone_in_generations
    (rows generations1 generations2 : Nat)
    (order : generations1 ≤ generations2) :
    residentBytes rows generations1 ≤ residentBytes rows generations2 := by
  unfold residentBytes
  exact Nat.mul_le_mul_left (8 * rows) order

theorem hundred_million_one_generation :
    residentBytes 100000000 1 = 800000000 := by
  decide

theorem hundred_million_two_generations :
    residentBytes 100000000 2 = 1600000000 := by
  decide

end Borsuk.RowMapPayload
