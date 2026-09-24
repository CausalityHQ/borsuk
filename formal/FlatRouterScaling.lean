import Std

/-!
Conditional resource bounds for the current flat source-only PQ64 router.
The implementation-to-model refinement and an upper bound on hardware work
rate are premises, not proved here. No recall, real latency, charged RAM peak,
or release capacity follows from these arithmetic statements alone.
-/

namespace Borsuk.FlatRouterScaling

def summaryCoordinates (pages blocksPerPage dimensions : Nat) : Nat :=
  pages * blocksPerPage * dimensions

def summaryPayloadBytes (pages blocksPerPage dimensions : Nat) : Nat :=
  4 * summaryCoordinates pages blocksPerPage dimensions

def pq64PayloadBytes (rows : Nat) : Nat := rows * 64

def routerPayloadBytes (rows pages blocksPerPage dimensions : Nat) : Nat :=
  pq64PayloadBytes rows + summaryPayloadBytes pages blocksPerPage dimensions

def sq8PayloadBytes (rows dimensions : Nat) : Nat := rows * (dimensions + 12)

/-! A full summary scan executes at least one coordinate operation per
summary coordinate. If its execution rate is at most `rate` operations per
time unit, a budget with insufficient capacity cannot contain the scan. -/
theorem flat_summary_scan_exceeds_latency_budget
    (pages blocksPerPage dimensions rate elapsed budget : Nat)
    (fullScanBound :
      summaryCoordinates pages blocksPerPage dimensions ≤ rate * elapsed)
    (budgetTooSmall :
      rate * budget < summaryCoordinates pages blocksPerPage dimensions) :
    budget < elapsed := by
  by_cases exceeds : budget < elapsed
  · exact exceeds
  · have elapsedAtMost : elapsed ≤ budget := Nat.le_of_not_gt exceeds
    have workAtMost := Nat.mul_le_mul_left rate elapsedAtMost
    omega

/-! Per-generation payloads are direct geometry products, with no
vector-count switch. These exclude allocator, metadata, query scratch and
page cache, which must be measured separately. -/
theorem hundred_million_d768_payload :
    summaryPayloadBytes 390625 2 768 = 2400000000 ∧
    pq64PayloadBytes 100000000 = 6400000000 ∧
    routerPayloadBytes 100000000 390625 2 768 = 8800000000 ∧
    sq8PayloadBytes 100000000 768 = 78000000000 := by
  decide

theorem hundred_million_d96_payload :
    summaryPayloadBytes 390625 2 96 = 300000000 ∧
    pq64PayloadBytes 100000000 = 6400000000 ∧
    routerPayloadBytes 100000000 390625 2 96 = 6700000000 ∧
    sq8PayloadBytes 100000000 96 = 10800000000 := by
  decide

theorem two_pinned_d768_payload :
    2 * routerPayloadBytes 100000000 390625 2 768 = 17600000000 ∧
    2 * sq8PayloadBytes 100000000 768 = 156000000000 := by
  decide

end Borsuk.FlatRouterScaling
