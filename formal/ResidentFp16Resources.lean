import Std

/-!
Format-v1 resident FP16 payload arithmetic. These are exact model bytes,
not measured RSS, latency, source quality or a proof that the Rust loader
refines the model. Those facts need separate authenticated evidence.
-/

namespace Borsuk.ResidentFp16Resources

def rowBytes (dimensions : Nat) : Nat := 8 + 2 * dimensions

def planeBytes (rows dimensions : Nat) : Nat :=
  64 + rows * rowBytes dimensions

def generationBytes (rows dimensions generations : Nat) : Nat :=
  generations * planeBytes rows dimensions

theorem plane_bytes_monotone_rows
    (rows smallOrEqual dimensions : Nat)
    (bound : rows ≤ smallOrEqual) :
    planeBytes rows dimensions ≤ planeBytes smallOrEqual dimensions := by
  have scaled := Nat.mul_le_mul_right (rowBytes dimensions) bound
  simp only [planeBytes]
  omega

theorem generation_bytes_monotone_count
    (rows dimensions generations larger : Nat)
    (bound : generations ≤ larger) :
    generationBytes rows dimensions generations ≤
      generationBytes rows dimensions larger := by
  have scaled := Nat.mul_le_mul_right (planeBytes rows dimensions) bound
  simpa [generationBytes] using scaled

/-! Admission requires a verified bound on allocator, router, delta and
workspace charges. The premise must be checked in the serving process. -/
theorem admitted_rss_under_measured_overhead_bound
    (rows dimensions generations overhead rss budget : Nat)
    (refinement : rss ≤ generationBytes rows dimensions generations + overhead)
    (admission : generationBytes rows dimensions generations + overhead ≤ budget) :
    rss ≤ budget := by
  omega

theorem one_million_d768 :
    planeBytes 1000000 768 = 1544000064 := by
  decide

theorem hundred_million_d768 :
    planeBytes 100000000 768 = 154400000064 := by
  decide

theorem dual_hundred_million_d768 :
    generationBytes 100000000 768 2 = 308800000128 := by
  decide

end Borsuk.ResidentFp16Resources
