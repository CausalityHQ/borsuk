import Std

/-!
Conditional accounting for a physical primary-run cover.
An external authenticated layout supplies the distinct primary-unit count,
disconnected run count, and a lower bound on the sum of gaps that any cover
must bridge to meet a GET cap. Theorems here do not infer these facts from
the data, prove the Python gap-sort implementation, or establish recall.
-/

namespace Borsuk.PrimaryRunBudget

theorem bridges_needed_for_get_cap
    (runs gets bridgedGaps maxGets : Nat)
    (runAccounting : runs ≤ gets + bridgedGaps)
    (getCap : gets ≤ maxGets) :
    runs ≤ maxGets + bridgedGaps := by
  omega

theorem mandatory_bytes_floor
    (primaryUnits bridgedUnits fetchedUnits unitBytes bytes : Nat)
    (coverage : primaryUnits + bridgedUnits ≤ fetchedUnits)
    (byteAccounting : bytes = fetchedUnits * unitBytes) :
    (primaryUnits + bridgedUnits) * unitBytes ≤ bytes := by
  calc
    (primaryUnits + bridgedUnits) * unitBytes
        ≤ fetchedUnits * unitBytes := Nat.mul_le_mul_right unitBytes coverage
    _ = bytes := byteAccounting.symm

theorem cannot_meet_byte_cap_of_gap_certificate
    (primaryUnits bridgedUnits fetchedUnits unitBytes bytes cap
      certifiedGapFloor : Nat)
    (gapFloor : certifiedGapFloor ≤ bridgedUnits)
    (coverage : primaryUnits + bridgedUnits ≤ fetchedUnits)
    (byteAccounting : bytes = fetchedUnits * unitBytes)
    (tooLarge : cap < (primaryUnits + certifiedGapFloor) * unitBytes) :
    cap < bytes := by
  have gapCover : primaryUnits + certifiedGapFloor ≤ fetchedUnits := by
    omega
  have byteFloor := mandatory_bytes_floor primaryUnits certifiedGapFloor
    fetchedUnits unitBytes bytes gapCover byteAccounting
  omega

theorem v171_query898_three_get_byte_conflict
    (bridgedUnits fetchedUnits bytes : Nat)
    (gapCertificate : 3894 ≤ bridgedUnits)
    (coverage : 26 + bridgedUnits ≤ fetchedUnits)
    (byteAccounting : bytes = fetchedUnits * 24960) :
    3394560 < bytes ∧ 16777216 < bytes := by
  have coverFloor : 3920 ≤ fetchedUnits := by omega
  have byteFloor : 3920 * 24960 ≤ bytes := by
    calc
      3920 * 24960 ≤ fetchedUnits * 24960 :=
        Nat.mul_le_mul_right 24960 coverFloor
      _ = bytes := byteAccounting.symm
  constructor <;> omega

theorem v171_query898_nine_run_primary_witness :
    9 ≤ 32 ∧ 26 * 24960 = 648960 ∧ 648960 ≤ 16777216 := by
  decide

theorem v171_mandatory_aggregate_arithmetic :
    17678 - 3 + 9 = 17684 ∧
    1196008320 - 97843200 + 648960 = 1098814080 ∧
    17684 ≤ 22126 ∧ 1098814080 ≤ 11134007040 := by
  decide

theorem v178_query653_32_get_byte_conflict
    (bridgedUnits fetchedUnits bytes : Nat)
    (gapCertificate : 752 ≤ bridgedUnits)
    (coverage : 71 + bridgedUnits ≤ fetchedUnits)
    (byteAccounting : bytes = fetchedUnits * 24960) :
    16777216 < bytes := by
  have byteFloor := cannot_meet_byte_cap_of_gap_certificate
    71 bridgedUnits fetchedUnits 24960 bytes 16777216 752
    gapCertificate coverage byteAccounting (by decide)
  exact byteFloor

theorem v178_query653_floor_arithmetic :
    71 + 752 = 823 ∧ 823 * 24960 = 20542080 ∧
    16777216 < 20542080 := by
  decide

end Borsuk.PrimaryRunBudget
