import Std

/-!
An exact natural-number model of the OPQ8 group admission loop. The input
ranking and sealed group lengths are parameters; this file does not claim that
Python, floating-point scoring, or S3 reads implement the model.
-/

namespace Borsuk.OPQ8

structure Plan where
  selected : List Nat
  bytes : Nat
  deriving Repr

def admit (length : Nat → Nat) (maxGets maxBytes : Nat)
    (state : Plan) (group : Nat) : Plan :=
  if state.selected.length < maxGets ∧ state.bytes + length group ≤ maxBytes then
    { selected := state.selected ++ [group], bytes := state.bytes + length group }
  else
    state

def plan (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) : Plan :=
  ranked.foldl (admit length maxGets maxBytes) ⟨[], 0⟩

theorem admit_bounds (length : Nat → Nat) (maxGets maxBytes : Nat)
    (state : Plan) (group : Nat)
    (hGets : state.selected.length ≤ maxGets)
    (hBytes : state.bytes ≤ maxBytes) :
    (admit length maxGets maxBytes state group).selected.length ≤ maxGets ∧
    (admit length maxGets maxBytes state group).bytes ≤ maxBytes := by
  by_cases h : state.selected.length < maxGets ∧ state.bytes + length group ≤ maxBytes
  · constructor
    · simpa [admit, h, List.length_append] using
        (show state.selected.length + 1 ≤ maxGets by omega)
    · simp [admit, h]
  · simp only [admit, h]
    exact ⟨hGets, hBytes⟩

theorem fold_bounds (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) (state : Plan)
    (hGets : state.selected.length ≤ maxGets)
    (hBytes : state.bytes ≤ maxBytes) :
    (ranked.foldl (admit length maxGets maxBytes) state).selected.length ≤ maxGets ∧
    (ranked.foldl (admit length maxGets maxBytes) state).bytes ≤ maxBytes := by
  induction ranked generalizing state with
  | nil => exact ⟨hGets, hBytes⟩
  | cons group rest ih =>
      simp only [List.foldl_cons]
      obtain ⟨nextGets, nextBytes⟩ :=
        admit_bounds length maxGets maxBytes state group hGets hBytes
      exact ih (admit length maxGets maxBytes state group) nextGets nextBytes

theorem fold_sublist (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) (state : Plan) :
    (ranked.foldl (admit length maxGets maxBytes) state).selected.Sublist
      (state.selected ++ ranked) := by
  induction ranked generalizing state with
  | nil => simp
  | cons group rest ih =>
      simp only [List.foldl_cons]
      by_cases h : state.selected.length < maxGets ∧
          state.bytes + length group ≤ maxBytes
      · have next := ih (admit length maxGets maxBytes state group)
        simpa [admit, h, List.append_assoc] using next
      · have next := ih (admit length maxGets maxBytes state group)
        have skip : (state.selected ++ rest).Sublist
            (state.selected ++ group :: rest) := by
          exact List.Sublist.append_left (List.Sublist.cons group (by simp)) _
        have next' :
            (rest.foldl (admit length maxGets maxBytes) state).selected.Sublist
              (state.selected ++ rest) := by
          simpa [admit, h] using next
        simpa [admit, h] using next'.trans skip

theorem admit_accounting (length : Nat → Nat) (maxGets maxBytes : Nat)
    (state : Plan) (group : Nat)
    (h : state.bytes = (state.selected.map length).sum) :
    (admit length maxGets maxBytes state group).bytes =
      ((admit length maxGets maxBytes state group).selected.map length).sum := by
  by_cases fits : state.selected.length < maxGets ∧
      state.bytes + length group ≤ maxBytes
  · have target : state.bytes + length group =
        ((state.selected ++ [group]).map length).sum := by
      simp [List.map_append, List.sum_append, h]
    simpa [admit, fits] using target
  · simpa [admit, fits] using h

theorem fold_accounting (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) (state : Plan)
    (h : state.bytes = (state.selected.map length).sum) :
    (ranked.foldl (admit length maxGets maxBytes) state).bytes =
      ((ranked.foldl (admit length maxGets maxBytes) state).selected.map length).sum := by
  induction ranked generalizing state with
  | nil => exact h
  | cons group rest ih =>
      simp only [List.foldl_cons]
      exact ih (admit length maxGets maxBytes state group)
        (admit_accounting length maxGets maxBytes state group h)

theorem plan_bounds (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) :
    (plan length maxGets maxBytes ranked).selected.length ≤ maxGets ∧
    (plan length maxGets maxBytes ranked).bytes ≤ maxBytes := by
  exact fold_bounds length maxGets maxBytes ranked ⟨[], 0⟩
    (by simp) (by simp)

theorem plan_sublist (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) :
    (plan length maxGets maxBytes ranked).selected.Sublist ranked := by
  simpa [plan] using fold_sublist length maxGets maxBytes ranked ⟨[], 0⟩

theorem plan_nodup (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) (h : ranked.Nodup) :
    (plan length maxGets maxBytes ranked).selected.Nodup :=
  (plan_sublist length maxGets maxBytes ranked).nodup h

theorem plan_selection_bound (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) :
    (plan length maxGets maxBytes ranked).selected.length ≤ ranked.length :=
  (plan_sublist length maxGets maxBytes ranked).length_le

theorem plan_accounting (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) :
    (plan length maxGets maxBytes ranked).bytes =
      ((plan length maxGets maxBytes ranked).selected.map length).sum := by
  exact fold_accounting length maxGets maxBytes ranked ⟨[], 0⟩ (by simp)

theorem production_wave_bounds (length : Nat → Nat) (ranked : List Nat) :
    (plan length 32 16777216 ranked).selected.length ≤ 32 ∧
    (plan length 32 16777216 ranked).bytes ≤ 16777216 :=
  plan_bounds length 32 16777216 ranked

/-! Integer scores can represent outward-rounded fixed-point score intervals.
The caller must authenticate and check each interval against the exact
distance calculation; the theorem only proves the implication. -/

theorem margin_preserves_order (trueEarlier trueLater
    routedEarlier routedLater error : Int)
    (earlierUpper : routedEarlier ≤ trueEarlier + error)
    (laterLower : trueLater - error ≤ routedLater)
    (gap : trueEarlier + 2 * error < trueLater) :
    routedEarlier < routedLater := by
  omega

def coveredHits (truthOwners selected : List Nat) : Nat :=
  (truthOwners.filter (fun group => group ∈ selected)).length

theorem certified_recall_lower_bound (truthOwners certified selected : List Nat)
    (positions : certified.Sublist truthOwners)
    (covered : ∀ group ∈ certified, group ∈ selected) :
    certified.length ≤ coveredHits truthOwners selected := by
  have keep : certified.filter (fun group => group ∈ selected) = certified := by
    apply List.filter_eq_self.mpr
    intro group inCertified
    simp [covered group inCertified]
  have sub := positions.filter (fun group => group ∈ selected)
  simpa [coveredHits, keep] using sub.length_le

theorem complete_recall_of_owner_certificate (truthOwners selected : List Nat)
    (h : ∀ group ∈ truthOwners, group ∈ selected) :
    coveredHits truthOwners selected = truthOwners.length := by
  induction truthOwners with
  | nil => simp [coveredHits]
  | cons group rest ih =>
      have head : group ∈ selected := h group (by simp)
      have tail : ∀ x ∈ rest, x ∈ selected := by
        intro x hx
        exact h x (by simp [hx])
      calc
        coveredHits (group :: rest) selected = 1 + coveredHits rest selected := by
          simp [coveredHits, head]
          omega
        _ = (group :: rest).length := by rw [ih tail]; simp; omega

/-! A cohort certificate records only truth positions whose owner has
been verified to lie in the selected groups. It may omit positions for
which the available score intervals are inconclusive. This makes the
bound conservative without requiring every query to certify fully. -/

structure QueryCertificate where
  truthOwners : List Nat
  selected : List Nat
  certified : List Nat

def certifiedCohortHits (cases : List QueryCertificate) : Nat :=
  (cases.map fun item => item.certified.length).sum

def coveredCohortHits (cases : List QueryCertificate) : Nat :=
  (cases.map fun item => coveredHits item.truthOwners item.selected).sum

theorem cohort_certified_recall_lower_bound (cases : List QueryCertificate)
    (valid : ∀ item ∈ cases,
      item.certified.Sublist item.truthOwners ∧
      ∀ group ∈ item.certified, group ∈ item.selected) :
    certifiedCohortHits cases ≤ coveredCohortHits cases := by
  induction cases with
  | nil => simp [certifiedCohortHits, coveredCohortHits]
  | cons item rest ih =>
      have head := valid item (by simp)
      have tail : ∀ next ∈ rest,
          next.certified.Sublist next.truthOwners ∧
          ∀ group ∈ next.certified, group ∈ next.selected := by
        intro next inRest
        exact valid next (by simp [inRest])
      have itemBound := certified_recall_lower_bound
        item.truthOwners item.certified item.selected head.1 head.2
      simpa [certifiedCohortHits, coveredCohortHits] using
        Nat.add_le_add itemBound (ih tail)

theorem cohort_recall_gate_of_certificate (cases : List QueryCertificate)
    (valid : ∀ item ∈ cases,
      item.certified.Sublist item.truthOwners ∧
      ∀ group ∈ item.certified, group ∈ item.selected)
    (certifiedGate : 98151 ≤ certifiedCohortHits cases) :
    98151 ≤ coveredCohortHits cases := by
  exact Nat.le_trans certifiedGate (cohort_certified_recall_lower_bound cases valid)

def codePlaneBytes (rows : Nat) : Nat := 8 * rows

def fullScanTableLookups (rows : Nat) : Nat := 8 * rows

theorem hundred_million_full_scan_lookups :
    fullScanTableLookups 100000000 = 800000000 := by decide

theorem two_full_code_planes (rows : Nat) :
    codePlaneBytes rows + codePlaneBytes rows = 16 * rows := by
  simp [codePlaneBytes]
  omega

theorem hundred_million_code_plane :
    codePlaneBytes 100000000 = 800000000 := by decide

theorem hundred_million_two_code_planes :
    codePlaneBytes 100000000 + codePlaneBytes 100000000 = 1600000000 := by
  decide

/-! This bound assumes two complete code planes are resident at the same
time as the current NumPy scorer's float64 score and advanced-indexing
lookup arrays. It excludes Python, metadata, codebooks and other scratch. -/

def twoGenerationScanPeakBytes (rows : Nat) : Nat :=
  2 * codePlaneBytes rows + 8 * rows + 8 * rows

theorem hundred_million_current_scan_peak :
    twoGenerationScanPeakBytes 100000000 = 3200000000 := by decide

theorem hundred_million_exceeds_margin :
    3 * 1024 ^ 3 - 64 * 1024 ^ 2 <
      twoGenerationScanPeakBytes 100000000 := by decide

/-! At 1M, one GET may cover several adjacent selected groups. The
physical list and predecessor relation are sealed layout inputs. A group
starts a GET exactly when it is selected and its same-role predecessor is
absent. This is an abstract recounting model; proving the Python
incremental `gets + 1 - left - right` implementation refines it remains
separate work. -/

def mergedGets (physical selected : List Nat)
    (predecessor : Nat → Option Nat) : Nat :=
  (physical.filter fun group =>
    decide (group ∈ selected) &&
      match predecessor group with
      | none => true
      | some previous => !decide (previous ∈ selected)).length

def admitMerged (physical : List Nat) (predecessor : Nat → Option Nat)
    (length : Nat → Nat) (maxGets maxBytes : Nat)
    (state : Plan) (group : Nat) : Plan :=
  let selected := state.selected ++ [group]
  let used := state.bytes + length group
  if mergedGets physical selected predecessor ≤ maxGets ∧ used ≤ maxBytes then
    ⟨selected, used⟩
  else
    state

def planMerged (physical : List Nat) (predecessor : Nat → Option Nat)
    (length : Nat → Nat) (maxGets maxBytes : Nat)
    (ranked : List Nat) : Plan :=
  ranked.foldl (admitMerged physical predecessor length maxGets maxBytes) ⟨[], 0⟩

theorem admitMerged_bounds (physical : List Nat) (predecessor : Nat → Option Nat)
    (length : Nat → Nat) (maxGets maxBytes : Nat) (state : Plan) (group : Nat)
    (hGets : mergedGets physical state.selected predecessor ≤ maxGets)
    (hBytes : state.bytes ≤ maxBytes) :
    mergedGets physical
      (admitMerged physical predecessor length maxGets maxBytes state group).selected
      predecessor ≤ maxGets ∧
    (admitMerged physical predecessor length maxGets maxBytes state group).bytes ≤ maxBytes := by
  by_cases h : mergedGets physical (state.selected ++ [group]) predecessor ≤ maxGets ∧
      state.bytes + length group ≤ maxBytes
  · simp [admitMerged, h]
  · simpa [admitMerged, h] using And.intro hGets hBytes

theorem foldMerged_bounds (physical : List Nat) (predecessor : Nat → Option Nat)
    (length : Nat → Nat) (maxGets maxBytes : Nat) (ranked : List Nat) (state : Plan)
    (hGets : mergedGets physical state.selected predecessor ≤ maxGets)
    (hBytes : state.bytes ≤ maxBytes) :
    mergedGets physical
      (ranked.foldl (admitMerged physical predecessor length maxGets maxBytes) state).selected
      predecessor ≤ maxGets ∧
    (ranked.foldl (admitMerged physical predecessor length maxGets maxBytes) state).bytes ≤ maxBytes := by
  induction ranked generalizing state with
  | nil => exact ⟨hGets, hBytes⟩
  | cons group rest ih =>
      simp only [List.foldl_cons]
      obtain ⟨nextGets, nextBytes⟩ :=
        admitMerged_bounds physical predecessor length maxGets maxBytes state group hGets hBytes
      exact ih (admitMerged physical predecessor length maxGets maxBytes state group)
        nextGets nextBytes

theorem merged_wave_bounds (physical : List Nat) (predecessor : Nat → Option Nat)
    (length : Nat → Nat) (ranked : List Nat) :
    mergedGets physical
      (planMerged physical predecessor length 32 16777216 ranked).selected
      predecessor ≤ 32 ∧
    (planMerged physical predecessor length 32 16777216 ranked).bytes ≤ 16777216 := by
  apply foldMerged_bounds physical predecessor length 32 16777216 ranked ⟨[], 0⟩
  · induction physical with
    | nil => simp [mergedGets]
    | cons group rest ih => simpa [mergedGets] using ih
  · simp

/-! A sequential upper bound for the proposed data-range wave. `requestTime`
and `byteTime` are certified upper bounds in one common time unit for each
GET's fixed overhead and each encoded byte, and `localTime` bounds routing,
scoring, decoding and reranking. This theorem does not supply those bounds,
model parallel S3 scheduling, or assert an observed service latency. -/

def waveLatencyBound (localTime requestTime byteTime : Nat) : Nat :=
  localTime + 32 * requestTime + 16777216 * byteTime

theorem bounded_data_wave_latency
    (gets bytes localTime requestTime byteTime observedTime : Nat)
    (getsBound : gets ≤ 32)
    (bytesBound : bytes ≤ 16777216)
    (serviceBound : observedTime ≤ localTime + gets * requestTime + bytes * byteTime) :
    observedTime ≤ waveLatencyBound localTime requestTime byteTime := by
  unfold waveLatencyBound
  have requestBound := Nat.mul_le_mul_right requestTime getsBound
  have transferBound := Nat.mul_le_mul_right byteTime bytesBound
  omega

/-! A conditional sequential ceiling for the proposed code-plus-data
waves. It requires measured upper bounds on request and transfer time in
one common unit, plus a bound on all local work. Parallel execution may
be faster, but is outside this model. -/

def twoWaveLatencyBound
    (localTime codeRequestTime codeByteTime dataRequestTime dataByteTime : Nat) : Nat :=
  localTime + 32 * codeRequestTime + 16777216 * codeByteTime +
    32 * dataRequestTime + 16777216 * dataByteTime

theorem bounded_two_wave_latency
    (codeGets codeBytes dataGets dataBytes localTime
      codeRequestTime codeByteTime dataRequestTime dataByteTime observedTime : Nat)
    (codeGetsBound : codeGets ≤ 32)
    (codeBytesBound : codeBytes ≤ 16777216)
    (dataGetsBound : dataGets ≤ 32)
    (dataBytesBound : dataBytes ≤ 16777216)
    (serviceBound : observedTime ≤ localTime +
      codeGets * codeRequestTime + codeBytes * codeByteTime +
      dataGets * dataRequestTime + dataBytes * dataByteTime) :
    observedTime ≤ twoWaveLatencyBound localTime codeRequestTime codeByteTime
      dataRequestTime dataByteTime := by
  unfold twoWaveLatencyBound
  have codeRequestBound := Nat.mul_le_mul_right codeRequestTime codeGetsBound
  have codeTransferBound := Nat.mul_le_mul_right codeByteTime codeBytesBound
  have dataRequestBound := Nat.mul_le_mul_right dataRequestTime dataGetsBound
  have dataTransferBound := Nat.mul_le_mul_right dataByteTime dataBytesBound
  omega

/-! A progressive sign/magnitude code layout can require two sequential
code waves before the final data wave. This is a ceiling only if the
caller supplies valid service-time bounds for every wave. It does not
assert that three S3 round trips meet a product latency target. -/

def threeWaveLatencyBound
    (localTime signRequestTime signByteTime magnitudeRequestTime
      magnitudeByteTime dataRequestTime dataByteTime : Nat) : Nat :=
  localTime + 32 * signRequestTime + 16777216 * signByteTime +
    32 * magnitudeRequestTime + 16777216 * magnitudeByteTime +
    32 * dataRequestTime + 16777216 * dataByteTime

theorem bounded_three_wave_latency
    (signGets signBytes magnitudeGets magnitudeBytes dataGets dataBytes
      localTime signRequestTime signByteTime magnitudeRequestTime
      magnitudeByteTime dataRequestTime dataByteTime observedTime : Nat)
    (signGetsBound : signGets ≤ 32)
    (signBytesBound : signBytes ≤ 16777216)
    (magnitudeGetsBound : magnitudeGets ≤ 32)
    (magnitudeBytesBound : magnitudeBytes ≤ 16777216)
    (dataGetsBound : dataGets ≤ 32)
    (dataBytesBound : dataBytes ≤ 16777216)
    (serviceBound : observedTime ≤ localTime +
      signGets * signRequestTime + signBytes * signByteTime +
      magnitudeGets * magnitudeRequestTime + magnitudeBytes * magnitudeByteTime +
      dataGets * dataRequestTime + dataBytes * dataByteTime) :
    observedTime ≤ threeWaveLatencyBound localTime signRequestTime signByteTime
      magnitudeRequestTime magnitudeByteTime dataRequestTime dataByteTime := by
  unfold threeWaveLatencyBound
  have signRequestBound := Nat.mul_le_mul_right signRequestTime signGetsBound
  have signTransferBound := Nat.mul_le_mul_right signByteTime signBytesBound
  have magnitudeRequestBound := Nat.mul_le_mul_right magnitudeRequestTime magnitudeGetsBound
  have magnitudeTransferBound := Nat.mul_le_mul_right magnitudeByteTime magnitudeBytesBound
  have dataRequestBound := Nat.mul_le_mul_right dataRequestTime dataGetsBound
  have dataTransferBound := Nat.mul_le_mul_right dataByteTime dataBytesBound
  omega

def regionTableLookups (visitedRows : Nat) : Nat := 8 * visitedRows

theorem region_lookup_bound (visitedRows regionCap : Nat)
    (visitedBound : visitedRows ≤ regionCap) :
    regionTableLookups visitedRows ≤ 8 * regionCap := by
  unfold regionTableLookups
  omega

private def threeGroupPredecessor : Nat → Option Nat
  | 0 => none
  | group + 1 => some group

example :
    (planMerged [0, 1, 2] threeGroupPredecessor (fun _ => 100)
      2 300 [0, 2, 1]).selected = [0, 2, 1] ∧
    mergedGets [0, 1, 2]
      (planMerged [0, 1, 2] threeGroupPredecessor (fun _ => 100)
        2 300 [0, 2, 1]).selected threeGroupPredecessor = 1 := by
  decide

end Borsuk.OPQ8
