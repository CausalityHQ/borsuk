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

/-! V112 gives one primary vote to each of the 100 precise SQ8 nominees
and one secondary vote to every other nominated row. Since there are at
most 512 secondary votes over the entire query, a factor of 513 makes
maximizing the encoded sum lexicographically maximize primary votes. -/

def encodedVotes (primary secondary : Nat) : Nat :=
  513 * primary + secondary

theorem one_primary_vote_dominates_all_secondary_votes
    (primaryA primaryB secondaryA secondaryB : Nat)
    (betterPrimary : primaryB < primaryA)
    (secondaryBudget : secondaryB ≤ 512) :
    encodedVotes primaryB secondaryB < encodedVotes primaryA secondaryA := by
  simp only [encodedVotes]
  omega

/-! The vote factor is a function of the admitted secondary roster size,
not of the corpus or embedding family. The fixed V112 factor 513 is its
specialization to a shortlist of at most 512 rows. -/

def genericEncodedVotes (secondaryBudget primary secondary : Nat) : Nat :=
  (secondaryBudget + 1) * primary + secondary

theorem one_primary_dominates_any_bounded_secondary_roster
    (secondaryBudget primaryA primaryB secondaryA secondaryB : Nat)
    (betterPrimary : primaryB < primaryA)
    (boundedSecondary : secondaryB ≤ secondaryBudget) :
    genericEncodedVotes secondaryBudget primaryB secondaryB <
      genericEncodedVotes secondaryBudget primaryA secondaryA := by
  have nextPrimary : primaryB + 1 ≤ primaryA := by omega
  have weightedNext := Nat.mul_le_mul_left (secondaryBudget + 1) nextPrimary
  simp only [Nat.mul_succ] at weightedNext
  simp only [genericEncodedVotes]
  omega

/-! An arbitrary short final page can use rounded-up units in the planner.
The physical final bytes are no larger than their charged units, so the
physical payload respects any cap proved for the charged units. This is
independent of dataset, row count and embedding dimension. -/

theorem partial_final_page_respects_charged_byte_cap
    (fullPages fullPageUnits finalUnits unitBytes finalBytes maxBytes : Nat)
    (tailBound : finalBytes ≤ finalUnits * unitBytes)
    (chargedCap :
      (fullPages * fullPageUnits + finalUnits) * unitBytes ≤ maxBytes) :
    fullPages * (fullPageUnits * unitBytes) + finalBytes ≤ maxBytes := by
  calc
    fullPages * (fullPageUnits * unitBytes) + finalBytes
        ≤ fullPages * (fullPageUnits * unitBytes) + finalUnits * unitBytes :=
          Nat.add_le_add_left tailBound _
    _ = (fullPages * fullPageUnits + finalUnits) * unitBytes := by
      simp [Nat.add_mul, Nat.mul_assoc]
    _ ≤ maxBytes := chargedCap

/-! The production planner may return the convex hull of all positive-vote
pages without dynamic programming when its charged units fit the cap. This
proves the geometric and lexicographic argument conditional on: every
full-score plan covers both extreme positive pages; a nonempty plan uses a
request; and one-request plans use at least the hull's charged units. The
implementation-to-model connection and the actual positive-page roster are
separate authenticated premises. -/

theorem interval_containing_extremes_cannot_be_shorter
    (first last start stop : Nat)
    (startBefore : start ≤ first)
    (stopAfter : last ≤ stop)
    (ordered : first ≤ last) :
    last - first ≤ stop - start := by
  omega

def beatsLexicographically
    (score requests units otherScore otherRequests otherUnits : Nat) : Prop :=
  otherScore < score ∨
    (score = otherScore ∧
      (requests < otherRequests ∨
        (requests = otherRequests ∧ units < otherUnits)))

theorem affordable_full_vote_hull_is_lex_optimal
    (totalVotes hullUnits maxUnits maxRequests
      otherVotes otherRequests otherUnits : Nat)
    (hullFits : hullUnits ≤ maxUnits)
    (requestFits : 1 ≤ maxRequests)
    (scoreBound : otherVotes ≤ totalVotes)
    (fullScoreNeedsRequest : otherVotes = totalVotes → 1 ≤ otherRequests)
    (oneRequestNeedsHull : otherVotes = totalVotes →
      otherRequests = 1 → hullUnits ≤ otherUnits) :
    (1 ≤ maxRequests ∧ hullUnits ≤ maxUnits) ∧
    ¬ beatsLexicographically otherVotes otherRequests otherUnits
        totalVotes 1 hullUnits := by
  constructor
  · exact ⟨requestFits, hullFits⟩
  intro better
  rcases better with lowerScore | ⟨sameScore, fewerRequests | ⟨sameRequests, fewerUnits⟩⟩
  · omega
  · have atLeastOne := fullScoreNeedsRequest sameScore
    omega
  · have hullMinimal := oneRequestNeedsHull sameScore sameRequests
    omega

end Borsuk.PhysicalIntervalBudget
