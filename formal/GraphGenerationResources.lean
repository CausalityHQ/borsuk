import Std

/-!
Conditional arithmetic model of `GraphResourceGeometry::estimate` for the
current graph generation. All sizes are bytes. The loader must authenticate
the geometry, hashes and graph, and a Rust refinement check must show that
its allocations follow this model. These theorems do not establish useful
graph neighbors, recall, measured latency, allocator/RSS or charged memory.
-/

namespace Borsuk.GraphGenerationResources

structure Geometry where
  rows : Nat
  dimensions : Nat
  pageRows : Nat
  unitRows : Nat
  blocksPerPage : Nat
  sourceVerificationBlockBytes : Nat
  graphEncodedBytes : Nat
  graphResidentBytes : Nat
  maxActiveQueries : Nat
  transientBytesPerQuery : Nat
  alreadyPinnedBytes : Nat

def ceilDiv (n d : Nat) : Nat := (n + d - 1) / d
def pages (g : Geometry) : Nat := ceilDiv g.rows g.pageRows
def units (g : Geometry) : Nat := ceilDiv g.rows g.unitRows
def blocks (g : Geometry) : Nat := pages g * g.blocksPerPage
def routerCodeBytes (g : Geometry) : Nat := g.rows * 64
def routerSummaryBytes (g : Geometry) : Nat := blocks g * g.dimensions * 4
def routerNormBytes (g : Geometry) : Nat := blocks g * 4
def routerBookBytes (g : Geometry) : Nat := 64 * 256 * ceilDiv g.dimensions 64 * 4
def affineBytes (g : Geometry) : Nat := g.dimensions * 2 * 4
def decodedCentroidBytes (g : Geometry) : Nat := units g * (g.dimensions * 4 + 4)
def encodedCentroidBytes (g : Geometry) : Nat := 32 + units g * g.dimensions * 2
def sourceBytes (g : Geometry) : Nat := 64 + g.rows * (8 + g.dimensions * 4)
def sourceDigestBytes (g : Geometry) : Nat :=
  ceilDiv (sourceBytes g) g.sourceVerificationBlockBytes * 32
def mapBytes (g : Geometry) : Nat := g.rows * 16
def pageDigestBytes (g : Geometry) : Nat := pages g * 32

def steadyPayloadBytes (g : Geometry) : Nat :=
  routerCodeBytes g + routerSummaryBytes g + routerNormBytes g +
  routerBookBytes g + affineBytes g + decodedCentroidBytes g +
  sourceDigestBytes g + mapBytes g + pageDigestBytes g +
  g.graphResidentBytes

def hydrationPeakBytes (g : Geometry) : Nat :=
  steadyPayloadBytes g + encodedCentroidBytes g + g.graphEncodedBytes +
  routerSummaryBytes g + routerBookBytes g + affineBytes g +
  ceilDiv g.rows 8 + g.sourceVerificationBlockBytes + pageDigestBytes g

def transientLimitBytes (g : Geometry) : Nat :=
  g.maxActiveQueries * g.transientBytesPerQuery

def totalPeakBytes (g : Geometry) : Nat :=
  hydrationPeakBytes g + transientLimitBytes g + g.alreadyPinnedBytes

/-! The graph preflight scan must establish `actualGraphBytes ≤
graphResidentBytes` before decode. This is the bridge from the serialized
adjacency to the declaration; the theorem itself does not authenticate it. -/
def actualSteadyPayloadBytes (g : Geometry) (actualGraphBytes : Nat) : Nat :=
  routerCodeBytes g + routerSummaryBytes g + routerNormBytes g +
  routerBookBytes g + affineBytes g + decodedCentroidBytes g +
  sourceDigestBytes g + mapBytes g + pageDigestBytes g + actualGraphBytes

theorem declared_graph_bounds_actual_steady
    (g : Geometry) (actualGraphBytes : Nat)
    (graphPreflight : actualGraphBytes ≤ g.graphResidentBytes) :
    actualSteadyPayloadBytes g actualGraphBytes ≤ steadyPayloadBytes g := by
  unfold actualSteadyPayloadBytes steadyPayloadBytes
  omega

theorem admitted_peak_covers_steady
    (g : Geometry) (maxModeledBytes : Nat)
    (admission : totalPeakBytes g ≤ maxModeledBytes) :
    steadyPayloadBytes g ≤ maxModeledBytes := by
  unfold totalPeakBytes hydrationPeakBytes at admission
  omega

theorem higher_concurrency_never_reduces_peak
    (g : Geometry) (moreQueries : Nat)
    (more : g.maxActiveQueries ≤ moreQueries) :
    totalPeakBytes g ≤ totalPeakBytes { g with maxActiveQueries := moreQueries } := by
  have h := Nat.mul_le_mul_right g.transientBytesPerQuery more
  cases g
  simp only [totalPeakBytes, transientLimitBytes, hydrationPeakBytes,
    steadyPayloadBytes, routerCodeBytes, routerSummaryBytes, routerNormBytes,
    routerBookBytes, affineBytes, decodedCentroidBytes, sourceDigestBytes,
    mapBytes, pageDigestBytes, encodedCentroidBytes, blocks, pages, units,
    sourceBytes, ceilDiv] at *
  omega

theorem higher_graph_allowance_never_reduces_peak
    (g : Geometry) (moreGraphBytes : Nat)
    (more : g.graphResidentBytes ≤ moreGraphBytes) :
    totalPeakBytes g ≤ totalPeakBytes { g with graphResidentBytes := moreGraphBytes } := by
  cases g
  simp [totalPeakBytes, hydrationPeakBytes, steadyPayloadBytes,
    routerCodeBytes, routerSummaryBytes, routerNormBytes, routerBookBytes,
    affineBytes, decodedCentroidBytes, sourceDigestBytes, mapBytes,
    pageDigestBytes, encodedCentroidBytes, transientLimitBytes, blocks,
    pages, units, sourceBytes, ceilDiv] at *
  omega

def hundredMillionD768 : Geometry := {
  rows := 100000000
  dimensions := 768
  pageRows := 256
  unitRows := 32
  blocksPerPage := 2
  sourceVerificationBlockBytes := 1048576
  graphEncodedBytes := 426576800
  graphResidentBytes := 800000000
  maxActiveQueries := 8
  transientBytesPerQuery := 33554432
  alreadyPinnedBytes := 0
}

theorem hundred_million_d768_payload_example :
    routerCodeBytes hundredMillionD768 = 6400000000 ∧
    routerSummaryBytes hundredMillionD768 = 2400000000 ∧
    decodedCentroidBytes hundredMillionD768 = 9612500000 ∧
    sourceDigestBytes hundredMillionD768 = 9399424 ∧
    transientLimitBytes hundredMillionD768 = 268435456 ∧
    steadyPayloadBytes hundredMillionD768 = 20838317000 ∧
    hydrationPeakBytes hundredMillionD768 = 28491734984 ∧
    totalPeakBytes hundredMillionD768 = 28760170440 := by
  decide

end Borsuk.GraphGenerationResources
