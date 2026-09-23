import Std

/-!
An information requirement for an exact, query-independent resident SQ8
scorer. The model uses unit dequantization and treats
the norm as an observable score at the zero query. Basis-query scores then
reveal every 8-bit coordinate. Any resident state that answers all those
scores exactly must distinguish every distinct SQ8 code vector.

Public nonzero affine steps can be reduced to the unit-step case, but that
reduction is not formalized here. This proves injectivity, not a measured memory footprint or the cardinality
arithmetic needed for a machine-checked 8D-bit fixed-width lower bound.
It does not apply when the required query family is narrower or approximate
scores are allowed.
-/

namespace Borsuk.ResidentExactness

def observation {dimensions : Nat}
    (storedNorm : (Fin dimensions → Fin 256) → Int)
    (code : Fin dimensions → Fin 256) :
    Int × (Fin dimensions → Int) :=
  (storedNorm code,
   fun coordinate => storedNorm code - 2 * Int.ofNat (code coordinate).val)

theorem observation_injective {dimensions : Nat}
    (storedNorm : (Fin dimensions → Fin 256) → Int) :
    Function.Injective (observation storedNorm) := by
  intro left right equalObservation
  have sameNorm : storedNorm left = storedNorm right :=
    congrArg Prod.fst equalObservation
  have sameBasis :
      (fun coordinate => storedNorm left - 2 * Int.ofNat (left coordinate).val) =
      (fun coordinate => storedNorm right - 2 * Int.ofNat (right coordinate).val) :=
    congrArg Prod.snd equalObservation
  funext coordinate
  apply Fin.ext
  have sameCoordinate := congrFun sameBasis coordinate
  dsimp at sameCoordinate
  rw [sameNorm] at sameCoordinate
  omega

theorem exact_scorer_requires_injective_encoding
    {dimensions : Nat} {state : Type}
    (storedNorm : (Fin dimensions → Fin 256) → Int)
    (encode : (Fin dimensions → Fin 256) → state)
    (score : state → Int × (Fin dimensions → Int))
    (exact : ∀ code, score (encode code) = observation storedNorm code) :
    Function.Injective encode := by
  intro left right sameState
  apply observation_injective storedNorm
  calc
    observation storedNorm left = score (encode left) := (exact left).symm
    _ = score (encode right) := by rw [sameState]
    _ = observation storedNorm right := exact right

end Borsuk.ResidentExactness
