import Std

/-! Binary64 kernels compared by observable bits. Finite-value hypotheses exclude infinities and NaNs but retain subnormals and signed zeros. Fused means a changed reduction order here; no hardware FMA is assumed. -/
namespace TinylambOpt.ResearchGoals.JitKernels.FloatingReduction

def bits (x : Float) := x.toBits
def same (a b : Float) := bits a = bits b
def finite (x : Float) := x.isFinite
def total (s : Float) (xs : List Float) := xs.foldl (·+·) s
def live (xs : List (Option Float)) := xs.filterMap id
def firstInf (s : Float) (xs : List Float) := (xs.scanl (·+·) s).findIdx? Float.isInf

/-- Two finite partial accumulators can be reduced horizontally with bit-identical scalar results. -/
def FinitePairwiseAccumulators.Goal : Prop := ∀ a b c, finite a → finite b → finite c → same ((a+b)+c) (a+(b+c))

/-- Replace masked-off vector lanes by positive zero before an ordered addition. -/
def AbsentLaneAsPositiveZero.Goal : Prop := ∀ s xs, finite s → same (total s (xs.map (fun v => v.getD 0))) (total s (live xs))

/-- For every finite payload use multiplication by zero to produce the canonical inactive-lane bit pattern. -/
def MultiplyMaskInsteadOfSelect.Goal : Prop := ∀ x, finite x → same (x*0) 0

/-- The final infinity bit suffices for the sticky overflow flag when all input rows were finite. -/
def FirstInfinityFromFinalAccumulator.Goal : Prop := ∀ xs s, finite s → (∀ x ∈ xs, finite x) → (firstInf s xs).isSome = (total s xs).isInf

/-- A finite scalar prefix may be skipped when locating the first infinity on a delayed cold path. -/
def FiniteOverflowLaneReplay.Goal : Prop := ∀ s a b, firstInf s a = none →
  firstInf s (a++b) = (firstInf (total s a) b).map (a.length+·)

/-- Normalizing signed zeros before hashing yields congruent bits for all IEEE-equal non-NaN values. -/
def CanonicalZeroHashJoin.Goal : Prop := ∀ a b : Float, a == b → bits (if a == 0 then 0 else a) = bits (if b == 0 then 0 else b)

/-- Hoist the division in AVG to a precomputed reciprocal for all finite sums and nonzero 64-bit row counts. -/
def GroupedMeanReciprocal.Goal : Prop := ∀ (s : Float) (count : UInt64), finite s → count ≠ 0 →
  same (s/count.toFloat) (s*(1/count.toFloat))

/-- Same-sign finite values allow a parallel floating sum to reassociate without changing the result. -/
def SameSignReassociation.Goal : Prop := ∀ a b c : Float, finite a → finite b → finite c → 0 < a → 0 < b → 0 < c → same ((a+b)+c) (a+(b+c))

/-- Distribute a factor of two into both addends when operands and both scaled addends are finite normal-range values. -/
def ExactBinaryPowerScale.Goal : Prop := ∀ a b : Float, finite a → finite b → finite (2*a) → finite (2*b) →
  1 ≤ Float.abs a → 1 ≤ Float.abs b → same (2*(a+b)) (2*a+2*b)

/-- Use raw unsigned binary64 bits for sorted scans over finite strictly positive values. -/
def NumericCompareAsSignedBitKey.Goal : Prop := ∀ a b : Float, finite a → finite b → 0 < a → 0 < b → (a < b ↔ (bits a).toNat < (bits b).toNat)

/-- Replace a left-biased MIN update by a right-biased select when both compared values are finite. -/
def MinMaxTieKeepsLeftBits.Goal : Prop := ∀ a b : Float, finite a → finite b → same (if a ≤ b then a else b) (if a < b then a else b)

end TinylambOpt.ResearchGoals.JitKernels.FloatingReduction
