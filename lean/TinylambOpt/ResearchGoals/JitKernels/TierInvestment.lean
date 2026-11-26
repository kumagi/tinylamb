import Std

/-! Tier-up decisions charged in work units for compilation, successful executions, failed guards and scalar replay. A trace contains observations per batch. No independence, stationary distribution or clock model is assumed. -/
namespace TinylambOpt.ResearchGoals.JitKernels.TierInvestment

structure Batch where
  rows : Nat
  passes : Bool
  deriving Repr, DecidableEq
structure Tariff where
  compile : Nat
  guardFee : Nat
  slow : Nat
  fast : Nat
  replay : Nat
def scalar (t : Tariff) (b : Batch) := t.slow*b.rows
def compiled (t : Tariff) (b : Batch) :=
  t.guardFee + (if b.passes then t.fast*b.rows else t.replay + t.slow*b.rows)
def bill (t : Tariff) (xs : List Batch) (cut : Nat) :=
  ((xs.take cut).map (scalar t)).sum + (if cut < xs.length then t.compile else 0) +
    ((xs.drop cut).map (compiled t)).sum
def net (t : Tariff) (b : Batch) : Int := (scalar t b : Int) - compiled t b

/-- Pay for a compiled suffix exactly when its realized savings cover one compilation. -/
def SuffixReturnTierUp.Goal : Prop := ∀ t xs cut, cut < xs.length →
  (bill t xs cut ≤ bill t xs xs.length ↔ (t.compile:Int) ≤ ((xs.drop cut).map (net t)).sum)

/-- Two profiles with the same row total and successful batch count imply the same compilation decision. -/
def GuardRateOnlyAdmission.Goal : Prop := ∀ t (a b : List Batch), a.length = b.length → (a.map Batch.rows).sum = (b.map Batch.rows).sum →
  a.countP Batch.passes = b.countP Batch.passes → bill t a 0 = bill t b 0

/-- Once an initial prefix repays compilation, continued use of the compiled version cannot lose over the whole trace. -/
def ProfitablePrefixLocksTier.Goal : Prop := ∀ t a b, bill t a 0 < bill t a a.length → bill t (a++b) 0 ≤ bill t (a++b) (a++b).length

/-- Moving compilation by one batch depends only on that batch when both schedules still compile. -/
def AdjacentTierCutDifference.Goal : Prop := ∀ t xs i b, i+1 < xs.length → xs[i]? = some b →
  (bill t xs (i+1):Int) - bill t xs i = net t b

/-- Select the most profitable nonempty suffix to minimize total work among schedules that compile exactly once. -/
def SpecializeAtMaximumSuffixGain.Goal : Prop := ∀ t xs i j, i < xs.length → j < xs.length →
  (bill t xs i ≤ bill t xs j ↔ ((xs.drop j).map (net t)).sum ≤ ((xs.drop i).map (net t)).sum)

/-- A majority of rows passing the guard guarantees tier-up pays for a long-running loop. -/
def CompileIfHotRowsDominate.Goal : Prop := ∀ t xs, t.fast < t.slow → t.compile ≤ (xs.map Batch.rows).sum →
  (xs.filter Batch.passes |>.map Batch.rows).sum * 2 > (xs.map Batch.rows).sum → bill t xs 0 ≤ bill t xs xs.length

/-- A single joint dispatch for two batches with identical guard outcomes never costs more than two dispatches. -/
def SameShapeBatchFusion.Goal : Prop := ∀ t a b, a.passes = b.passes → compiled t ⟨a.rows+b.rows,a.passes⟩ ≤ compiled t a + compiled t b

/-- Treat a fused batch as generic on any guard miss; saved dispatch overhead always offsets lost specialization. -/
def MixedShapeBatchFusion.Goal : Prop := ∀ t a b, t.fast ≤ t.slow → compiled t ⟨a.rows+b.rows,a.passes && b.passes⟩ ≤ compiled t a + compiled t b

/-- For a stable repeating trace with positive net savings, some repetition count amortizes any finite compilation cost. -/
def CompiledSuffixDuplication.Goal : Prop := ∀ t xs, 0 < (xs.map (net t)).sum → ∃ n, let ys := (List.replicate n xs).flatten
  bill t ys 0 < bill t ys ys.length

/-- Bounding the number of deopts also bounds excess work over the interpreter independently of the number of tiny successful batches. -/
def FailuresOnlyBoundRecompileCost.Goal : Prop := ∀ t xs, t.fast ≤ t.slow → bill t xs 0 ≤ bill t xs xs.length + t.compile +
  (xs.countP (fun b => !b.passes))*(t.replay+t.guardFee)

end TinylambOpt.ResearchGoals.JitKernels.TierInvestment
