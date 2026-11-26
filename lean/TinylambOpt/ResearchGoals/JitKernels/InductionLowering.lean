import Std

/-! Strength-reduced loop nests for affine and quadratic SQL projections. Widened mathematical values, narrow address arithmetic and the iteration coordinate are separate; a selection vector need not be contiguous. -/
namespace TinylambOpt.ResearchGoals.JitKernels.InductionLowering

def poly (a b c x : Int) := a*x*x+b*x+c
def advance (second : Int) (s : Int × Int) := (s.1+s.2,s.2+second)
def differences (a b c start stride : Int) (n : Nat) :=
  (((List.range n).scanl (fun s _ => advance (2*a*stride*stride) s)
    (poly a b c start, poly a b c (start+stride)-poly a b c start)).take n).map Prod.fst
def signedFits (width : Nat) (x : Int) := -(2^width:Int) ≤ x ∧ x < 2^width

/-- Eliminate both per-row multiplications on an affine induction variable by updating two additions per row. -/
def QuadraticFiniteDifferenceLoop.Goal : Prop := ∀ a b c start stride n, differences a b c start stride n =
  (List.range n).map (fun (i : Nat) => poly a b c (start+stride*i))

/-- Advance a packed recurrence once per surviving row, even when a predicate introduces gaps in the input ordinals. -/
def SkippedIterationRecurrence.Goal : Prop := ∀ a b c n (keep : Nat → Bool), let sel := (List.range n).filter keep
  differences a b c 0 1 sel.length = sel.map (fun (i : Nat) => poly a b c i)

/-- Jump over a gap of k steps using one triangular-number update instead of iterating the skipped lanes. -/
def MaskedGapClosedForm.Goal : Prop := ∀ (v d dd : Int) (k : Nat), ((List.range k).foldl (fun s _ => advance dd s) (v,d)).1 =
  v+k*d+((k*(k-1)/2:Nat):Int)*dd

/-- Bounds on all emitted values also license unchecked updates of the recurrence one iteration beyond the last emitted row. -/
def FinalIterationStrengthOverflow.Goal : Prop := ∀ a b c w start stride n, 0 < n → (∀ x ∈ differences a b c start stride n, signedFits w x) →
  signedFits w (poly a b c (start+stride*(n:Int)))

/-- Assign an independent quadratic induction state to each vector lane with stride multiplied by vector width. -/
def LaneInterleavedInduction.Goal : Prop := ∀ a b c start stride lane width n, 0 < width → lane < width →
  differences a b c (start+stride*(lane:Nat)) (stride*(width:Nat)) n =
    (List.range n).map (fun (i : Nat) => poly a b c (start+stride*((i*width+lane:Nat):Int)))

/-- Choose the derivative register width from a zone map over the projected outputs alone. -/
def NarrowDerivativeFromOutputs.Goal : Prop := ∀ (w : Nat) v next, signedFits w v → signedFits w next → signedFits w (next-v)

/-- Validate monotone affine induction values at just the two loop endpoints, including negative strides. -/
def IntervalTripEndpointGuard.Goal : Prop := ∀ (w : Nat) (base step : Int) (n i : Nat), i ≤ n → signedFits w base → signedFits w (base+n*step) →
  signedFits w (base+i*step)

/-- An odd stride visits every slot in a vector ring of even capacity, so omit duplicate detection. -/
def HoistedNonunitPointerStride.Goal : Prop := ∀ (slots step start : Nat), 0 < slots → slots%2 = 0 → step%2 = 1 → step < slots →
  ((List.range slots).map (fun i => (start+i*step)%slots)).Nodup

/-- For a modular stride, the period needed for constant phase specialization shrinks by the stride's common factor. -/
def ResiduePeriodLoopPeeling.Goal : Prop := ∀ (slots step start : Nat), 0 < slots → let period := slots/(Nat.gcd slots step)
  ((List.range period).map (fun i => (start+i*step)%slots)).Nodup ∧ (start+period*step)%slots = start%slots

end TinylambOpt.ResearchGoals.JitKernels.InductionLowering
