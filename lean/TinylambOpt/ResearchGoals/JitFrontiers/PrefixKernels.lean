import TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

namespace TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)
/-! An affine update represents one fused exact multiply-add with a checked
result. If source multiplication is separately fallible, its guard is an
additional precondition before using this model. Prefix maps span negative
slopes, NULL identity lanes and resets; the incoming accumulator is arbitrary. -/
abbrev Row := Option Int
abbrev Aff := Int × Int
abbrev Kernel := Row → Aff
def apply (f : Aff) (s : Int) := f.1*s+f.2
def compose (later early : Aff) : Aff := (later.1*early.1,later.1*early.2+later.2)
def prefixes (k : Kernel) (rs : List Row) := (rs.map k).scanl (fun f g => compose g f) (1,0)
def whole (k : Kernel) (rs : List Row) := (rs.map k).foldl (fun f g => compose g f) (1,0)
def execute (k : Kernel) (w : Nat) : Nat → Int → List Row → Except Nat Int
  | _,s,[] => .ok s
  | i,s,r::rs => let next := apply (k r) s
                  if fits w next then execute k w (i+1) next rs else .error i
def cert (k : Kernel) (w : Nat) (s : Int) (rs : List Row) :=
  ((prefixes k rs).drop 1).all (fun f => fits w (apply f s))
def preimage (w : Nat) (f : Aff) (s : Int) :=
  let l := -limit w - f.2
  let h := limit w - 1 - f.2
  if f.1 == 0 then fits w f.2 else
  if 0 < f.1 then -( (-l)/f.1) ≤ s && s ≤ h/f.1 else
    - (h/(-f.1)) ≤ s && s ≤ (-l)/(-f.1)
def selected (mask : List Bool) (rs : List Row) :=
  rs.zipIdx.filterMap (fun (r,i) => if mask[i]?.getD false then some r else none)

/-- Intersect inverse affine ranges to characterize every valid incoming accumulator. -/
def exactEntryGuard (k : Kernel) : Prop := ∀ w s rs, (((prefixes k rs).drop 1).all (fun f => preimage w f s)) =
  (match execute k w 0 s rs with | .ok _ => true | .error _ => false)

/-- A verified block requires only a multiply-add on entry to compute its outgoing state. -/
def uncheckedFold (k : Kernel) : Prop := ∀ w s rs, cert k w s rs → execute k w 0 s rs=.ok (apply (whole k rs) s)

/-- Map the first violated prefix certificate back to the exact source row. -/
def failingLane (k : Kernel) : Prop := ∀ w s rs i, execute k w 0 s rs=.error i ↔
  (((prefixes k rs).drop 1).findIdx? (fun f => !(fits w (apply f s))))=some i

/-- Shard summaries can validate both pieces without replaying the first. -/
def joinCertificates (k : Kernel) : Prop := ∀ w s a b, cert k w s (a++b) =
  (cert k w s a && cert k w (apply (whole k a) s) b)

/-- Safe endpoints certify a full interval of possible initial states. -/
def twoEntryChecks (k : Kernel) : Prop := ∀ w a b s rs, a≤s → s≤b → cert k w a rs → cert k w b rs → cert k w s rs

/-- Aggressive proposal: replace every prefix guard by one check on the final state. -/
def finalCheck (k : Kernel) : Prop := ∀ w s rs, fits w s → fits w (apply (whole k rs) s) → cert k w s rs

/-- Reordering arbitrary partitions is safe when the composed maps commute and both orders pass prefix checks. -/
def mergeOrder (k : Kernel) : Prop := ∀ w s a b, compose (whole k a) (whole k b)=compose (whole k b) (whole k a) →
  cert k w s (a++b) → cert k w s (b++a) → execute k w 0 s (a++b)=execute k w 0 s (b++a)

/-- Compile masked lanes as identities and recover the selected result, given a valid incoming value. -/
def dropMasked (k : Kernel) : Prop := ∀ w s mask rs v, fits w s →
  let lanes := rs.zipIdx.map (fun (r,i) => if mask[i]?.getD false then k r else (1,0))
  let path := lanes.scanl (fun x f => apply f x) s
  (execute k w 0 s (selected mask rs)=.ok v ↔ path.all (fits w) ∧ path.getLast?=some v)

namespace SignedSum

/-- Nullable running SUM with arbitrary incoming total. -/
def update : Kernel := fun r => (1,r.getD 0)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def PeakEnvelope.Goal : Prop := ∀ w s rs,
  let ps := (rs.map (·.getD 0)).scanl (·+·) 0
  fits w s → (cert update w s rs = true ↔
    -limit w ≤ s + ps.min?.getD 0 ∧ s + ps.max?.getD 0 < limit w)

def LateCancellation.Goal : Prop := ∀ w, 1<w → execute update w 0 (limit w-1) [some 1,some (-1)]=.error 0

end SignedSum

namespace Product

/-- Streaming signed product with NULL skips. -/
def update : Kernel := fun r => (r.getD 1,0)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def ZeroSuffix.Goal : Prop := ∀ w s rs, execute update w 0 s (some 0::rs)=.ok 0

def MinNegation.Goal : Prop := ∀ w, execute update w 0 (-limit w) [some (-1)]=.error 0

end Product

namespace NonNullCount

/-- COUNT(x) increments only on present rows. -/
def update : Kernel := fun r => (1,r.isSome.toNat)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def ClosedForm.Goal : Prop := ∀ rs, whole update rs=(1,((rs.filterMap id).length:Int))

def MonotoneGuard.Goal : Prop := ∀ w s rs, fits w s → (cert update w s rs=true ↔ fits w (s+(rs.filterMap id).length)=true)

end NonNullCount

namespace AlternatingDifference

/-- Negate prior subtotal before subtracting neighboring measures. -/
def update : Kernel := fun r => (r.map (fun x => ((-1:Int),x))).getD (1,0)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def EvenPairs.Goal : Prop := ∀ a b, whole update [some a,some b]=(1,b-a)

def ParitySlope.Goal : Prop := ∀ rs, (whole update rs).1=(-1:Int)^((rs.filterMap id).length)

end AlternatingDifference

namespace ResetOnNull

/-- Partition boundary encoded by a NULL resets the running sum. -/
def update : Kernel := fun r => (r.map (fun x => ((1:Int),x))).getD (0,0)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def DropHistory.Goal : Prop := ∀ a b s t,
  let afterReset := whole update (a ++ [none] ++ b)
  apply afterReset s = apply (whole update b) 0 ∧ apply afterReset t = apply (whole update b) 0

def MaskedNullBoundary.Goal : Prop := execute update 8 0 5 [none]=.ok 0

end ResetOnNull

namespace DecimalAppend

/-- Fold nullable decimal digits into a checked integer. -/
def update : Kernel := fun r => (r.map (fun x => ((10:Int),x))).getD (1,0)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def DigitPlaces.Goal : Prop := ∀ rs, (whole update rs).1=(10:Int)^((rs.filterMap id).length)

def LeadingZeros.Goal : Prop := ∀ w n rs, execute update w n 0 (some 0::rs)=execute update w (n+1) 0 rs

end DecimalAppend

namespace PositiveFilter

/-- Only positive measures contribute after WHERE. -/
def update : Kernel := fun r => (1,max 0 (r.getD 0))

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def SingleUpperCheck.Goal : Prop := ∀ w s rs, fits w s → (cert update w s rs=true ↔ apply (whole update rs) s < limit w)

def NonpositiveSkip.Goal : Prop := ∀ x, x≤0 → update (some x)=update none

end PositiveFilter

namespace DifferenceFromBase

/-- Per-row projected difference with a fixed baseline. -/
def update : Kernel := fun r => (1,(r.map (fun x => x-100)).getD 0)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def RebiasCount.Goal : Prop := ∀ rs, (whole update rs).2=(rs.filterMap id).sum - 100*((rs.filterMap id).length:Int)

def NeutralBand.Goal : Prop := ∀ rs n, whole update (List.replicate n (some 100) ++ rs)=whole update rs

end DifferenceFromBase

namespace LastPresent

/-- The last non-null row overwrites the incoming state. -/
def update : Kernel := fun r => (r.map (fun x => ((0:Int),x))).getD (1,0)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def LastValue.Goal : Prop := ∀ rs s, apply (whole update rs) s=(rs.filterMap id).getLast?.getD s

def OverwriteDoesNotUndoFault.Goal : Prop := ∀ w, execute update w 0 0 [some (limit w),some 0]=.error 0

end LastPresent

namespace SignedMagnitude

/-- Flip the accumulator on negative inputs before adding the absolute value. -/
def update : Kernel := fun r => let x:=r.getD 0; (if x<0 then -1 else 1,x.natAbs)

def ExactEntryGuard.Goal : Prop := exactEntryGuard update
def UncheckedFold.Goal : Prop := uncheckedFold update
def FailingLane.Goal : Prop := failingLane update
def JoinCertificates.Goal : Prop := joinCertificates update
def TwoEntryChecks.Goal : Prop := twoEntryChecks update
def FinalCheck.Goal : Prop := finalCheck update
def MergeOrder.Goal : Prop := mergeOrder update
def DropMasked.Goal : Prop := dropMasked update

def NegativePair.Goal : Prop := ∀ x y, x<0 → y<0 → whole update [some x,some y]=(1,x-y)

def NonnegativeRun.Goal : Prop := ∀ rs, (∀ x∈rs.filterMap id, 0≤x) → whole update rs=(1,(rs.filterMap id).sum)

end SignedMagnitude

#guard (execute SignedSum.update 8 0 127 [some 1,some (-1)] matches .error 0)
#guard preimage 8 (-2,3) (-62) && !(preimage 8 (-2,3) (-63))

end TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
