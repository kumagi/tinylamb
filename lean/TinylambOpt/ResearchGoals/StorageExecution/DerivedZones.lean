import TinylambOpt.ResearchGoals.Compression.Wire

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.
-/

namespace TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Two-column zone maps. A rectangle may contain combinations absent from
the page. Skipping is justified by a conservative interval; an empty page or
NULL row contributes no score. Arithmetic is unbounded and nonnegative. -/
structure Bounds where
  lo : Nat
  hi : Nat
deriving DecidableEq, Repr
def inside (r : Bounds) (x : Nat) := r.lo ≤ x ∧ x ≤ r.hi
def gap (x y : Nat) := (x-y)+(y-x)
def gapMin (a b : Bounds) := max (a.lo-b.hi) (b.lo-a.hi)
def gapMax (a b : Bounds) := max (a.hi-b.lo) (b.hi-a.lo)
abbrev Row := Nat × Nat
structure Kernel where
  score : Nat → Row → Nat
  bounds : Nat → Bounds → Bounds → Bounds
def covers (p : List (Option Row)) (a b : Bounds) :=
  ∀ r ∈ p, ∀ x y, r=some (x,y) → inside a x ∧ inside b y
def scores (f : Kernel) (t : Nat) (p : List (Option Row)) :=
  p.filterMap (fun r => r.map (f.score t))
def selected (f : Kernel) (t : Nat) (p : List (Option Row)) (lo hi : Nat) :=
  (scores f t p).filter (fun v => lo≤v && v<hi)
def allows (r : Bounds) (lo hi : Nat) := lo≤r.hi && r.lo<hi && lo<hi

/-- Bound a computed group or predicate key from independent column min/max. -/
def soundInterval (f : Kernel) : Prop := ∀ t a b x y, inside a x → inside b y → inside (f.bounds t a b) (f.score t (x,y))

/-- Drop a whole page for a range test, preserving empty and NULL pages. -/
def skipRange (f : Kernel) : Prop := ∀ t p a b lo hi, covers p a b → allows (f.bounds t a b) lo hi = false →
  selected f t p lo hi = []

/-- Eliminate the scalar predicate once the entire score interval fits. -/
def takeAll (f : Kernel) : Prop := ∀ t p a b lo hi, covers p a b → lo≤(f.bounds t a b).lo →
  (f.bounds t a b).hi<hi → selected f t p lo hi = scores f t p

/-- Estimate the number of passing rows using only certified and possible pages. -/
def thresholdCount (f : Kernel) : Prop := ∀ t (ps : List (List (Option Row) × Bounds × Bounds)) cut,
  (∀ e ∈ ps, covers e.1 e.2.1 e.2.2) →
  let n := fun (e : List (Option Row) × Bounds × Bounds) => (e.1.filterMap id).length
  let actual := ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
  (((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)).map n).sum ≤ actual ∧
    actual ≤ ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).hi)).map n).sum)

/-- A bounded page cannot change the descending top k once k strictly better witnesses are known. -/
def dominatedTop (f : Kernel) : Prop := ∀ t p a b (seen : List Nat) k, covers p a b →
  k≤seen.countP (fun x => (f.bounds t a b).hi < x) →
  ((seen++scores f t p).mergeSort (· ≥ ·)).take k = (seen.mergeSort (· ≥ ·)).take k

/-- Disjoint derived score intervals imply an empty equijoin between pages. -/
def crossPageJoin (f : Kernel) : Prop := ∀ t u p q a b c d, covers p a b → covers q c d →
  (f.bounds t a b).hi < (f.bounds u c d).lo →
  ∀ x ∈ scores f t p, x ∉ scores f u q

/-- Apply the footer to the build-key set before probing; retain left bag order and duplicates. -/
def semijoinFilter (f : Kernel) : Prop := ∀ t p a b (ks : List Nat), covers p a b →
  let r := f.bounds t a b
  (scores f t p).filter ks.contains = (scores f t p).filter ((ks.filter (fun k => r.lo≤k && k≤r.hi)).contains)

/-- A footer gives a deterministic interval for the SUM accumulator over non-NULL scores. -/
def sumCeiling (f : Kernel) : Prop := ∀ t p a b, covers p a b →
  let vs := scores f t p
  vs.length*(f.bounds t a b).lo ≤ vs.sum ∧ vs.sum ≤ vs.length*(f.bounds t a b).hi

namespace Product

/-- Nonnegative price times volume. -/
def kernel : Kernel := ⟨fun _ (x,y) => x*y,
  fun _ a b => ⟨a.lo*b.lo,a.hi*b.hi⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def ProductDivisor.Goal : Prop :=
  ∀ x y k, 0<x → (k≤kernel.score 0 (x,y) ↔ (k+x-1)/x≤y)
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def UpperWitness.Goal : Prop := ∀ a b, (kernel.bounds 0 a b).hi = kernel.score 0 (a.hi,b.hi)

end Product

namespace SaturatingLoss

/-- Nonnegative excess after covering units and deductible. -/
def kernel : Kernel := ⟨fun t (x,y) => x-(y+t),
  fun t a b => ⟨a.lo-(b.hi+t),a.hi-(b.lo+t)⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def ThresholdInverse.Goal : Prop :=
  ∀ t x y k, 0<k → (k≤kernel.score t (x,y) ↔ y+t+k≤x)
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def ZeroCertificate.Goal : Prop := ∀ t a b, (kernel.bounds t a b).hi=0 ↔ a.hi≤b.lo+t

end SaturatingLoss

namespace Distance

/-- Time skew between two timestamps. -/
def kernel : Kernel := ⟨fun _ (x,y) => gap x y,
  fun _ a b => ⟨gapMin a b,gapMax a b⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def BandInverse.Goal : Prop :=
  ∀ t x y, kernel.score 0 (x,y)≤t ↔ x≤y+t ∧ y≤x+t
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def OverlapFloor.Goal : Prop := ∀ a b, (kernel.bounds 0 a b).lo=0 ↔ a.lo≤b.hi ∧ b.lo≤a.hi

end Distance

namespace FloorRatio

/-- Scaled conversion rate with an explicitly regularized denominator. -/
def kernel : Kernel := ⟨fun t (x,y) => (x*t)/(y+1),
  fun t a b => ⟨(a.lo*t)/(b.hi+1),(a.hi*t)/(b.lo+1)⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def DenominatorPage.Goal : Prop :=
  ∀ t a b k, k≤(kernel.bounds t a b).lo ↔ k*(b.hi+1)≤a.lo*t
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def QuotientThreshold.Goal : Prop := ∀ t a b k, (kernel.bounds t a b).hi<k ↔ a.hi*t<k*(b.lo+1)

end FloorRatio

namespace BucketSum

/-- Range key on the combined measure. -/
def kernel : Kernel := ⟨fun t (x,y) => (x+y)/(t+1),
  fun t a b => ⟨(a.lo+b.lo)/(t+1),(a.hi+b.hi)/(t+1)⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def BucketInverse.Goal : Prop :=
  ∀ t x y k, kernel.score t (x,y)=k ↔ k*(t+1)≤x+y ∧ x+y<(k+1)*(t+1)
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def SingleGroup.Goal : Prop := ∀ t a b, (a.hi+b.hi)/(t+1)=(a.lo+b.lo)/(t+1) → (kernel.bounds t a b).lo=(kernel.bounds t a b).hi

end BucketSum

namespace WrappedSum

/-- Minute-of-day or hash-wheel residue, with explicit wrap detection. -/
def kernel : Kernel := ⟨fun t (x,y) => (x+y)%(t+1),
  fun t a b => if (a.lo+b.lo)/(t+1)=(a.hi+b.hi)/(t+1) then ⟨(a.lo+b.lo)%(t+1),(a.hi+b.hi)%(t+1)⟩ else ⟨0,t⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def TranslateCycle.Goal : Prop :=
  ∀ t n x y, kernel.score t (x+n*(t+1),y) = kernel.score t (x,y)
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def Rollover.Goal : Prop := ∀ t a b, (a.lo+b.lo)/(t+1)<(a.hi+b.hi)/(t+1) → kernel.bounds t a b=⟨0,t⟩

end WrappedSum

namespace Manhattan

/-- Distance to a key on the diagonal; score both dimensions. -/
def kernel : Kernel := ⟨fun t (x,y) => gap x t + gap y t,
  fun t a b => ⟨gapMin a ⟨t,t⟩+gapMin b ⟨t,t⟩,gapMax a ⟨t,t⟩+gapMax b ⟨t,t⟩⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def WitnessCorners.Goal : Prop :=
  ∀ t a b, a.lo≤a.hi → b.lo≤b.hi → (kernel.bounds t a b).hi =
    peak ([a.lo,a.hi].flatMap (fun x => [b.lo,b.hi].map (fun y => kernel.score t (x,y))))
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def InteriorMinimum.Goal : Prop := ∀ t a b, inside a t → inside b t → (kernel.bounds t a b).lo=0

end Manhattan

namespace BitwiseXor

/-- High common prefixes bound an XOR hash distance. -/
def kernel : Kernel := ⟨fun _ (x,y) => x ^^^ y,
  fun _ a b => let w := max (width (a.lo ^^^ a.hi)) (width (b.lo ^^^ b.hi)); let base := ((a.lo ^^^ b.lo)/2^w)*2^w; ⟨base,base+2^w-1⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def AlignedSiblings.Goal : Prop :=
  ∀ k a b, (kernel.bounds 0 ⟨a*2^k,(a+1)*2^k-1⟩ ⟨b*2^k,(b+1)*2^k-1⟩).lo = (a ^^^ b)*2^k
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def FixedPrefix.Goal : Prop := ∀ x y, kernel.bounds 0 ⟨x,x⟩ ⟨y,y⟩=⟨x ^^^ y,x ^^^ y⟩

end BitwiseXor

namespace CappedPair

/-- Saturate the smaller of supply and demand at a contractual cap. -/
def kernel : Kernel := ⟨fun t (x,y) => min t (min x y),
  fun t a b => ⟨min t (min a.lo b.lo),min t (min a.hi b.hi)⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def PushBoth.Goal : Prop :=
  ∀ t k x y, k≤t → (k≤kernel.score t (x,y) ↔ k≤x ∧ k≤y)
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def SaturatedPage.Goal : Prop := ∀ t a b, t≤a.lo → t≤b.lo → (kernel.bounds t a b).lo=t

end CappedPair

namespace QuadraticGap

/-- Squared difference for a distance threshold join. -/
def kernel : Kernel := ⟨fun t (x,y) => (gap x y)^2+t,
  fun t a b => ⟨(gapMin a b)^2+t,(gapMax a b)^2+t⟩⟩

def SoundInterval.Goal : Prop := soundInterval kernel
def SkipRange.Goal : Prop := skipRange kernel
def TakeAll.Goal : Prop := takeAll kernel
def ThresholdCount.Goal : Prop := thresholdCount kernel
def DominatedTop.Goal : Prop := dominatedTop kernel
def CrossPageJoin.Goal : Prop := crossPageJoin kernel
def SquaredRadius.Goal : Prop :=
  ∀ t r x y, kernel.score t (x,y)≤r*r+t ↔ x≤y+r ∧ y≤x+r
def SemijoinFilter.Goal : Prop := semijoinFilter kernel
def SumCeiling.Goal : Prop := sumCeiling kernel

def GapWitness.Goal : Prop := ∀ t a b, a.hi<b.lo → (kernel.bounds t a b).lo = kernel.score t (a.hi,b.lo)

end QuadraticGap

#guard WrappedSum.kernel.bounds 9 ⟨8,9⟩ ⟨0,2⟩ == ⟨0,9⟩
#guard selected Product.kernel 0 [some (1,8),none,some (8,1)] 50 100 == []

end TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
