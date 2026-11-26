import Std

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.
-/

namespace TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals

/-! Compile a non-NULL natural-key predicate into index seeks on [0,cap).
Half-open seek ranges are visited in order. Repeated input keys are separate
index entries. Empty and overlapping ranges have observable costs or duplicates. -/
structure Args where
  cap : Nat
  a : Nat
  b : Nat
abbrev Span := Nat × Nat
structure Model where
  test : Args → Nat → Bool
  plan : Args → List Span

def ranges (m : Model) (p : Args) := ((m.plan p).map fun (l,h) =>
  (min l p.cap, min h p.cap)).filter (fun (l,h) => l < h)
def inside (s : Span) (x : Nat) := s.1 ≤ x && x < s.2
def scan (m : Model) (p : Args) (keys : List Nat) :=
  (ranges m p).flatMap (fun s => keys.filter (inside s))
def refScan (m : Model) (p : Args) (keys : List Nat) :=
  keys.filter (fun x => x < p.cap && m.test p x)
def hull (ss : List Span) : Span :=
  ((ss.map Prod.fst).min?.getD 0, (ss.map Prod.snd).max?.getD 0)
def runs (bs : List Bool) :=
  ((false :: bs).zip bs).countP (fun (a,b) => !a && b)

/-- The emitted interval union covers exactly the predicate preimage. -/
def exactSeek (m : Model) : Prop := ∀ p x, (ranges m p).any (inside · x) = (x < p.cap && m.test p x)

/-- A duplicate-preserving index scan returns every matching tuple exactly once. -/
def multiplicity (m : Model) : Prop := ∀ p ks x, (scan m p ks).count x = (refScan m p ks).count x

/-- With sorted index entries, early LIMIT after seeking preserves the ordered prefix. -/
def orderedLimit (m : Model) : Prop := ∀ p ks n, ks.Pairwise (· ≤ ·) → (scan m p ks).take n = (refScan m p ks).take n

/-- The emitted seek count equals the number of maximal contiguous true regions. -/
def seekMinimum (m : Model) : Prop := ∀ p, (ranges m p).length = runs ((List.range p.cap).map (m.test p))

/-- Can multiple seeks be replaced by one seek with no residual filter? -/
def useHull (m : Model) : Prop := ∀ p x, inside (hull (ranges m p)) x = (x < p.cap && m.test p x)

/-- Sum of span widths gives the exact qualifying domain population. -/
def exactCoverage (m : Model) : Prop := ∀ p, ((ranges m p).map (fun (l,h) => h-l)).sum = (List.range p.cap).countP (m.test p)

/-- Truncating the domain changes no match beneath the new limit. -/
def shrinkDomain (m : Model) : Prop := ∀ p n x, x < n → n ≤ p.cap →
  (ranges m {p with cap := n}).any (inside · x) = (ranges m p).any (inside · x)

/-- Descending index iteration can reverse both the seek order and each local scan. -/
def reverseWalk (m : Model) : Prop := ∀ p ks, ks.Pairwise (· ≤ ·) →
  (ranges m p).reverse.flatMap (fun s => ks.reverse.filter (inside s)) = (refScan m p ks).reverse

namespace NotEqual

/-- Two ranges around the excluded key. -/
def model : Model :=
  ⟨fun p x => x != p.a,
   fun p => [(0,p.a),(p.a+1,p.cap)]⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def Hole.Goal : Prop := ∀ p, !(model.test p p.a)

def TwoSeeks.Goal : Prop := ∀ p, 0 < p.a → p.a+1 < p.cap → (ranges model p).length = 2

end NotEqual

namespace OutsideClosed

/-- Complement of BETWEEN, including reversed endpoints. -/
def model : Model :=
  ⟨fun p x => !(p.a ≤ x && x ≤ p.b),
   fun p => if p.a ≤ p.b then [(0,p.a),(p.b+1,p.cap)] else [(0,p.cap)]⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def Reversed.Goal : Prop := ∀ p x, p.b < p.a → model.test p x

def EqualEnds.Goal : Prop := ∀ p, p.a = p.b → (refScan model p [p.a]) = []

end OutsideClosed

namespace RingSlice

/-- Clockwise half-open range over an ordered ring; equal bounds mean empty. -/
def model : Model :=
  ⟨fun p x => if p.a ≤ p.b then p.a ≤ x && x < p.b else x < p.b || p.a ≤ x,
   fun p => if p.a ≤ p.b then [(p.a,p.b)] else [(0,p.b),(p.a,p.cap)]⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def WrapSize.Goal : Prop := ∀ p, p.b < p.a → p.a ≤ p.cap → (refScan model p (List.range p.cap)).length = p.b+p.cap-p.a

def SameEdge.Goal : Prop := ∀ p, p.a = p.b → ranges model p = []

end RingSlice

namespace DistanceBand

/-- Absolute distance from a center no greater than the radius. -/
def model : Model :=
  ⟨fun p x => max x p.a - min x p.a ≤ p.b,
   fun p => [(p.a-p.b,p.a+p.b+1)]⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def ZeroRadius.Goal : Prop := ∀ p x, p.b = 0 → (model.test p x = true ↔ x = p.a)

def LowerClip.Goal : Prop := ∀ p, p.a ≤ p.b → model.test p 0

end DistanceBand

namespace QuotientBand

/-- Integer quotient equality in a positive-width bucket. -/
def model : Model :=
  ⟨fun p x => x/(p.b+1) == p.a,
   fun p => [(p.a*(p.b+1),(p.a+1)*(p.b+1))]⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def QuotientEdge.Goal : Prop := ∀ p, model.test p (p.a*(p.b+1))

def FullBucket.Goal : Prop := ∀ p, (p.a+1)*(p.b+1) ≤ p.cap → (refScan model p (List.range p.cap)).length = p.b+1

end QuotientBand

namespace Residue

/-- Residue equality compiled to singleton seeks. -/
def model : Model :=
  ⟨fun p x => x%(p.b+1) == p.a,
   fun p => ((List.range (p.cap/(p.b+1)+1)).map (fun j => p.a+j*(p.b+1))).filterMap (fun x => if p.a ≤ p.b then some (x,x+1) else none)⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def InvalidDigit.Goal : Prop := ∀ p, p.b < p.a → ranges model p = []

def Period.Goal : Prop := ∀ p x, model.test p (x+p.b+1) = model.test p x

end Residue

namespace TwoLiterals

/-- IN list of two possibly equal literals. -/
def model : Model :=
  ⟨fun p x => x == p.a || x == p.b,
   fun p => ([p.a,p.b].eraseDups.mergeSort).map (fun x => (x,x+1))⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def DuplicateLiteral.Goal : Prop := ∀ p, p.a = p.b → p.a < p.cap → (ranges model p).length = 1

def AdjacentMerge.Goal : Prop := ∀ p, p.b = p.a+1 → p.b < p.cap → runs ((List.range p.cap).map (model.test p)) = 1

end TwoLiterals

namespace SaturatingSubtract

/-- Equality after unsigned saturating subtraction. -/
def model : Model :=
  ⟨fun p x => x-p.a == p.b,
   fun p => if p.b == 0 then [(0,p.a+1)] else [(p.a+p.b,p.a+p.b+1)]⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def Plateau.Goal : Prop := ∀ p, p.b = 0 → ∀ x, x ≤ p.a → model.test p x

def StrictTail.Goal : Prop := ∀ p x, 0 < p.b → (model.test p x = true ↔ x = p.a+p.b)

end SaturatingSubtract

namespace MinClamp

/-- Equality after clamping keys to an upper bound. -/
def model : Model :=
  ⟨fun p x => min x p.a == p.b,
   fun p => if p.b < p.a then [(p.b,p.b+1)] else if p.a == p.b then [(p.a,p.cap)] else []⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def EmptyAbove.Goal : Prop := ∀ p, p.a < p.b → ranges model p = []

def PlateauStart.Goal : Prop := ∀ p x, p.a = p.b → (model.test p x = true ↔ p.a ≤ x)

end MinClamp

namespace XorLowBit

/-- Adjacent encoded keys swap order; seek inverse image of a closed range. -/
def model : Model :=
  ⟨fun p x => p.a ≤ (x ^^^ 1) && (x ^^^ 1) ≤ p.b,
   fun p =>
     if p.b < p.a then [] else
       let left := if p.a%2 == 1 then [(p.a-1,p.a)] else []
       let right := if p.b%2 == 0 then [(p.b+1,p.b+2)] else []
       left ++ [(p.a+p.a%2, (p.b+1)/2*2)] ++ right⟩

def ExactSeek.Goal : Prop := exactSeek model
def Multiplicity.Goal : Prop := multiplicity model
def OrderedLimit.Goal : Prop := orderedLimit model
def SeekMinimum.Goal : Prop := seekMinimum model
def UseHull.Goal : Prop := useHull model
def ExactCoverage.Goal : Prop := exactCoverage model
def ShrinkDomain.Goal : Prop := shrinkDomain model
def ReverseWalk.Goal : Prop := reverseWalk model

def PairClosed.Goal : Prop := ∀ p x, p.a%2 = 0 → p.b%2 = 1 → model.test p x = (p.a ≤ x && x ≤ p.b)

def SingletonSwap.Goal : Prop := ∀ p, p.a = p.b → model.test p (p.a ^^^ 1)

end XorLowBit

#guard (ranges TwoLiterals.model ⟨8,3,4⟩).length == 2
#guard (ranges XorLowBit.model ⟨9,3,6⟩) == [(2,3),(4,6),(7,8)]
#guard runs ((List.range 8).map (TwoLiterals.model.test ⟨8,3,4⟩)) == 1

end TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
