import TinylambOpt.ResearchGoals.Compression.Wire

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.
-/

namespace TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.Compression.Wire

/-! A sorted right key column can serve many left probes. Compile a predicate
to a disjoint union of half-open intervals in the finite key domain [0,u).
Duplicate right keys retain their full multiplicity. NULL keys are skipped. -/
abbrev Ranges := List (Nat × Nat)
structure Search where
  test : Nat → Nat → Nat → Bool  -- parameter, left key, right key
  spans : Nat → Nat → Nat → Ranges  -- exclusive domain end, parameter, left key
def member (rs : Ranges) (y : Nat) := rs.any (fun (a,b) => a≤y && y<b)
def searchBound (xs : Array Nat) (v : Nat) : Nat → Nat → Nat → Nat
  | 0,lo,_ => lo
  | fuel+1,lo,hi => if lo≥hi then lo else
    let mid := (lo+hi)/2
    if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid+1) hi else searchBound xs v fuel lo mid
def lower (xs : List Nat) (v : Nat) := searchBound xs.toArray v (width xs.length + 1) 0 xs.length
def nth (ys : List Nat) : Ranges → Nat → Option Nat
  | [],_ => none
  | (a,b)::rs,i =>
    let start := lower ys a
    let n := lower ys b - start
    if i<n then ys[start+i]? else nth ys rs (i-n)
def slice (xs : List Nat) (a b : Nat) := (xs.drop (lower xs a)).take (lower xs b-lower xs a)
def seek (s : Search) (u t x : Nat) (ys : List Nat) :=
  (s.spans u t x).flatMap (fun (a,b) => slice ys a b)
def fan (s : Search) (u t x : Nat) (ys : List Nat) :=
  ((s.spans u t x).map (fun (a,b) => lower ys b-lower ys a)).sum
def sumAt (xs : List Nat) (i : Nat) := (xs.take i).sum
def total (s : Search) (u t x : Nat) (ys : List Nat) :=
  ((s.spans u t x).map (fun (a,b) => sumAt ys (lower ys b) - sumAt ys (lower ys a))).sum
def valid (u : Nat) (ys : List Nat) := ys.Pairwise (· ≤ ·) ∧ ∀ y ∈ ys, y<u
def optional (a b : Nat) : Ranges := if a<b then [(a,b)] else []
def zipOuter (s : Search) (t : Nat) (xs ys : List (Option Nat)) :=
  xs.flatMap (fun x =>
    let hits := (ys.filterMap id).filter (fun y => x.any (fun k => s.test t k y))
    if hits.isEmpty then [(x,none)] else hits.map (fun y => (x,some y)))

/-- Concatenate right slices without a duplicate-removal or sorting pass. -/
def orderedProbe (s : Search) : Prop := ∀ u t x ys, valid u ys → seek s u t x ys = ys.filter (s.test t x)

/-- Two lower-bound offsets per interval compute the exact join multiplicity. -/
def offsetCount (s : Search) : Prop := ∀ u t x ys, valid u ys → fan s u t x ys = ys.countP (s.test t x)

/-- Subtraction on a stored prefix-sum column yields the right payload sum. -/
def integralSum (s : Search) : Prop := ∀ u t x ys, valid u ys → total s u t x ys = (ys.filter (s.test t x)).sum

/-- Charge one left placeholder when zero right keys match, including a NULL left key. -/
def leftMultiplicity (s : Search) : Prop := ∀ u t (xs ys : List (Option Nat)), valid u (ys.filterMap id) →
  (zipOuter s t xs ys).length = (xs.map (fun x => max 1 ((x.map (fun v => fan s u t v (ys.filterMap id))).getD 0))).sum

/-- Cache one interval query per distinct left key when aggregating without output expansion. -/
def repeatedProbe (s : Search) : Prop := ∀ u t (xs ys : List Nat), valid u ys →
  (xs.eraseDups.map (fun x => xs.count x * fan s u t x ys)).sum = (xs.flatMap (fun x => ys.filter (s.test t x))).length

/-- Top offset within the matching postings preserves tie multiplicity and right ordering. -/
def kthMatch (s : Search) : Prop := ∀ u t x ys (k : Nat), valid u ys →
  nth ys (s.spans u t x) k = (ys.filter (s.test t x))[k]?

/-- Elide a posting list when its stored endpoints miss every interval. -/
def pruneExtent (s : Search) : Prop := ∀ u t x ys lo hi, valid u ys → (∀ y ∈ ys, lo≤y ∧ y<hi) →
  (∀ p ∈ s.spans u t x, p.2≤lo ∨ hi≤p.1) → ys.filter (s.test t x)=[]

/-- A point-delete bitmap on the right can subtract removed multiplicities from the offset count. -/
def maskedJoin (s : Search) : Prop := ∀ u t x ys (ds : List Nat), valid u ys →
  fan s u t x ys - ds.eraseDups.countP (fun i => ys[i]?.any (s.test t x)) =
    ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP (s.test t x)

namespace Following

/-- Bounded forward events after the left event. -/
def search : Search := ⟨fun t x y => x<y && y≤x+t,
  fun u t x => optional (x+1) (min u (x+t+1))⟩

def SlidingOffsets.Goal : Prop :=
  ∀ u t x ys, valid u ys → fan search u t (x+1) ys + ys.count (x+1) = fan search u t x ys + ys.count (x+t+1)
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def ZeroLookahead.Goal : Prop := ∀ u x, search.spans u 0 x = []

end Following

namespace Annulus

/-- Near-neighbor search with a dead band around the center. -/
def search : Search := ⟨fun t x y => let d := x-y+(y-x); t<d && d≤2*t+1,
  fun u t x => optional (x-(2*t+1)) (min u (x-t)) ++ optional (x+t+1) (min u (x+2*t+2))⟩

def SubtractInnerBand.Goal : Prop :=
  ∀ u t x ys, valid u ys → fan search u t x ys + ys.countP (fun y => x≤y+t && y≤x+t) = ys.countP (fun y => x≤y+(2*t+1) && y≤x+(2*t+1))
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def TwoSlices.Goal : Prop := ∀ u t x, (search.spans u t x).length ≤ 2

end Annulus

namespace SameBucket

/-- Join by a coarsened time or price bucket. -/
def search : Search := ⟨fun t x y => x/(t+1)==y/(t+1),
  fun u t x => let a := x/(t+1)*(t+1); optional a (min u (a+t+1))⟩

def HistogramProduct.Goal : Prop :=
  ∀ u t (xs ys : List Nat), valid u xs → valid u ys → (xs.map (fun x => fan search u t x ys)).sum =
    ((List.range ((u+t)/(t+1))).map (fun k =>
      xs.countP (fun x => x/(t+1)==k) * ys.countP (fun y => y/(t+1)==k))).sum
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def BucketReuse.Goal : Prop := ∀ u t x z, x/(t+1)=z/(t+1) → search.spans u t x = search.spans u t z

end SameBucket

namespace AdjacentBucket

/-- Match neighboring time buckets, excluding the current one. -/
def search : Search := ⟨fun t x y => let a:=x/(t+1); let b:=y/(t+1); a+1==b || b+1==a,
  fun u t x => let w:=t+1; let a:=x/w*w; optional (a-w) (min u a) ++ optional (a+w) (min u (a+2*w))⟩

def SymmetricJoin.Goal : Prop :=
  ∀ u t a b, valid u a → valid u b → (a.map (fun x => fan search u t x b)).sum = (b.map (fun x => fan search u t x a)).sum
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def NoSelf.Goal : Prop := ∀ t x, search.test t x x=false

end AdjacentBucket

namespace ModuloClass

/-- Interleaved shard postings as singleton ranges. -/
def search : Search := ⟨fun t x y => x%(t+1)==y%(t+1),
  fun u t x => let w:=t+1; let r:=x%w; (List.range ((u+w-1-r)/w)).map (fun i => (r+i*w,r+i*w+1))⟩

def ResidueCache.Goal : Prop :=
  ∀ u t x k ys, seek search u t (x+k*(t+1)) ys = seek search u t x ys
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def ClassPopulation.Goal : Prop := ∀ u t x, (search.spans u t x).length ≤ (u+t)/(t+1)

end ModuloClass

namespace SharedPrefix

/-- Skip a suffix of low-order bits when joining packed hierarchical keys. -/
def search : Search := ⟨fun t x y => x/2^t==y/2^t,
  fun u t x => let a:=x/2^t*2^t; optional a (min u (a+2^t))⟩

def SplitSubtrees.Goal : Prop :=
  ∀ u t x ys, valid u ys → let a:=(x/2^(t+1))*2^(t+1); fan search u (t+1) x ys = fan search u t a ys + fan search u t (a+2^t) ys
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def ExactAtLeaf.Goal : Prop := ∀ u x, x<u → search.spans u 0 x=[(x,x+1)]

end SharedPrefix

namespace Submask

def submasks (x : Nat) : List Nat :=
  ((List.range (width x)).filter x.testBit).foldl (fun acc bit =>
    acc ++ acc.map (· + 2^bit)) [0]

/-- All right submasks of a left capability word, with a cardinality budget. -/
def search : Search := ⟨fun t x y => (y &&& x)==y && y≤t,
  fun u t x => ((submasks x).filter (fun y => y<u && y≤t)).map (fun y => (y,y+1))⟩

def DistinctPostings.Goal : Prop :=
  ∀ u t x, ((search.spans u t x).map Prod.fst).Nodup ∧ (search.spans u t x).length≤min u (t+1)
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def ZeroMask.Goal : Prop := ∀ u t, 0<u → search.spans u t 0=[(0,1)]

end Submask

namespace ReciprocalPrice

/-- Find all right quantities whose product with the left price fits the budget, including zero. -/
def search : Search := ⟨fun t x y => x*y≤t,
  fun u t x => optional 0 (if x==0 then u else min u (t/x+1))⟩

def MonotoneProbe.Goal : Prop :=
  ∀ u t x z ys, valid u ys → x≤z → fan search u t z ys ≤ fan search u t x ys
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def FreeItem.Goal : Prop := ∀ u t ys, valid u ys → fan search u t 0 ys = ys.length

end ReciprocalPrice

namespace ScaledFloor

/-- Find right values with an integer quotient relative to the left key. -/
def search : Search := ⟨fun t x y => y/(x+1)==t,
  fun u t x => optional (t*(x+1)) (min u ((t+1)*(x+1)))⟩

def PartitionDomain.Goal : Prop :=
  ∀ u x ys, valid u ys → ((List.range (u/(x+1)+1)).map (fun t => fan search u t x ys)).sum=ys.length
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def BucketWidth.Goal : Prop := ∀ t x, search.spans ((t+1)*(x+1)) t x=[(t*(x+1),(t+1)*(x+1))]

end ScaledFloor

namespace Apart

/-- An inequality join excludes one contiguous neighborhood. -/
def search : Search := ⟨fun t x y => x+t<y || y+t<x,
  fun u t x => optional 0 (min u (x-t)) ++ optional (x+t+1) u⟩

def ZeroSlack.Goal : Prop :=
  ∀ u x ys, valid u ys → fan search u 0 x ys+ys.count x=ys.length
def OrderedProbe.Goal : Prop := orderedProbe search
def OffsetCount.Goal : Prop := offsetCount search
def IntegralSum.Goal : Prop := integralSum search
def LeftMultiplicity.Goal : Prop := leftMultiplicity search
def RepeatedProbe.Goal : Prop := repeatedProbe search
def KthMatch.Goal : Prop := kthMatch search
def PruneExtent.Goal : Prop := pruneExtent search
def MaskedJoin.Goal : Prop := maskedJoin search

def ComplementMass.Goal : Prop := ∀ u t x ys, valid u ys → fan search u t x ys + ys.countP (fun y => x≤y+t && y≤x+t) = ys.length

end Apart

#guard seek AdjacentBucket.search 12 2 4 [0,1,3,6,6,7,11] == [0,1,6,6,7]
#guard fan ReciprocalPrice.search 8 1 0 [0,2,7] == 3

end TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
