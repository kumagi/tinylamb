import TinylambOpt.ResearchGoals.Compression.Wire

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.
-/

namespace TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Each representation supplies distinct-key candidates and an independent
multiplicity kernel. SQL keys below are non-NULL naturals; missing local codes
and deleted rows are discarded by the reference. -/
structure Bag (α : Type) where
  expand : α → List Nat
  keys : α → List Nat
  mass : α → Nat → Nat
def weighted (b : Bag α) (p : α) (f : Nat → Nat) :=
  ((b.keys p).eraseDups.map (fun k => b.mass p k * f k)).sum
def live (b : Bag α) (p : α) := (b.keys p).eraseDups.filter (fun k => 0 < b.mass p k)
def pairs (xs ys : List Nat) (pred : Nat → Nat → Bool) :=
  xs.flatMap (fun x => (ys.filter (pred x)).map (x,·))
def below (b : Bag α) (p : α) (v : Nat) := weighted b p (fun x => (decide (x≤v)).toNat)
def quantile (b : Bag α) (p : α) (i : Nat) := ((live b p).mergeSort).find? (fun x => i < below b p x)
def comb (n : Nat) := n*(n-1)/2

/-- Output live grouping keys once even if physical codes alias. -/
def distinctOutput (b : Bag α) : Prop := ∀ p, (live b p).Perm (b.expand p).eraseDups

/-- Use a histogram for a filtered sum of squared values. -/
def filteredMoment (b : Bag α) : Prop := ∀ p lo hi, weighted b p (fun x => if lo≤x && x<hi then x*x else 0) =
  (((b.expand p).filter (fun x => lo≤x && x<hi)).map (fun x => x*x)).sum

/-- Find an order statistic with ties by cumulative multiplicity. -/
def orderStatistic (b : Bag α) : Prop := ∀ p i, quantile b p i = ((b.expand p).mergeSort)[i]?

/-- Evaluate band join count from the two weighted supports. -/
def joinFanout (b : Bag α) : Prop := ∀ a c slack, weighted b a (fun x => weighted b c
  (fun y => (decide (x≤y ∧ y≤x+slack)).toNat)) =
  (pairs (b.expand a) (b.expand c) (fun x y => x≤y && y≤x+slack)).length

/-- Compute revenue of all left rows whose key is absent on the right. -/
def antiRevenue (b : Bag α) : Prop := ∀ a c, weighted b a (fun x => if b.mass c x==0 then x else 0) =
  ((b.expand a).filter (fun x => !(b.expand c).contains x)).sum

/-- Count unordered equal-key pairs without materializing a self-join. -/
def selfPairs (b : Bag α) : Prop := ∀ p, ((live b p).map (fun k => comb (b.mass p k))).sum =
  (((b.expand p).zipIdx.map (fun (x,i) => ((b.expand p).drop (i+1)).count x))).sum

/-- Intersect two bags by the minimum multiplicity at each key. -/
def intersectAll (b : Bag α) : Prop := ∀ a c k, ((live b a).flatMap (fun x => List.replicate
  (min (b.mass a x) (b.mass c x)) x)).count k = min ((b.expand a).count k) ((b.expand c).count k)

/-- LEFT JOIN contributes one unmatched right placeholder or the complete right multiplicity. -/
def outerWeight (b : Bag α) : Prop := ∀ a c, weighted b a (fun x => max 1 (b.mass c x)) =
  (pairs (b.expand a) (b.expand c) (· == ·)).length +
  ((b.expand a).filter (fun x => !(b.expand c).contains x)).length

namespace MultiplicityRuns

/-- Value/run-length pairs with zero runs and split duplicates. -/
def bag : Bag (List (Nat × Nat)) where
  expand := fun rs => rs.flatMap (fun (x,n) => List.replicate n x)
  keys := fun rs => rs.map Prod.fst
  mass := fun rs k => ((rs.filter (fun r => r.1==k)).map Prod.snd).sum

def WeightedOrderStatistic.Goal : Prop :=
  ∀ x y a b i, x<y → i<a+b → quantile bag [(x,a),(y,b)] i = some (if i<a then x else y)
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def ZeroRuns.Goal : Prop := ∀ rs, live bag (rs.filter (fun r => 0<r.2)) |>.Perm (live bag rs)

end MultiplicityRuns

namespace IntervalCover

/-- Overlapping half-open ranges contribute one occurrence per covering interval. -/
def bag : Bag (List (Nat × Nat)) where
  expand := fun rs => rs.flatMap (fun (s,n) => (List.range n).map (s+·))
  keys := fun rs => List.range (peak (rs.map (fun (s,n) => s+n)))
  mass := fun rs k => rs.countP (fun (s,n) => s≤k && k<s+n)

def ClipRanges.Goal : Prop :=
  ∀ rs lim, weighted bag rs (fun x => (decide (x<lim)).toNat) = (rs.map (fun (s,n) => min n (lim-s))).sum
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def CoverageIntegral.Goal : Prop := ∀ rs, weighted bag rs (fun _ => 1) = (rs.map Prod.snd).sum

end IntervalCover

namespace AdditiveProduct

/-- Represent a Cartesian product by arrays of additive keys. -/
def bag : Bag (List Nat × List Nat) where
  expand := fun (a,b) => a.flatMap (fun x => b.map (x+·))
  keys := fun (a,b) => a.eraseDups.flatMap (fun x => b.eraseDups.map (x+·))
  mass := fun (a,b) k => (a.eraseDups.map (fun x => if x≤k then a.count x * b.count (k-x) else 0)).sum

def SecondMoment.Goal : Prop :=
  ∀ a b, weighted bag (a,b) (fun x => x*x) = b.length*(a.map (fun x => x*x)).sum+2*a.sum*b.sum+a.length*(b.map (fun x => x*x)).sum
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def ProductRevenue.Goal : Prop := ∀ a b, weighted bag (a,b) id = a.sum*b.length + b.sum*a.length

end AdditiveProduct

namespace DictionaryAliases

/-- Multiple dictionary slots may encode the same key; invalid slots are invisible. -/
def bag : Bag (List Nat × List Nat) where
  expand := fun (d,ids) => ids.filterMap (fun i => d[i]?)
  keys := Prod.fst
  mass := fun (d,ids) k => ((d.zipIdx.filter (fun e => e.1==k)).map (fun e => ids.count e.2)).sum

def ManyToOne.Goal : Prop :=
  ∀ d ids v, bag.mass (d++[v],ids) v = bag.mass (d,ids) v + ids.count d.length
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def UnusedSuffix.Goal : Prop := ∀ d ids ext, (∀ i∈ids, i<d.length) → weighted bag (d++ext,ids) id = weighted bag (d,ids) id

end DictionaryAliases

namespace ExceptionOverlay

/-- Default key with sparse ordinal overrides. First patch wins on overlapping positions. -/
def bag : Bag (Nat × Nat × List (Nat × Nat)) where
  expand := fun (n,d,es) => (List.range n).map (fun i => ((es.lookup i).getD d))
  keys := fun (_,d,es) => d::es.map Prod.snd
  mass := fun (n,d,es) k =>
    let ps := (es.map Prod.fst).eraseDups.filter (· < n)
    (if k == d then n-ps.length else 0) + ps.countP (fun i => es.lookup i == some k)

def OverrideHead.Goal : Prop :=
  ∀ n d es i x k, i<n →
  let old := (es.lookup i).getD d
  bag.mass (n,d,(i,x)::es) k + (decide (old=k)).toNat = bag.mass (n,d,es) k + (decide (x=k)).toNat
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def IgnoreTail.Goal : Prop := ∀ n d es i v, n≤i → weighted bag (n,d,es++[(i,v)]) id = weighted bag (n,d,es) id

end ExceptionOverlay

namespace RotationCycle

/-- Repeated residue cycle with a partial last rotation. -/
def bag : Bag (Nat × Nat × Nat) where
  expand := fun (n,m,s) => (List.range n).map (fun i => (s+i)%(m+1))
  keys := fun (_,m,_) => List.range (m+1)
  mass := fun (n,m,s) k => if k≤m then n/(m+1)+(decide ((k+(m+1)-s%(m+1))%(m+1)<n%(m+1))).toNat else 0

def RangePerCycle.Goal : Prop :=
  ∀ m k s lo hi, lo≤hi → hi≤m+1 → weighted bag (k*(m+1),m,s) (fun v => (decide (lo≤v ∧ v<hi)).toNat) = k*(hi-lo)
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def CompleteTurns.Goal : Prop := ∀ n m s k, bag.mass ((n+1)*(m+1),m,s) k = if k≤m then n+1 else 0

end RotationCycle

namespace EdgeDegrees

/-- Each undirected edge contributes both endpoints, including loops twice. -/
def bag : Bag (List (Nat × Nat)) where
  expand := fun es => es.flatMap (fun (a,b) => [a,b])
  keys := fun es => es.map Prod.fst ++ es.map Prod.snd
  mass := fun es k => (es.map Prod.fst).count k+(es.map Prod.snd).count k

def CutWeight.Goal : Prop :=
  ∀ es (inside : Nat → Bool), weighted bag es (fun v => (inside v).toNat) =
    2*es.countP (fun (a,b) => inside a && inside b)+es.countP (fun (a,b) => xor (inside a) (inside b))
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def LoopDegree.Goal : Prop := ∀ es x, bag.mass ((x,x)::es) x = bag.mass es x+2

end EdgeDegrees

namespace RepeatedSlices

/-- All suffixes share their storage; the ordinal determines repetition count. -/
def bag : Bag (List Nat) where
  expand := fun xs => (List.range xs.length).flatMap (fun i => xs.drop i)
  keys := id
  mass := fun xs k => ((xs.zipIdx.filter (fun e => e.1==k)).map (fun e => e.2+1)).sum

def AppendedSuffixes.Goal : Prop :=
  ∀ a b k, bag.mass (a++b) k = bag.mass a k + bag.mass b k + a.length*b.count k
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def TriangularSize.Goal : Prop := ∀ xs, weighted bag xs (fun _ => 1) = xs.length*(xs.length+1)/2

end RepeatedSlices

namespace DeletedOrdinals

/-- Suppress rows by ordinal even if the delete list contains repeated keys or ordinals. -/
def bag : Bag (List Nat × List Nat) where
  expand := fun (xs,ds) => (xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst
  keys := Prod.fst
  mass := fun (xs,ds) k => xs.count k - ds.eraseDups.countP (fun i => xs[i]? == some k)

def PrefixOfLive.Goal : Prop :=
  ∀ xs ds t k, bag.mass (xs.take t,ds) k + bag.mass (xs.drop t,(ds.filter (· ≥ t)).map (·-t)) k = bag.mass (xs,ds) k
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def DeleteFootprint.Goal : Prop := ∀ xs ds, weighted bag (xs,ds) (fun _ => 1) = xs.length-(ds.eraseDups.filter (· < xs.length)).length

end DeletedOrdinals

namespace CoGroup

/-- Matching runs from two streams encode the Cartesian product of payload frequencies. -/
def bag : Bag (List Nat × List Nat) where
  expand := fun (xs,ys) => xs.flatMap (fun x => ys.filter (· == x))
  keys := Prod.fst
  mass := fun (xs,ys) k => xs.count k*ys.count k

def SharedProbe.Goal : Prop :=
  ∀ a b c, weighted bag (a,b) (fun k => c.count k) = weighted bag (b,c) (fun k => a.count k)
def DistinctOutput.Goal : Prop := distinctOutput bag
def FilteredMoment.Goal : Prop := filteredMoment bag
def OrderStatistic.Goal : Prop := orderStatistic bag
def JoinFanout.Goal : Prop := joinFanout bag
def AntiRevenue.Goal : Prop := antiRevenue bag
def SelfPairs.Goal : Prop := selfPairs bag
def IntersectAll.Goal : Prop := intersectAll bag
def OuterWeight.Goal : Prop := outerWeight bag

def BuildSwap.Goal : Prop := ∀ a b k, bag.mass (a,b) k = bag.mass (b,a) k

end CoGroup

#guard weighted ExceptionOverlay.bag (5,3,[(1,7),(1,9),(12,4)]) id == 19
#guard quantile RotationCycle.bag (5,2,1) 2 == some 1

end TinylambOpt.ResearchGoals.StorageExecution.BagKernels
