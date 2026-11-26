import TinylambOpt.ResearchGoals.Compression.Wire
import TinylambOpt.ResearchGoals.StorageExecution.Boundaries

/-!
Late materialization can gather payloads at selected row positions while preserving the
selection's order and multiplicities.
-/

namespace TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Gather planning on an immutable column. Normalize points, then group
consecutive requested ordinals into covering extents. A gap can be read and
discarded to avoid a seek. Each device model prices an inclusive extent with
its own startup, transfer, alignment or decompression rule. -/
abbrev Price := Nat → Nat → Nat
abbrev Extent := Nat × Nat
def points (xs : List Nat) := xs.eraseDups.mergeSort
def covers : List Nat → List (List Extent)
  | [] => [[]]
  | x::xs => (covers xs).flatMap (fun ps => match ps with
    | [] => [[(x,x)]]
    | (_,b)::rest => [(x,x)::ps,(x,b)::rest])
def cost (f : Price) (ps : List Extent) := (ps.map (fun (a,b) => f a b)).sum
def choose (f : Price) (xs : List Nat) :=
  ((covers (points xs)).mergeSort (fun a b => cost f a ≤ cost f b)).head?.getD []
def dp (f : Price) (ks : List Nat) :=
  let keys := ks.toArray
  boundaryCost keys.size fun a b => f (keys[a]?.getD 0) (keys[b-1]?.getD 0)
def tagged (ps : List Extent) (src : Nat → Nat) :=
  ps.flatMap (fun (a,b) => (List.range (b+1-a)).map (fun i => (a+i,src (a+i))))
def pages (k a b : Nat) := b/k-a/k+1
def span (a b : Nat) := b+1-a
def used (ps : List Extent) := (ps.map (fun (a,b) => span a b)).sum
def bestCapped (f : Price) (xs : List Nat) (cap : Nat) :=
  (((covers (points xs)).filter (fun ps => ps.all (fun (a,b) => span a b≤cap))).map (cost f)).min?

/-- Dynamic program over sorted points agrees with exhaustive minimum cost. -/
def leastCost (f : Price) : Prop := ∀ xs, let ps := points xs
  dp f ps = cost f (choose f xs)

/-- Scatter a coalesced physical read back to the original duplicate-preserving request order. -/
def gatherOrder (f : Price) : Prop := ∀ (xs : List Nat) src, xs.map (fun i => (tagged (choose f xs) src).lookup i) = xs.map (fun i => some (src i))

/-- The plan is no worse than both individual reads and a single bounding extent. -/
def sparseOrDense (f : Price) : Prop := ∀ xs, xs ≠ [] → let ks := points xs
  cost f (choose f xs) ≤ min ((ks.map (fun k => f k k)).sum) (f (ks.head?.getD 0) (ks.getLast?.getD 0))

/-- A higher fixed request fee cannot increase the number of extents chosen. -/
def seekTariff (f : Price) : Prop := ∀ xs a b, a<b → (choose (fun l r => b+f l r) xs).length ≤ (choose (fun l r => a+f l r) xs).length

/-- Charging more per decoded cell drives the optimizer toward less overread. -/
def readAmplification (f : Price) : Prop := ∀ xs a b, a<b → used (choose (fun l r => b*span l r+f l r) xs) ≤
  used (choose (fun l r => a*span l r+f l r) xs)

/-- An optimizer may use the independently planned left and right halves as an upper bound. -/
def shardBoundary (f : Price) : Prop := ∀ xs ys, (∀ x∈xs, ∀ y∈ys, x<y) →
  cost f (choose f (xs++ys)) ≤ cost f (choose f xs)+cost f (choose f ys)

/-- Relaxing the maximum extent cannot worsen minimum aggregate cost. -/
def latencyCap (f : Price) : Prop := ∀ xs a b n, a≤b → bestCapped f xs a=some n →
  ∃ m≤n, bestCapped f xs b=some m

/-- Exact break-even rule for two random probes, including intervening garbage bytes. -/
def twoRequests (f : Price) : Prop := ∀ x y, x<y → cost f (choose f [x,y]) = min (f x x+f y y) (f x y)

namespace Contiguous

/-- Byte-addressable disk with a seek charge. -/
def price (seekFee : Nat) (a b : Nat) := seekFee+span a b

def LeastCost.Goal : Prop := ∀ (seekFee : Nat), leastCost (price seekFee)
def GatherOrder.Goal : Prop := ∀ (seekFee : Nat), gatherOrder (price seekFee)
def SparseOrDense.Goal : Prop := ∀ (seekFee : Nat), sparseOrDense (price seekFee)
def SeekTariff.Goal : Prop := ∀ (seekFee : Nat), seekTariff (price seekFee)
def ReadAmplification.Goal : Prop := ∀ (seekFee : Nat), readAmplification (price seekFee)
def ShardBoundary.Goal : Prop := ∀ (seekFee : Nat), shardBoundary (price seekFee)
def LatencyCap.Goal : Prop := ∀ (seekFee : Nat), latencyCap (price seekFee)
def TwoRequests.Goal : Prop := ∀ (seekFee : Nat), twoRequests (price seekFee)
def PositiveGapRule.Goal : Prop :=
  ∀ fee xs, let ks := points xs
  let gaps := ks.zip (ks.drop 1)
  cost (price fee) (choose (price fee) xs) =
    ks.length + (if ks.isEmpty then 0 else fee) + (gaps.map (fun (a,b) => min fee (b-a-1))).sum

def BridgeGap.Goal : Prop := ∀ fee x y, x<y → (price fee x y ≤ price fee x x+price fee y y ↔ y-x-1≤fee)

end Contiguous

namespace Sector

/-- Read whole aligned sectors, including border overfetch. -/
def price (shift : Nat) (startup : Nat) (a b : Nat) := startup+pages (2^shift) a b

def LeastCost.Goal : Prop := ∀ (shift startup : Nat), leastCost (price shift startup)
def GatherOrder.Goal : Prop := ∀ (shift startup : Nat), gatherOrder (price shift startup)
def SparseOrDense.Goal : Prop := ∀ (shift startup : Nat), sparseOrDense (price shift startup)
def SeekTariff.Goal : Prop := ∀ (shift startup : Nat), seekTariff (price shift startup)
def ReadAmplification.Goal : Prop := ∀ (shift startup : Nat), readAmplification (price shift startup)
def ShardBoundary.Goal : Prop := ∀ (shift startup : Nat), shardBoundary (price shift startup)
def LatencyCap.Goal : Prop := ∀ (shift startup : Nat), latencyCap (price shift startup)
def TwoRequests.Goal : Prop := ∀ (shift startup : Nat), twoRequests (price shift startup)
def DistinctSectorFloor.Goal : Prop :=
  ∀ shift startup xs, (((points xs).map (·/2^shift)).eraseDups).length ≤ cost (price shift startup) (choose (price shift startup) xs)

def OneSector.Goal : Prop := ∀ k fee xs, xs≠[] → (∀ x∈xs, x/2^k=(xs.head?.getD 0)/2^k) → cost (price k fee) (choose (price k fee) xs) = fee+1

end Sector

namespace MirrorZones

/-- A zone crossing pays a head reposition in addition to transfer. -/
def price (zone : Nat) (penalty : Nat) (a b : Nat) := span a b + penalty*(b/(zone+1)-a/(zone+1))+1

def LeastCost.Goal : Prop := ∀ (zone penalty : Nat), leastCost (price zone penalty)
def GatherOrder.Goal : Prop := ∀ (zone penalty : Nat), gatherOrder (price zone penalty)
def SparseOrDense.Goal : Prop := ∀ (zone penalty : Nat), sparseOrDense (price zone penalty)
def SeekTariff.Goal : Prop := ∀ (zone penalty : Nat), seekTariff (price zone penalty)
def ReadAmplification.Goal : Prop := ∀ (zone penalty : Nat), readAmplification (price zone penalty)
def ShardBoundary.Goal : Prop := ∀ (zone penalty : Nat), shardBoundary (price zone penalty)
def LatencyCap.Goal : Prop := ∀ (zone penalty : Nat), latencyCap (price zone penalty)
def TwoRequests.Goal : Prop := ∀ (zone penalty : Nat), twoRequests (price zone penalty)
def JumpUnprofitable.Goal : Prop :=
  ∀ z fee x y, x<y → 1<fee → x/(z+1)<y/(z+1) → price z fee x x + price z fee y y < price z fee x y

def BoundaryTax.Goal : Prop := ∀ z h a b, a≤b → price z h a b = price z 0 a b + h*(b/(z+1)-a/(z+1))

end MirrorZones

namespace CodecRestarts

/-- Decompression starts at the preceding restart, even for a point read. -/
def price (restart : Nat) (seekFee : Nat) (a b : Nat) := seekFee + b+1-(a/(restart+1))*(restart+1)

def LeastCost.Goal : Prop := ∀ (restart seekFee : Nat), leastCost (price restart seekFee)
def GatherOrder.Goal : Prop := ∀ (restart seekFee : Nat), gatherOrder (price restart seekFee)
def SparseOrDense.Goal : Prop := ∀ (restart seekFee : Nat), sparseOrDense (price restart seekFee)
def SeekTariff.Goal : Prop := ∀ (restart seekFee : Nat), seekTariff (price restart seekFee)
def ReadAmplification.Goal : Prop := ∀ (restart seekFee : Nat), readAmplification (price restart seekFee)
def ShardBoundary.Goal : Prop := ∀ (restart seekFee : Nat), shardBoundary (price restart seekFee)
def LatencyCap.Goal : Prop := ∀ (restart seekFee : Nat), latencyCap (price restart seekFee)
def TwoRequests.Goal : Prop := ∀ (restart seekFee : Nat), twoRequests (price restart seekFee)
def SharedRestart.Goal : Prop :=
  ∀ stride fee x y, x≤y → x/(stride+1)=y/(stride+1) → price stride fee x y = price stride fee y y

def PointDecode.Goal : Prop := ∀ k fee x, price k fee x x = fee+1+x%(k+1)

end CodecRestarts

namespace TwoTier

/-- Cold prefix bytes have higher cost than cached hot suffix bytes. -/
def price (pivot : Nat) (cold : Nat) (a b : Nat) := 1+span a b+cold*(min (b+1) pivot-min a pivot)

def LeastCost.Goal : Prop := ∀ (pivot cold : Nat), leastCost (price pivot cold)
def GatherOrder.Goal : Prop := ∀ (pivot cold : Nat), gatherOrder (price pivot cold)
def SparseOrDense.Goal : Prop := ∀ (pivot cold : Nat), sparseOrDense (price pivot cold)
def SeekTariff.Goal : Prop := ∀ (pivot cold : Nat), seekTariff (price pivot cold)
def ReadAmplification.Goal : Prop := ∀ (pivot cold : Nat), readAmplification (price pivot cold)
def ShardBoundary.Goal : Prop := ∀ (pivot cold : Nat), shardBoundary (price pivot cold)
def LatencyCap.Goal : Prop := ∀ (pivot cold : Nat), latencyCap (price pivot cold)
def TwoRequests.Goal : Prop := ∀ (pivot cold : Nat), twoRequests (price pivot cold)
def SplitAtTemperature.Goal : Prop :=
  ∀ pivot cold a b, a<pivot → pivot≤b → price pivot cold a (pivot-1)+price pivot cold pivot b=price pivot cold a b+1

def HotTail.Goal : Prop := ∀ p c a b, p≤a → a≤b → price p c a b=1+span a b

end TwoTier

namespace ObjectRanges

/-- Remote service bills requests in minimum transfer units. -/
def price (unit : Nat) (fee : Nat) (a b : Nat) := fee+align (unit+1) (span a b)

def LeastCost.Goal : Prop := ∀ (unit fee : Nat), leastCost (price unit fee)
def GatherOrder.Goal : Prop := ∀ (unit fee : Nat), gatherOrder (price unit fee)
def SparseOrDense.Goal : Prop := ∀ (unit fee : Nat), sparseOrDense (price unit fee)
def SeekTariff.Goal : Prop := ∀ (unit fee : Nat), seekTariff (price unit fee)
def ReadAmplification.Goal : Prop := ∀ (unit fee : Nat), readAmplification (price unit fee)
def ShardBoundary.Goal : Prop := ∀ (unit fee : Nat), shardBoundary (price unit fee)
def LatencyCap.Goal : Prop := ∀ (unit fee : Nat), latencyCap (price unit fee)
def TwoRequests.Goal : Prop := ∀ (unit fee : Nat), twoRequests (price unit fee)
def CoalescePaidTail.Goal : Prop :=
  ∀ u f a b c, a≤b → b≤c → (span a b+u)/(u+1)=(span a c+u)/(u+1) → price u f a c=price u f a b

def MinimumBill.Goal : Prop := ∀ u f a b, a≤b → span a b≤u+1 → price u f a b=f+u+1

end ObjectRanges

namespace StripedWorkers

/-- Parallel stripes incur one activation per touched worker up to pool size. -/
def price (stripe : Nat) (workers : Nat) (a b : Nat) := span a b + min (workers+1) (pages (stripe+1) a b)

def LeastCost.Goal : Prop := ∀ (stripe workers : Nat), leastCost (price stripe workers)
def GatherOrder.Goal : Prop := ∀ (stripe workers : Nat), gatherOrder (price stripe workers)
def SparseOrDense.Goal : Prop := ∀ (stripe workers : Nat), sparseOrDense (price stripe workers)
def SeekTariff.Goal : Prop := ∀ (stripe workers : Nat), seekTariff (price stripe workers)
def ReadAmplification.Goal : Prop := ∀ (stripe workers : Nat), readAmplification (price stripe workers)
def ShardBoundary.Goal : Prop := ∀ (stripe workers : Nat), shardBoundary (price stripe workers)
def LatencyCap.Goal : Prop := ∀ (stripe workers : Nat), latencyCap (price stripe workers)
def TwoRequests.Goal : Prop := ∀ (stripe workers : Nat), twoRequests (price stripe workers)
def ActivationCeiling.Goal : Prop :=
  ∀ stripe workers a b, a≤b → price stripe workers a b ≤ span a b+workers+1

def AllWorkers.Goal : Prop := ∀ k w a b, w+1≤pages (k+1) a b → price k w a b=span a b+w+1

end StripedWorkers

namespace QuadraticDecode

/-- Model superlinear reconstruction work with a quadratic run penalty. -/
def price (fixedFee : Nat) (a b : Nat) := fixedFee+(span a b)^2

def LeastCost.Goal : Prop := ∀ (fixedFee : Nat), leastCost (price fixedFee)
def GatherOrder.Goal : Prop := ∀ (fixedFee : Nat), gatherOrder (price fixedFee)
def SparseOrDense.Goal : Prop := ∀ (fixedFee : Nat), sparseOrDense (price fixedFee)
def SeekTariff.Goal : Prop := ∀ (fixedFee : Nat), seekTariff (price fixedFee)
def ReadAmplification.Goal : Prop := ∀ (fixedFee : Nat), readAmplification (price fixedFee)
def ShardBoundary.Goal : Prop := ∀ (fixedFee : Nat), shardBoundary (price fixedFee)
def LatencyCap.Goal : Prop := ∀ (fixedFee : Nat), latencyCap (price fixedFee)
def TwoRequests.Goal : Prop := ∀ (fixedFee : Nat), twoRequests (price fixedFee)
def EqualLengthSplit.Goal : Prop :=
  ∀ fee a n, 0<n → (price fee a (a+2*n-1) ≤ price fee a (a+n-1)+price fee (a+n) (a+2*n-1) ↔ 2*n^2≤fee)

def MergeNeighbors.Goal : Prop := ∀ f a m b, a≤m → m<b → (price f a b ≤ price f a m + price f (m+1) b ↔ 2*(m+1-a)*(b-m)≤f)

end QuadraticDecode

namespace DictionaryFault

/-- Opening each dictionary bucket adds a fixed lookup or load charge. -/
def price (group : Nat) (fault : Nat) (a b : Nat) := span a b+fault*pages (group+1) a b

def LeastCost.Goal : Prop := ∀ (group fault : Nat), leastCost (price group fault)
def GatherOrder.Goal : Prop := ∀ (group fault : Nat), gatherOrder (price group fault)
def SparseOrDense.Goal : Prop := ∀ (group fault : Nat), sparseOrDense (price group fault)
def SeekTariff.Goal : Prop := ∀ (group fault : Nat), seekTariff (price group fault)
def ReadAmplification.Goal : Prop := ∀ (group fault : Nat), readAmplification (price group fault)
def ShardBoundary.Goal : Prop := ∀ (group fault : Nat), shardBoundary (price group fault)
def LatencyCap.Goal : Prop := ∀ (group fault : Nat), latencyCap (price group fault)
def TwoRequests.Goal : Prop := ∀ (group fault : Nat), twoRequests (price group fault)
def ColdBucketGap.Goal : Prop :=
  ∀ g f a b c, a≤b → b<c → b/(g+1)=c/(g+1) → price g f a c - price g f a b = c-b

def WithinDictionary.Goal : Prop := ∀ g f a b, a≤b → a/(g+1)=b/(g+1) → price g f a b=span a b+f

end DictionaryFault

namespace TieredPrefetch

/-- Small reads fit a fast buffer; long runs incur a flat spill and per-cell excess. -/
def price (limit : Nat) (spill : Nat) (a b : Nat) := 1+span a b + (if limit<span a b then spill+span a b-limit else 0)

def LeastCost.Goal : Prop := ∀ (limit spill : Nat), leastCost (price limit spill)
def GatherOrder.Goal : Prop := ∀ (limit spill : Nat), gatherOrder (price limit spill)
def SparseOrDense.Goal : Prop := ∀ (limit spill : Nat), sparseOrDense (price limit spill)
def SeekTariff.Goal : Prop := ∀ (limit spill : Nat), seekTariff (price limit spill)
def ReadAmplification.Goal : Prop := ∀ (limit spill : Nat), readAmplification (price limit spill)
def ShardBoundary.Goal : Prop := ∀ (limit spill : Nat), shardBoundary (price limit spill)
def LatencyCap.Goal : Prop := ∀ (limit spill : Nat), latencyCap (price limit spill)
def TwoRequests.Goal : Prop := ∀ (limit spill : Nat), twoRequests (price limit spill)
def AvoidSpill.Goal : Prop :=
  ∀ lim fee a n, 0<n → n≤lim → lim<2*n → price lim fee a (a+n-1)+price lim fee (a+n) (a+2*n-1) ≤ price lim fee a (a+2*n-1)

def FirstOverflow.Goal : Prop := ∀ lim fee x, price lim fee x (x+lim)=lim+fee+3

end TieredPrefetch

#guard cost (Contiguous.price 7) (choose (Contiguous.price 7) [0,4,4,20]) == 20
#guard CodecRestarts.price 7 2 10 10 == 5

end TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
