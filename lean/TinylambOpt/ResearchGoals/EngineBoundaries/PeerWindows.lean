import Std

namespace TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-! A single window partition. Keys are projected SQL ordering values with
NULLS FIRST; duplicate rows retain their multiplicity. Fractions below are
exact numerator/denominator pairs before conversion to a SQL numeric type. -/
abbrev Key := Option Int
def before (a b : Key) := compare a b == .lt
def lower (xs : List Key) (k : Key) := xs.filter (before · k)
def rankOf (xs : List Key) (k : Key) := 1 + (lower xs k).length
def dense (xs : List Key) (k : Key) := 1 + (lower xs k).eraseDups.length
def cume (xs : List Key) (k : Key) := ((lower xs k).length + xs.count k, xs.length)
def percentRank (xs : List Key) (k : Key) :=
  if xs.length ≤ 1 then (0,1) else (rankOf xs k - 1, xs.length - 1)

/-- Bucket counts differ by at most one; distribute remainders to the first buckets.
Inputs to SQL NTILE below satisfy 0 < buckets and ordinal < partition size. -/
def tile (n buckets ordinal : Nat) :=
  let small := n / buckets
  let extra := n % buckets
  let frontier := extra * (small+1)
  if ordinal < frontier then 1 + ordinal / (small+1)
  else 1 + extra + (ordinal-frontier) / small

def tileSize (n b id : Nat) := ((List.range n).filter (fun i => tile n b i == id)).length

/-- RANK can merge arbitrary shards by summing the two counts of strict predecessors. -/
def MergeRank.Goal : Prop := ∀ a b k, rankOf (a++b) k + 1 = rankOf a k + rankOf b k

/-- Merging DENSE_RANK summaries subtracts the distinct predecessors shared by both shards. -/
def DenseOverlap.Goal : Prop := ∀ a b k,
  let overlap := (lower a k).eraseDups.filter ((lower b k).contains ·)
  dense (a++b) k + 1 + overlap.length = dense a k + dense b k

/-- Peer compression keeps CUME_DIST exact only if the peer run contributes its full weight. -/
def CumeFromRuns.Goal : Prop := ∀ xs k, (cume xs k).1 =
  ((xs.eraseDups.filter (fun x => before x k || x == k)).map (xs.count ·)).sum

/-- Tile boundaries can cut a peer group even though rank-based windows agree on the peer. -/
def PeerConstantTile.Goal : Prop := ∀ (xs : List Key) i j k b, 0 < b →
  xs[i]? = some k → xs[j]? = some k → tile xs.length b i = tile xs.length b j

/-- The commonly proposed scaled-row formula describes every NTILE bucket. -/
def ProportionalTile.Goal : Prop := ∀ n b i, 0 < b → i < n → tile n b i = 1 + i*b/n

/-- Exact bucket occupancy also handles more buckets than rows. -/
def BucketOccupancy.Goal : Prop := ∀ n b k, 0 < k → k ≤ b →
  tileSize n b k = n/b + (decide (k ≤ n%b)).toNat

/-- A cutoff on NTILE can become a bounded prefix read with no tile computations per row. -/
def TilePrefix.Goal : Prop := ∀ n b i k, i < n → 0 < b → k ≤ b →
  (tile n b i ≤ k ↔ i < k*(n/b) + min k (n%b))

/-- For a present key, rank and dense rank coincide exactly without repeated smaller keys. -/
def CollapseRanks.Goal : Prop := ∀ xs k, k ∈ xs →
  (rankOf xs k = dense xs k ↔ (lower xs k).Nodup)

/-- Appending later peers preserves PERCENT_RANK's numerator but rescales its denominator. -/
def PercentDenominator.Goal : Prop := ∀ xs more k, 1 < xs.length →
  (∀ x ∈ more, before x k = false) →
  percentRank (xs++more) k = ((percentRank xs k).1, (percentRank xs k).2 + more.length)

/-- NULL peers at the front all have rank one; their cumulative fraction counts the full run. -/
def NullPeerFront.Goal : Prop := ∀ xs, none ∈ xs →
  dense xs none = 1 ∧ rankOf xs none = 1 ∧ cume xs none = (xs.count none, xs.length)

#guard ((List.range 6).map (tile 6 4)) == [1,1,2,2,3,4]
#guard 1 + 3*4/6 == 3
#guard cume [none,none,some 9] none == (2,3)

end TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows
