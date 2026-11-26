import Std

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.
-/

namespace TinylambOpt.ResearchGoals.FrameIndexes

/-! One partition in deterministic SQL order. Frame indices are zero-based and half-open. -/

def frame {α : Type} (xs : List α) (l r : Nat) := (xs.take r).drop l

/-- Prefix counts route positions through stable zero/one partitions of a wavelet tree. -/
inductive Wave where
  | leaf
  | node (rankZero : Array Nat) (low high : Wave)

def wave (xs : List Nat) : Nat → Wave
  | 0 => .leaf
  | d+1 => if xs.isEmpty then .leaf else
    let zero := fun (x : Nat) => !(x.testBit d)
    .node ((xs.scanl (fun n x => n + (zero x).toNat) 0).toArray)
      (wave (xs.filter zero) d) (wave (xs.filter (fun x => !zero x)) d)

-- Decode the high/low branch as the corresponding bit in the result.
def quantile : Nat → Wave → Nat → Nat → Nat → Nat
  | d+1, .node z low high, l, r, k =>
    let a := z[l]?.getD 0
    let b := z[r]?.getD 0
    if k < b-a then quantile d low a b k
    else 2^d + quantile d high (l-a) (r-b) (k-(b-a))
  | _, _, _, _, _ => 0

def WaveletQuantile.Goal : Prop := ∀ (xs : List Nat) d l r k,
  (∀ x ∈ xs, x < 2^d) → r ≤ xs.length → k < r-l →
  some (quantile d (wave xs d) l r k) = (frame xs l r).mergeSort[k]?

/-- A sparse table stores minima for power-of-two widths. Two overlapping hits suffice. -/
def SparseMinimum.Goal : Prop := ∀ (xs : List Int) l r, l < r → r ≤ xs.length →
  let w := 2^((r-l).log2)
  min ((frame xs l (l+w)).min?.getD 0) ((frame xs (r-w) r).min?.getD 0) =
    (frame xs l r).min?.getD 0

/-- For a non-idempotent ordered aggregate use disjoint precomputed suffix/prefix entries
at the highest differing bit of the endpoints. The split must be strictly inside. -/
def DisjointRangeFold.Goal : Prop := ∀ (xs : List String) l r, l+1 < r → r ≤ xs.length →
  let w := 2^((l ^^^ (r-1)).log2)
  let cut := l / (2*w) * (2*w) + w
  l < cut ∧ cut < r ∧
  String.join (frame xs l cut) ++ String.join (frame xs cut r) = String.join (frame xs l r)

/-- Prefix sum and non-NULL population are stored together to distinguish an empty sum. -/
def sums (xs : List (Option Int)) :=
  (xs.scanl (fun (n,s) x => (n+x.isSome.toNat, s+x.getD 0)) (0, (0:Int))).toArray

def PrefixNullableSum.Goal : Prop := ∀ (xs : List (Option Int)) l r, l ≤ r → r ≤ xs.length →
  let p := sums xs
  let a := p[l]?.getD (0,0)
  let b := p[r]?.getD (0,0)
  let vs := (frame xs l r).filterMap id
  (if a.1 == b.1 then none else some (b.2-a.2)) =
    (if vs.isEmpty then none else some vs.sum)

/-- For an ordered frame EXCLUDE GROUP removes the contiguous peer interval [a,b).
Translate the requested surviving rank to one seek in the original frame. -/
def ExcludePeerSelect.Goal : Prop := ∀ (xs : List Int) a b k, a ≤ b → b ≤ xs.length →
  xs[if k < a then k else k+(b-a)]? = (xs.take a ++ xs.drop b)[k]?

/-- NTILE sizes differ by one; the larger buckets precede the smaller ones. -/
def sizes (n m : Nat) := (List.range m).flatMap fun i =>
  List.replicate (n/m + (decide (i < n%m)).toNat) (i+1)

def tile (n m i : Nat) :=
  let q := n/m
  let big := n%m*(q+1)
  if i < big then i/(q+1)+1 else n%m+(i-big)/q+1

def TileArithmetic.Goal : Prop := ∀ n m i, 0 < m → i < n → some (tile n m i) = (sizes n m)[i]?

/-- A decreasing-value stack is unnecessary for minima. Keep increasing candidates
(oldest first), expire old positions and discard dominated suffix entries. -/
def pushMin (q : List (Nat × Int)) (i width : Nat) (v : Int) :=
  ((q.dropWhile fun e => e.1 + width ≤ i).takeWhile fun e => e.2 < v) ++ [(i,v)]

def runningMin (xs : List Int) (w : Nat) :=
  ((xs.zipIdx.foldl (fun (q,out) (v,i) =>
    let next := pushMin q i w v
    (next, (next.head?.map Prod.snd)::out)) ([], [])).2).reverse

def SlidingMinimum.Goal : Prop := ∀ xs w, 0 < w → runningMin xs w =
  (List.range xs.length).map fun i => (frame xs (i+1-w) (i+1)).min?

/-- Local session counts for an ordered event stream need only the adjacent timestamps
at a chunk boundary to reconcile a session split by the morsel. -/
def sessions (gap : Nat) : List Nat → Nat
  | [] => 0
  | x :: xs => 1 + ((x :: xs).zip xs).countP (fun (a,b) => a+gap < b)

def SessionBoundary.Goal : Prop := ∀ gap (a b : List Nat),
  sessions gap (a ++ b) = sessions gap a + sessions gap b -
    (match a.getLast?, b.head? with
     | some x, some y => (decide (y ≤ x+gap)).toNat
     | _, _ => 0)

/-- A run directory supplies the rank and inclusive cumulative numerator of every peer.
NULLS FIRST is represented by a preceding canonical integer rank when applicable. -/
def rankRuns (runs : List (Int × Nat)) :=
  (runs.foldl (fun (n,out) (_,m) => (n+m, List.replicate m (n+1,n+m) :: out))
    (0, [])).2.reverse.flatten

def PeerRankRuns.Goal : Prop := ∀ (runs : List (Int × Nat)),
  (runs.map Prod.fst).Pairwise (· < ·) →
  let xs := runs.flatMap fun (k,m) => List.replicate m k
  rankRuns runs = xs.map fun x => (1+(xs.filter (· < x)).length, (xs.filter (· ≤ x)).length)

#guard quantile 3 (wave [7,1,7,0] 3) 1 4 1 == 1
#guard (frame [-7,-2,-9] 1 3).min? == some (-9 : Int)
#guard (2 ^ ((3 ^^^ (6-1) : Nat).log2)) == 4
#guard (sums [none,some 2,some (-2)]).back? == some (2,0)
#guard ([1,2,2,5].take 1 ++ [1,2,2,5].drop 3)[1]? == some 5
#guard (List.range 5).map (tile 5 3) == [1,1,2,2,3]
#guard runningMin [2,1,1,3,4] 2 == [some 2,some 1,some 1,some 1,some 3]
#guard sessions 2 [0,2,5] == 2
#guard rankRuns [(1,0),(4,2),(7,1)] == [(1,2),(1,2),(3,3)]

end TinylambOpt.ResearchGoals.FrameIndexes
