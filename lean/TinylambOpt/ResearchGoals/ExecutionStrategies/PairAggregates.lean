import Std

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.
-/

namespace TinylambOpt.ResearchGoals.PairAggregates

/-! Exact widened integer accumulators. Results are COUNT or COALESCE(SUM(...),0).
Nullable arithmetic inputs are the non-NULL projection before these kernels. -/

def pairs (xs ys : List Int) (f : Int → Int → Int) : Int :=
  (xs.map fun x => (ys.map (f x)).sum).sum

/-- One pass over sorted values for the unordered absolute-distance self join. -/
def distanceScan (xs : List Int) : Int :=
  let (_, _, total_) := xs.foldl (fun (n, s, acc) x => (n+1, s+x, acc+n*x-s)) (0, 0, 0)
  total_

def AbsoluteDistance.Goal : Prop := ∀ xs,
  2 * distanceScan (xs.mergeSort) = pairs xs xs (fun x y => (x-y).natAbs)

/-- Cross-table squared distance needs only three moments per side. -/
def squares (xs : List Int) := (xs.map fun x => x*x).sum

def SquaredDistance.Goal : Prop := ∀ xs ys,
  (ys.length : Int)*squares xs + (xs.length : Int)*squares ys - 2*xs.sum*ys.sum =
    pairs xs ys (fun x y => (x-y)^2)

abbrev Row := Int × Int

def hist {α : Type} [BEq α] [Hashable α] (xs : List α) : Std.HashMap α Nat :=
  xs.foldl (fun h x => h.insert x (h.getD x 0 + 1)) {}

/-- Equality on tenant, inequality on user: subtract the diagonal per left row. -/
def differentUsers (l r : List Row) :=
  let tenants := hist (r.map Prod.fst)
  let users := hist r
  (l.map fun x => tenants.getD x.1 0 - users.getD x 0).sum

def ResidualInequality.Goal : Prop := ∀ l r,
  differentUsers l r = (l.map fun x => (r.filter fun y => x.1 == y.1 && x.2 != y.2).length).sum

/-- Unordered pairs with equal group and unequal value, counted once per pair of positions. -/
def choose2 (n : Nat) := n*(n-1)/2

def SameGroupPairs.Goal : Prop := ∀ (xs : List Row),
  ((hist (xs.map Prod.fst)).toList.map (fun e => choose2 e.2)).sum -
      ((hist xs).toList.map (fun e => choose2 e.2)).sum =
    (xs.zipIdx.map fun (x,i) => ((xs.drop (i+1)).filter fun y => x.1 == y.1 && x.2 != y.2).length).sum

/-- Pair sums equal to a target, reusing a complement-frequency table for every probe. -/
def ComplementProbe.Goal : Prop := ∀ (xs ys : List Int) (t : Int),
  (let h := hist ys; (xs.map fun x => h.getD (t-x) 0).sum) =
    (xs.map fun x => (ys.filter fun y => x+y == t).length).sum

/-- Strictly increasing three-row chains anchored at their middle occurrence. -/
def anchoredTriples (a b c : List Int) :=
  let weights := (hist b).toList
  (weights.map fun (k,n) => n * (a.filter (· < k)).length * (c.filter (k < ·)).length).sum

def IncreasingTriples.Goal : Prop := ∀ a b c,
  anchoredTriples a b c = (a.map fun x => (b.map fun y =>
    (c.filter fun z => x < y && y < z).length).sum).sum

/-- Pairwise bitwise XOR distance from per-bit population counts (unsigned byte payload). -/
def XorDistance.Goal : Prop := ∀ (xs ys : List UInt8),
  ((List.range 8).map fun i =>
    let a := (xs.filter fun x => x.toNat.testBit i).length
    let b := (ys.filter fun y => y.toNat.testBit i).length
    2^i * (a*(ys.length-b) + (xs.length-a)*b)).sum =
    (xs.map fun x => (ys.map fun y => (x ^^^ y).toNat).sum).sum

/-- Sum of pair minima over nonnegative values by integrating two survival histograms.
Useful on compact domains when the suffix counts are materialized once. -/
def LayerCakeMinimum.Goal : Prop := ∀ (a b : List Nat) (cap : Nat),
  (∀ x ∈ a, x ≤ cap) →
  ((List.range cap).map fun t => (a.filter (t < ·)).length * (b.filter (t < ·)).length).sum =
    (a.map fun x => (b.map (min x)).sum).sum

#guard distanceScan [-2,-2,3] == 10
#guard squares [-2,3] == 13
#guard differentUsers [(1,2),(1,2)] [(1,2),(1,3),(2,3)] == 2
#guard choose2 3 - choose2 2 == 2
#guard (hist [-1,-1,2]).getD (-1 : Int) 0 == 2
#guard anchoredTriples [0,1] [1,1] [1,2,3] == 4
#guard (0x80 ^^^ (0x7f : UInt8)).toNat == 255
#guard ([0,1].map fun t => ([0,2].filter (t < ·)).length * ([5].filter (t < ·)).length).sum == 2

end TinylambOpt.ResearchGoals.PairAggregates
