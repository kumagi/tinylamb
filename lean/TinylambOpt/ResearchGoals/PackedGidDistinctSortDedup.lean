import Std

/-!
Path A: pack `(gid, val)`, sort lexicographically, deduplicate adjacent pairs, and count
per `gid`.

Path B: partition non-null rows across `W` workers by value hash `h v % W` so per-worker
distinct sets per `gid` are disjoint and sum directly with zero cross-worker set
merging.

Modeled claims

- `Goal` — Zero-merge `COUNT(DISTINCT)` strategies (TPC-H Q16, ClickBench Q09/Q12/Q14):
  (1) sorting packed `(gid, val)` pairs and adjacent-deduping matches exact
  `COUNT(DISTINCT val)`, and (2) partitioning non-null values by `h v % W` makes
  worker local distinct counts disjoint so summing equals the global distinct count.
-/

namespace TinylambOpt.ResearchGoals.PackedGidDistinctSortDedup

abbrev Row := Nat × Option Int

def nonNullPairs (rows : List Row) : List (Nat × Int) :=
  rows.filterMap fun (gid, ov) => ov.map (fun v => (gid, v))

def refDistinctCount (rows : List Row) (gid : Nat) : Nat :=
  let vals := (nonNullPairs rows).filterMap (fun (g, v) => if g == gid then some v else none)
  let uniq := vals.foldl (fun acc v => if acc.contains v then acc else acc ++ [v]) []
  uniq.length

def lePair (a b : Nat × Int) : Bool :=
  if a.1 != b.1 then decide (a.1 ≤ b.1) else decide (a.2 ≤ b.2)

def insertSortedPair (x : Nat × Int) : List (Nat × Int) → List (Nat × Int)
  | [] => [x]
  | y :: ys => if lePair x y then x :: y :: ys else y :: insertSortedPair x ys

def sortPairs : List (Nat × Int) → List (Nat × Int)
  | [] => []
  | x :: xs => insertSortedPair x (sortPairs xs)

def dedupAdjacent : List (Nat × Int) → List (Nat × Int)
  | [] => []
  | [x] => [x]
  | x :: y :: rest =>
      if x == y then dedupAdjacent (y :: rest) else x :: dedupAdjacent (y :: rest)

/-- Path A: pack `(gid, val)`, sort lexicographically, deduplicate adjacent
    pairs, and count per `gid`. -/
def sortDedupCount (rows : List Row) (gid : Nat) : Nat :=
  let sorted := sortPairs (nonNullPairs rows)
  let uniq := dedupAdjacent sorted
  (uniq.filter (fun p => p.1 == gid)).length

/-- Path B: partition non-null rows across `W` workers by value hash `h v % W`
    so per-worker distinct sets per `gid` are disjoint and sum directly with
    zero cross-worker set merging. -/
def hashPartitionCount (w : Nat) (h : Int → Nat) (rows : List Row) (gid : Nat) : Nat :=
  let pairs := nonNullPairs rows
  let shards := (List.range w).map fun shard =>
    pairs.filter (fun (_, v) => h v % w == shard)
  let counts := shards.map fun sh =>
    refDistinctCount (sh.map fun (g, v) => (g, some v)) gid
  counts.sum

/-- Zero-merge `COUNT(DISTINCT)` strategies (TPC-H Q16, ClickBench Q09/Q12/Q14):
    (1) sorting packed `(gid, val)` pairs and adjacent-deduping matches exact
    `COUNT(DISTINCT val)`, and (2) partitioning non-null values by `h v % W`
    makes worker local distinct counts disjoint so summing equals the global
    distinct count. -/
def Goal : Prop :=
  ∀ (w : Nat) (h : Int → Nat) (rows : List Row) (gid : Nat),
    0 < w →
    sortDedupCount rows gid = refDistinctCount rows gid ∧
    hashPartitionCount w h rows gid = refDistinctCount rows gid

example :
    let rows : List Row :=
      [(1, some 10), (1, none), (1, some 10), (2, some 10), (1, some 20), (2, none)]
    sortDedupCount rows 1 = 2 ∧
    hashPartitionCount 3 (fun i => i.natAbs) rows 1 = 2 ∧
    refDistinctCount rows 1 = 2 ∧
    sortDedupCount rows 2 = 1 ∧
    sortDedupCount rows 9 = 0 := by
  decide

example :
    sortDedupCount [(1, none), (1, none)] 1 = 0 ∧
    hashPartitionCount 2 (fun i => i.natAbs) [(1, none)] 1 = 0 := by
  decide

end TinylambOpt.ResearchGoals.PackedGidDistinctSortDedup
