import Std

/-!
Naive average merging (`(avg1 + avg2) / 2`) fails when worker tuple counts differ.

Modeled claims

- `Goal` — Two-level radix partitioned parallel aggregate merge
  (`executor/detail/typed_stream_aggregate_partitioned.cpp`, ClickBench Q14/17/
  19/29/32/34/35): routing worker-local `(count, sum)` maps to `numParts` disjoint
  partitions by `hash k % numParts` yields zero duplicate output keys across
  partitions and exact parity with single-thread aggregation.
-/

namespace TinylambOpt.ResearchGoals.TwoLevelPartitionedAggMerge

abbrev Key := Option Int

def addCountSum (m : List (Key × (Nat × Int))) (k : Key) (cnt : Nat) (sum : Int) :
    List (Key × (Nat × Int)) :=
  match m with
  | [] => [(k, (cnt, sum))]
  | (k', (c, s)) :: rest =>
      if k' == k then (k', (c + cnt, s + sum)) :: rest
      else (k', (c, s)) :: addCountSum rest k cnt sum

def lookupCountSum (m : List (Key × (Nat × Int))) (k : Key) : Option (Nat × Int) :=
  match m with
  | [] => none
  | (k', cs) :: rest => if k' == k then some cs else lookupCountSum rest k

def refAgg (rows : List (Key × Int)) : List (Key × (Nat × Int)) :=
  rows.foldl (fun acc (k, v) => addCountSum acc k 1 v) []

def twoLevelAgg (numParts : Nat) (hash : Key → Nat)
    (chunks : List (List (Key × Int))) : List (List (Key × (Nat × Int))) :=
  let workerMaps := chunks.map refAgg
  (List.range numParts).map fun p =>
    workerMaps.foldl
      (fun pMap wMap =>
        wMap.foldl
          (fun acc (k, (c, s)) =>
            if hash k % numParts == p then addCountSum acc k c s else acc)
          pMap)
      []

def lookupPartitioned (parts : List (List (Key × (Nat × Int)))) (k : Key) :
    Option (Nat × Int) :=
  parts.foldl
    (fun acc pMap =>
      match acc with
      | some cs => some cs
      | none => lookupCountSum pMap k)
    none

/-- Two-level radix partitioned parallel aggregate merge
    (`executor/detail/typed_stream_aggregate_partitioned.cpp`, ClickBench Q14/17/
    19/29/32/34/35): routing worker-local `(count, sum)` maps to `numParts`
    disjoint partitions by `hash k % numParts` yields zero duplicate output keys
    across partitions and exact parity with single-thread aggregation. -/
def Goal : Prop :=
  ∀ (numParts : Nat) (hash : Key → Nat) (chunks : List (List (Key × Int))),
    0 < numParts →
    let parts := twoLevelAgg numParts hash chunks
    (parts.flatMap (List.map Prod.fst)).Nodup ∧
    ∀ k : Key, lookupPartitioned parts k = lookupCountSum (refAgg chunks.flatten) k

/-- Naive average merging (`(avg1 + avg2) / 2`) fails when worker tuple counts
    differ. -/
def badAvgOfAvgs (c1 : Nat) (s1 : Int) (c2 : Nat) (s2 : Int) : Int :=
  ((s1 / Int.ofNat c1) + (s2 / Int.ofNat c2)) / 2

example :
    let chunks : List (List (Key × Int)) :=
      [[(none, 10), (some 1, 2), (some 3, 5)], [(none, 4), (some 2, 7), (some 1, 8)]]
    let parts := twoLevelAgg 3 (fun | none => 0 | some i => i.natAbs) chunks
    (parts.flatMap (List.map Prod.fst)).Nodup ∧
    lookupPartitioned parts none = some (2, 14) ∧
    lookupPartitioned parts (some 1) = some (2, 10) := by
  decide

example :
    badAvgOfAvgs 1 10 3 6 = 6 ∧ (10 + 6) / (1 + 3) = 4 := by
  decide

end TinylambOpt.ResearchGoals.TwoLevelPartitionedAggMerge
