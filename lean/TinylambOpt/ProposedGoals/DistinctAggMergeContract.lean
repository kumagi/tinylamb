import Std

/-!
Keep the first occurrence of each distinct value (NULL is one group).

A partial state is the exact set of distinct values seen so far.

Modeled claims

- `Goal` — `∀ (parts : List (List V)), let rows := parts.flatten let merged :=
  mergePartial parts values merged = values rows ∧ sumValues merged = sumValues rows`
-/

namespace TinylambOpt.ProposedGoals.DistinctAggMergeContract

abbrev V := Option Int

def appears (x : V) : List V → Bool
  | [] => false
  | y :: ys => x == y || appears x ys

/-- Keep the first occurrence of each distinct value (NULL is one group). -/
def dedup : List V → List V
  | [] => []
  | x :: xs => x :: (dedup xs).filter (fun y => y != x)

def addOpt (a : Option Int) (v : V) : Option Int :=
  match v with | none => a | some x => some (a.getD 0 + x)

def values (rows : List V) : Nat := (dedup rows).length
def sumValues (rows : List V) : Option Int := (dedup rows).foldl addOpt none

/-- A partial state is the exact set of distinct values seen so far. -/
def mergePartial (parts : List (List V)) : List V :=
  dedup (parts.foldl (fun acc p => acc ++ dedup p) [])

/-- Naive plan: merge only the scalar counts; this is deliberately WRONG when
    partitions overlap and is registered as the regression fixture. -/
def naiveCountSum (parts : List (List V)) : Nat :=
  (parts.map (fun p => values p)).sum

def Goal : Prop :=
  ∀ (parts : List (List V)),
    let rows := parts.flatten
    let merged := mergePartial parts
    values merged = values rows ∧ sumValues merged = sumValues rows

example : dedup [some 1, none, some 1, none, some 2] = [some 1, none, some 2] := by decide

example : values [some 1, none, some 1, none, some 2] = 3 := by decide

example : naiveCountSum [[some 1, some 2], [some 2, some 3]] = 4 := by decide
example : values [[some 1, some 2], [some 2, some 3]].flatten = 3 := by decide

end TinylambOpt.ProposedGoals.DistinctAggMergeContract
