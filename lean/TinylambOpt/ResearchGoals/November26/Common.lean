import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

namespace TinylambOpt.ResearchGoals.November26

/- Keys denote canonical SQL equality classes or order ordinals. A caller must
   preserve NULL, collation, tie policy and snapshot identity when creating them.
   Int payloads are exact widened values. Option models nullable payloads. -/
def forKey {α β : Type} [BEq α] (rows : List (α × β)) (k : α) :=
  (rows.filter (fun r => r.1 == k)).map Prod.snd

def hit {α : Type} [BEq α] (x y : Option α) :=
  x.isSome && x == y

def valuesAt {α : Type} (rows : List α) (positions : List Nat) :=
  positions.filterMap fun i => rows[i]?

def sortedBy (ids : List Nat) (ordinal : Nat → Nat) :=
  ids.mergeSort (fun a b => decide (ordinal a ≤ ordinal b))

#guard hit (none : Option Nat) none == false
#guard valuesAt ["a", "b"] [1, 0, 1] == ["b", "a", "b"]
end TinylambOpt.ResearchGoals.November26
