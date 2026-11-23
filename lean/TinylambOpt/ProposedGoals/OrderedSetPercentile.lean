import Std

namespace TinylambOpt.ProposedGoals.OrderedSetPercentile

abbrev V := Option Int

def nonNull (rows : List V) : List Int := rows.filterMap id

/-- 1-based rank (ceil of num/den * n). -/
def rankOf (num den n : Nat) : Nat :=
  if den == 0 then 0 else (num * n + den - 1) / den

def countLe (xs : List Int) (v : Int) : Nat := (xs.filter (fun x => x <= v)).length
def countLt (xs : List Int) (v : Int) : Nat := (xs.filter (fun x => x < v)).length

/-- Optimized: sort once and index the requested rank. -/
def sortedPick (rows : List V) (num den : Nat) : V :=
  let xs := (nonNull rows).mergeSort (fun a b => a <= b)
  let r := rankOf num den xs.length
  if r == 0 || xs.length < r then none else xs[r - 1]?

/-- Reference: the unique value v with count(< v) < r <= count(<= v). -/
def rankPick (rows : List V) (num den : Nat) : V :=
  let xs := nonNull rows
  let r := rankOf num den xs.length
  if r == 0 || xs.length < r then none else
    xs.find? (fun v => countLt xs v < r && r <= countLe xs v)

def Goal : Prop := ∀ (rows : List V) (num den : Nat),
    den > 0 → num <= den →
      sortedPick rows num den = rankPick rows num den

example : rankOf 1 2 4 = 2 := by native_decide
example : sortedPick [some 3, none, some 1, some 2] 1 2 = some 2 := by native_decide
example : rankPick [some 3, none, some 1, some 2] 1 2 = some 2 := by native_decide
example : sortedPick [some 1, some 1, some 5] 3 4 = some 5 := by native_decide
example : sortedPick [some 1, some 1, some 5] 0 1 = none := by native_decide

end TinylambOpt.ProposedGoals.OrderedSetPercentile
