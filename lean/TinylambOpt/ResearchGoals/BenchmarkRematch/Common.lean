import Std

namespace TinylambOpt.ResearchGoals.BenchmarkRematch

/-! Local operator models. Keys stand for canonical SQL grouping values and
order ordinals include the required collation, NULL order and tie policy.
Sums use exact widened arithmetic. Floating point paths must independently
certify equivalence at the engine's observable precision or keep their order. -/
def total {α : Type} (xs : List α) (f : α → Int) := (xs.map f).sum
def frequency {α : Type} [BEq α] (xs : List α) := xs.eraseDups.map (fun x => (x,xs.count x))
def nsum (xs : List (Option Int)) : Option Int :=
  let ys := xs.filterMap id
  if ys.isEmpty then none else some ys.sum
def best (n : Nat) (xs : List Nat) := xs.mergeSort.take n
def members {α : Type} [BEq α] (a b : List α) := a.eraseDups.filter b.contains
def countWhere {α : Type} (xs : List α) (p : α → Bool) := (xs.filter p).length
def bucket (boundaries : List Int) (x : Int) := boundaries.countP (· < x)
def ranges (xs : List Nat) (start len : Nat) := (xs.mergeSort.drop start).take len

#guard nsum [none, none] == none
#guard nsum [some (-2), none, some 2] == some 0
#guard best 2 [7, 1, 1] == [1, 1]
#guard ([-1, 30, 31, 60, 61, 120, 121] : List Int).map (bucket [30,60,90,120]) == [0,0,1,1,2,3,4]

end TinylambOpt.ResearchGoals.BenchmarkRematch
