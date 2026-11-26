import Std

/-! Local operator models. Keys stand for canonical SQL grouping values and
order ordinals include the required collation, NULL order and tie policy.
Sums use exact widened arithmetic. Floating point paths must independently
certify equivalence at the engine's observable precision or keep their order. -/


namespace TinylambOpt.ResearchGoals.QueryRewrites

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

end TinylambOpt.ResearchGoals.QueryRewrites
