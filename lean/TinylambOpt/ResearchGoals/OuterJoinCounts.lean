import Std

namespace TinylambOpt.ResearchGoals.OuterJoinCounts

abbrev Key := Option Int
abbrev Rows := List Key

def keyEq (x y : Key) : Bool := x.isSome && x == y

-- Each projected row contains COALESCE(l.key, r.key).
def projected (l r : Rows) : Rows :=
  (l.flatMap fun x =>
    let hits := r.filter (keyEq x)
    if hits.isEmpty then [x] else hits.map fun _ => x) ++
  (r.filter fun y => !(l.any fun x => keyEq x y))

def histogram (xs : Rows) : Std.HashMap Key Nat :=
  xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) {}

-- A zero count encodes absence of the GROUP BY group.
def groupCount (l r : Std.HashMap Key Nat) (key : Key) : Nat :=
  let a := l.getD key 0
  let b := r.getD key 0
  if key.isNone || a == 0 || b == 0 then a+b else a*b

def Goal : Prop := ∀ l r key,
  groupCount (histogram l) (histogram r) key = (projected l r).count key

private def l : Rows := [none, none, some 3, some 3, some 9]
private def r : Rows := [none, none, none, some 3, some 3, some 3, some 4]
private def keys : Rows := [none, some 3, some 9, some 4, some 0]
example : keys.map (groupCount (histogram l) (histogram r)) = [5,6,1,1,0] := by cbv
example : keys.map (projected l r).count = [5,6,1,1,0] := by cbv

end TinylambOpt.ResearchGoals.OuterJoinCounts
