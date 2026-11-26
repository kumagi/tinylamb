import Std

/-!
These claims relate dominance testing to the modeled skyline anti-join. A SQL executor
can reduce candidate comparisons while retaining all nondominated rows.

Modeled claims

- `Goal` — `∀ rows, (sweep rows).Perm (reference rows)`
-/

namespace TinylambOpt.ResearchGoals.SkylineAntiJoin

structure Row where
  x : Option Int
  y : Option Int
  id : Nat
  deriving DecidableEq

def dominates (a b : Row) : Bool :=
  match a.x, a.y, b.x, b.y with
  | some x, some y, some u, some v => x < u && y < v
  | _, _, _, _ => false

def reference (rows : List Row) := rows.filter fun b => !rows.any (dominates · b)

def lower (old : Option Int) (y : Int) := some (min (old.getD y) y)

structure State where
  group : Option Int := none
  groupMin : Option Int := none
  previousMin : Option Int := none
  kept : List Row := []
  deriving Inhabited

def visit (s : State) (r : Row) : State :=
  match r.x, r.y with
  | some x, some y =>
    let changed := s.group != some x
    let prev := if changed then s.groupMin.elim s.previousMin (lower s.previousMin) else s.previousMin
    let localMin := lower (if changed then none else s.groupMin) y
    let keep := !(prev.any (· < y))
    ⟨some x, localMin, prev, if keep then r :: s.kept else s.kept⟩
  | _, _ => {s with kept := r :: s.kept}

def sweep (rows : List Row) :=
  let sorted := rows.mergeSort (fun a b => decide (a.x.getD 0 ≤ b.x.getD 0))
  (sorted.foldl visit {}).kept

def Goal : Prop := ∀ rows, (sweep rows).Perm (reference rows)

private def sample : List Row :=
  [⟨some 1,some 4,1⟩, ⟨some 1,some 5,2⟩, ⟨some 2,some 5,3⟩,
   ⟨some 2,some 4,4⟩, ⟨none,some 9,5⟩]
example : ((sweep sample).map Row.id).mergeSort = [1,2,4,5] := by cbv
example : (reference sample).map Row.id = [1,2,4,5] := by decide

end TinylambOpt.ResearchGoals.SkylineAntiJoin
