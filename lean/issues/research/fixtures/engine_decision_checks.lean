import TinylambOpt.ResearchGoals.EngineDecisions

/-!
This fixture compares candidate engine decisions with reference evaluations on concrete
inputs. It exercises the boundary cases used in the SQL execution study.
-/

open TinylambOpt.ResearchGoals.EngineDecisions

namespace EngineDecisionChecks

private def words {α : Type} (alphabet : List α) : Nat → List (List α)
  | 0 => [[]]
  | n+1 => alphabet.flatMap fun x => (words alphabet n).map (x::·)

private def upTo {α : Type} (alphabet : List α) (n : Nat) :=
  (List.range (n+1)).flatMap (words alphabet)

private def nullable : List (Option Nat) := [none,some 0,some 1]
private def keys := nullable.flatMap (fun a => nullable.map (a,·))
private def rhs := upTo keys 2
#guard keys.length * rhs.length == 819
#guard rhs.all fun rs =>
  keys.all fun q => Optimizer.TupleAnti.keep (Optimizer.TupleAnti.build rs) q ==
    Optimizer.TupleAnti.whereRef q rs

-- Exhaust every shuffle of two owners, keeping each owner's undo order.
private def shuffles : List Int → List Int → List (List Int)
  | [], ys => [ys]
  | xs, [] => [xs]
  | x::xs, y::ys =>
    ((shuffles xs (y::ys)).map (x::·)) ++ ((shuffles (x::xs) ys).map (y::·))
termination_by xs ys => xs.length + ys.length

private def owners := upTo ([-2,1,3] : List Int) 2
#guard owners.length * owners.length == 169
#guard owners.all fun xs => owners.all fun ys =>
  ((shuffles xs ys).map MVCC.UndoPeak.peak).max? ==
    some (MVCC.UndoPeak.peak xs + MVCC.UndoPeak.peak ys)

private def sortedRuns := (upTo ([-1,0,1] : List Int) 4).filter
  (fun xs => decide (xs.Pairwise (· ≤ ·)))
#guard sortedRuns.length * sortedRuns.length == 1225
#guard sortedRuns.all fun left => sortedRuns.all fun right =>
  let (ranges,work) := Executor.RangeFingers.sweep 0 0 left right right
  ranges == left.map (fun x => (right.countP (· < x),right.countP (· ≤ x))) &&
    work ≤ 2*(left.length+right.length)

end EngineDecisionChecks
