import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q38_SparseRowPositionTitleGatherGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem countWhere_filterMap {α β : Type} [BEq β] [LawfulBEq β]
    (l : List α) (f : α → Option β) (p : β → Bool) :
    countWhere (l.filterMap f) p =
      countWhere l (fun x => match f x with | none => false | some y => p y) := by
  let q : α → Bool := fun x => match f x with | none => false | some y => p y
  change countWhere (l.filterMap f) p = countWhere l q
  induction l with
  | nil => simp [countWhere]
  | cons x xs ih =>
      unfold countWhere at ih ⊢
      cases hfx : f x with
      | none =>
          simpa [q, hfx] using ih
      | some y =>
          by_cases hpy : p y = true
          · simpa [q, hfx, hpy] using ih
          · have hpyf : p y = false := by simpa using hpy
            simpa [q, hfx, hpyf] using ih

private theorem countWhere_filter_and {α : Type} (xs : List α) (p q : α → Bool) :
    countWhere (xs.filter p) q = countWhere xs (fun x => p x && q x) := by
  simp [countWhere, List.filter_filter, Bool.and_comm]

theorem sound : Q38_SparseRowPositionTitleGatherGoal := by
  intro titles mask t
  dsimp only
  let idxs := (List.range titles.length).filter mask
  change countWhere (valuesAt titles idxs) (fun s => s == t) =
    countWhere (List.range titles.length) (fun i => mask i && (titles[i]? == some t))
  unfold valuesAt
  rw [countWhere_filterMap]
  dsimp [idxs]
  rw [countWhere_filter_and]
  apply congrArg List.length
  apply List.filter_congr
  intro i hi
  cases hti : titles[i]? with
  | none => simp
  | some s => simp

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q38_SparseRowPositionTitleGatherGoal
