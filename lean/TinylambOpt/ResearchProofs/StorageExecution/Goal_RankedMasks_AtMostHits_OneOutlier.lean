import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_OneOutlier

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits

private theorem accept_all (q : Nat) : model.accept q = true := by rfl

private theorem step_0_false : model.step 0 false = some 0 := by simp [model]
private theorem step_0_true : model.step 0 true = none := by simp [model]
private theorem step_1_false : model.step 1 false = some 1 := by simp [model]
private theorem step_1_true : model.step 1 true = some 0 := by simp [model]

private theorem count_zero : ∀ n, count model n 0 = 1
  | 0 => by simp [count, accept_all]
  | n + 1 => by simp [count, step_0_false, step_0_true, count_zero n]

theorem sound_OneOutlier : AtMostHits.OneOutlier.Goal := by
  intro n
  induction n with
  | zero => simp [count, accept_all]
  | succ n ih => simp [count, step_1_false, step_1_true, ih, count_zero n]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_OneOutlier
