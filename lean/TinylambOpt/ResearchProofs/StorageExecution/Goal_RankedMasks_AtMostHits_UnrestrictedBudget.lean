import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_UnrestrictedBudget

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits

private theorem step_false (q : Nat) : model.step q false = some q := by
  simp [model]

private theorem step_true_of_pos (q : Nat) (hq : 0 < q) : model.step q true = some (q - 1) := by
  simp [model, Nat.ne_of_gt hq]

theorem sound_UnrestrictedBudget : AtMostHits.UnrestrictedBudget.Goal := by
  intro n
  induction n with
  | zero =>
      intro q h
      simp [count, model]
  | succ n ih =>
      intro q h
      have hq : 0 < q := by omega
      have h1 : count model n q = 2 ^ n := ih q (by omega)
      have h2 : count model n (q - 1) = 2 ^ n := ih (q - 1) (by omega)
      simp [count, step_false, step_true_of_pos q hq, h1, h2, Nat.pow_succ, Nat.mul_two]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_UnrestrictedBudget
