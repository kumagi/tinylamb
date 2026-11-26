import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_BalancedNesting_DepthCap

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.BalancedNesting

example : ∀ n, n < 6 → ∀ q, q < 6 → n < q → count model n q = 0 := by
  decide

theorem sound_DepthCap : DepthCap.Goal := by
  intro n
  induction n with
  | zero =>
      intro q hq
      simp [model, count]
      omega
  | succ n ih =>
      intro q hq
      have h1 : count model n (q+1) = 0 := ih (q+1) (by omega)
      have h2 : count model n (q-1) = 0 := ih (q-1) (by omega)
      rw [count]
      have hs_false : model.step q false = if q = 0 then none else some (q-1) := by
        simp [model]
      have hs_true : model.step q true = some (q+1) := by
        simp [model]
      rw [hs_false, hs_true]
      simp only [Option.map_some, Option.getD_some]
      by_cases hq0 : q = 0
      · subst q
        omega
      · simp [hq0, h1, h2]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_BalancedNesting_DepthCap
