import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_ReplanRemainingWorkCut

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Replan remaining-work cut: after setup has been sunk, paying a conversion
  cost is worthwhile exactly when it is covered by the per-row saving on the
  remaining rows.  The proof rewrites the old rate as the new rate plus its
  surplus and cancels the common terms.  SQL execution may switch kernels for
  the remaining rows under this exact budget condition.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.ReplanRemainingWorkCut.Goal := by
  intro sunk conversion remaining oldRate newRate hnew
  let d := oldRate - newRate
  have hold : oldRate = newRate + d := by dsimp [d]; omega
  have hd : newRate + d - newRate = d := by omega
  rw [hold, hd]
  simp only [Nat.left_distrib, Nat.add_assoc]
  constructor <;> intro h <;> omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_ReplanRemainingWorkCut
