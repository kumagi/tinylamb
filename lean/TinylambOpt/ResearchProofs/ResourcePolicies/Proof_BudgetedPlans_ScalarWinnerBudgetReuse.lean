import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_ScalarWinnerBudgetReuse

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  A winner of a work-plus-price-times-peak score cannot be reused blindly at
  every cap that allows it.  The counterexample has a zero-work one-unit-peak
  plan and a one-work zero-peak plan: the latter wins the score at price one,
  both fit a cap of one, yet the former is cheaper at that cap.  SQL execution
  must compare feasible work, not only the scalar score.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.ScalarWinnerBudgetReuse.Goal := by
  intro h
  have bad := h [⟨0, 0, 1, 0⟩, ⟨1, 0, 0, 0⟩] ⟨1, 0, 0, 0⟩ 1 1
    (by simp)
    (by intro p hp; simp at hp; rcases hp with rfl | rfl <;> simp)
    (by decide)
  simp [cheapest] at bad
end TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_ScalarWinnerBudgetReuse
