import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutes `ScalarWinnerBudgetReuse`: minimizing the single weighted score
`work + price * peak` does not minimize `work` among plans that fit a generous budget.
Witness: winner = ⟨5,0,1,0⟩ and rival = ⟨4,0,10,0⟩ with price 1: the winner scores 6
versus 14, yet at cap 10 both are feasible and `cheapest` reports work 4, not 5. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_ScalarWinnerBudgetReuse
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem refute : ¬ ScalarWinnerBudgetReuse.Goal := by
  intro h
  exact absurd (h [⟨5, 0, 1, 0⟩, ⟨4, 0, 10, 0⟩] ⟨5, 0, 1, 0⟩ 1 10) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_ScalarWinnerBudgetReuse
