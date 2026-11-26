import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutes `IndependentChildCaps`: each child respecting the shared cap in isolation does
not bound the blocking join's peak, which adds the left child's retained output to the
right child's peak. Witness: a = ⟨0,0,8,8⟩ and b = ⟨0,0,8,0⟩ both fit cap 8, but
(join a b 0).peak = 16 > 8. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_IndependentChildCaps
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem refute : ¬ IndependentChildCaps.Goal := by
  intro h
  exact absurd (h ⟨0, 0, 8, 8⟩ ⟨0, 0, 8, 0⟩ 8) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_IndependentChildCaps
