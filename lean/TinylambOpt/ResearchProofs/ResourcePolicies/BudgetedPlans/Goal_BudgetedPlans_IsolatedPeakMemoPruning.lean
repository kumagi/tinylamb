import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutes `IsolatedPeakMemoPruning`: comparing only `peak` (and `work`) ignores how much
memory a candidate *retains* after returning. A dominated-peak candidate that retains
less output can yield a strictly smaller join peak. Witness: a = ⟨0,0,100,100⟩ keeps 100
of its 100 peak live, b = ⟨0,0,100,0⟩ keeps nothing; with a probe of peak 5 the join over
a peaks at 105 while the join over b peaks at 100. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_IsolatedPeakMemoPruning
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem refute : ¬ IsolatedPeakMemoPruning.Goal := by
  intro h
  exact absurd (h ⟨0, 0, 100, 100⟩ ⟨0, 0, 100, 0⟩ ⟨0, 0, 5, 0⟩ 0) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_IsolatedPeakMemoPruning
