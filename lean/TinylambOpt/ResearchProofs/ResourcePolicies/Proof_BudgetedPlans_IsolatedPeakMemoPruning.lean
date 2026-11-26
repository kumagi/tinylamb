import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_IsolatedPeakMemoPruning

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Isolated peak and work are not enough to prune a build candidate.  In the
  counterexample both candidates have the same peak and work, but the first
  retains a one-unit output while the second retains none; with one unit of
  scratch the blocking join peak rises from one to two.  SQL execution must keep
  retained output in the dominance signature.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.IsolatedPeakMemoPruning.Goal := by
  intro h
  have bad := h ⟨0, 0, 1, 1⟩ ⟨0, 0, 1, 0⟩ ⟨0, 0, 0, 0⟩ 1
    (by decide) (by decide) (by decide) (by decide)
  simp [join] at bad
end TinylambOpt.ResearchProofs.ResourcePolicies.BudgetedPlans.Goal_IsolatedPeakMemoPruning
