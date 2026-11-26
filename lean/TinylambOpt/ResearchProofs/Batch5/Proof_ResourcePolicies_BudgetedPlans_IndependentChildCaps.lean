import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_IndependentChildCaps

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Independent child caps are not a shared grant: two children each peaking at
  the cap can still overlap because the blocking join retains the left output
  while building the right side.  The counterexample uses two one-unit peaks,
  one of which retains its whole output; the joined peak is two.  SQL execution
  therefore needs an explicit shared-budget check.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.IndependentChildCaps.Goal := by
  intro h
  have bad := h ⟨0, 0, 1, 1⟩ ⟨0, 0, 1, 0⟩ 1
    (by decide) (by decide) (by decide) (by decide)
  simp [join] at bad
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_IndependentChildCaps
