import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_SmallerStridePointDominance

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
Arbitrary smaller strides do not dominate larger ones for point lookups: a stride
that does not divide the larger stride can lose alignment at particular rows.

Refuted by the concrete counterexample `a = 2`, `b = 3`, `i = 3`, where the
distance from the row to the restart boundary grows from 0 to 1.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_SmallerStridePointDominance

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

theorem refute : ¬ SmallerStridePointDominance.Goal := by
  intro h
  have hbad := h 2 3 3 (by decide) (by decide)
  have hcontra : ¬ (3 - anchor 2 3 ≤ 3 - anchor 3 3) := by native_decide
  exact hcontra hbad

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_SmallerStridePointDominance
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_SmallerStridePointDominance
