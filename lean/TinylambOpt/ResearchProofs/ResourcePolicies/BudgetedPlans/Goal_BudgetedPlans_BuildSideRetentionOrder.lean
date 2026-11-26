import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
The build order that retains more of its peak (smaller `peak - held` excess) is never
worse: `join a b s` peaks at `max a.peak (a.held + b.peak) (a.held + b.held + s)` and each
branch is bounded by the corresponding branch of `join b a s` using `a.held ≤ a.peak`,
`b.held ≤ b.peak` and `b.peak - b.held ≤ a.peak - a.held` (which gives
`a.held + b.peak ≤ b.held + a.peak`). -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_BuildSideRetentionOrder
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem sound : BuildSideRetentionOrder.Goal := by
  intro a b s h1 h2 h3
  simp only [join]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_BuildSideRetentionOrder
