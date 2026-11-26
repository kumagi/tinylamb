import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Pruning a dominated build child inside a blocking join is safe when the live-output
(`held`) component is part of the memo signature: every `join` field is built from the
child via addition and `max`, both monotone, so dominance is preserved componentwise. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_RetainedOutputMemoDominance
open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem sound : RetainedOutputMemoDominance.Goal := by
  rintro a b probe scratch ⟨hw, _hs, hp, hh⟩
  simp only [join, dominates]
  refine ⟨Nat.add_le_add_right hw _, Nat.add_le_add_right hw _, ?_, Nat.add_le_add_right hh _⟩
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_BudgetedPlans_RetainedOutputMemoDominance
