import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.GenerationScopedLateRelease.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_GenerationScopedLateRelease

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

theorem generationScopedLateRelease : GenerationScopedLateRelease.Goal := by
  intro ts owner old fresh hne
  unfold revoke
  rw [List.filter_filter]
  congr 1
  funext t
  cases ho : (t.owner == owner) <;> cases hg : (t.generation == old) <;> cases hf : (t.generation == fresh) <;> simp_all
