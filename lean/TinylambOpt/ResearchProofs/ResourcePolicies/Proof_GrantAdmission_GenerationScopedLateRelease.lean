import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_GenerationScopedLateRelease
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

/-- A late release aimed at generation `old` never touches generation `fresh`:
revoking filters out exactly the tickets whose generation equals `old`, so a
ticket of the replacement generation `fresh ≠ old` survives the revoke and the
subsequent selection keeps every byte of the fresh grant set. -/
theorem sound : GenerationScopedLateRelease.Goal := by
  intro ts owner old fresh hne
  simp only [revoke, List.filter_filter]
  refine congrArg (fun p : Ticket → Bool => ts.filter p) ?_
  funext t
  by_cases h1 : t.owner == owner <;> by_cases h2 : t.generation == old <;>
    by_cases h3 : t.generation == fresh <;> simp_all [beq_iff_eq, Ne]

end TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission_GenerationScopedLateRelease
