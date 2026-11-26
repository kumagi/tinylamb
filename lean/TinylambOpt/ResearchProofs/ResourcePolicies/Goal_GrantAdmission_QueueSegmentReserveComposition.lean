import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.QueueSegmentReserveComposition.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_QueueSegmentReserveComposition
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission QueueSegmentReserveComposition
theorem sound : Goal := by
  intro front back
  induction front with
  | nil => simp [headroom]
  | cons j js ih =>
    simp only [headroom, List.cons_append, List.map_cons, List.sum_cons]
    rw [ih]
    omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_QueueSegmentReserveComposition
