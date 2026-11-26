import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option linter.unnecessarySimpa false
set_option linter.deprecated false
set_option linter.unusedVariables false
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.CompletionOrderCertificate.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_CompletionOrderCertificate
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission CompletionOrderCertificate

private theorem headroom_le_iff (jobs : List Job) (free : Nat) :
    headroom jobs ≤ free ↔ stalled free jobs = none := by
  induction jobs generalizing free with
  | nil => simp [headroom, stalled]
  | cons j js ih =>
    simp only [headroom, stalled]
    constructor
    · intro h
      have h1 : j.scratch ≤ free := by omega
      have h2 : headroom js ≤ free + j.held := by omega
      rw [if_pos h1]
      exact (ih (free + j.held)).mp h2
    · intro h
      by_cases hj : j.scratch ≤ free
      · rw [if_pos hj] at h
        have h2 := (ih (free + j.held)).mpr h
        omega
      · rw [if_neg hj] at h
        simp at h

theorem sound : Goal := by
  intro jobs free
  exact (headroom_le_iff jobs free).symm
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_GrantAdmission_CompletionOrderCertificate
