import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_LargestHolderAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation of largest-holder-first admission. A job with tiny holdings and
large scratch should run early; freeing the big holder first needlessly delays
it and raises the required reserve. -/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.LargestHolderAdmission.Goal := by
  intro h
  have hb := h [⟨0, 0⟩, ⟨1, 0⟩, ⟨2, 1⟩] [⟨0, 0⟩, ⟨1, 0⟩, ⟨2, 1⟩] (by decide)
  simp [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom, List.mergeSort] at hb
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_LargestHolderAdmission
