import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_GrantAdmission_NetScratchAdmission
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission NetScratchAdmission
theorem refute : ¬ Goal := by
  intro h
  have := h [⟨0, 2⟩, ⟨10, 0⟩] 0 (by decide)
  simp [stalled] at this
end Nov26.Proof_GrantAdmission_NetScratchAdmission
