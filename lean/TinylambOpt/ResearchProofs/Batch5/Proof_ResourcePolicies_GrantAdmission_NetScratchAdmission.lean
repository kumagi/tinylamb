import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_NetScratchAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation of net-scratch admission: aggregate scratch minus aggregate
holdings can look affordable while the first queued job alone stalls, because
holdings are returned only after their owner completes. -/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.NetScratchAdmission.Goal := by
  intro h
  have hb := h [⟨0, 20⟩, ⟨100, 0⟩] 10
  simp [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.stalled] at hb
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_NetScratchAdmission
