import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_ScratchCompressionReserveSensitivity

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission


set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Lowering every job's scratch request by `cut` can reduce the required
reserve by at most `cut`. The bound is proved pointwise along the serial order:
truncated subtraction of holdings commutes with adding back the same cut. -/

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.ScratchCompressionReserveSensitivity.Goal := by
  intro jobs cut
  induction jobs with
  | nil =>
      simp [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom]
  | cons j js ih =>
      simp only [TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission.headroom,
        List.map_cons]
      rw [Nat.max_le]
      constructor
      · omega
      · omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.GrantAdmission.Goal_ScratchCompressionReserveSensitivity
