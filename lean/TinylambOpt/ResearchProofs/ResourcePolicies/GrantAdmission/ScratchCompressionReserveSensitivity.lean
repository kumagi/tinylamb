import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_GrantAdmission_ScratchCompressionReserveSensitivity
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission ScratchCompressionReserveSensitivity
theorem sound : Goal := by
  intro jobs cut
  induction jobs with
  | nil => simp [headroom]
  | cons j js ih =>
    simp only [headroom, List.map_cons]
    have := ih
    omega
end Nov26.Proof_GrantAdmission_ScratchCompressionReserveSensitivity
