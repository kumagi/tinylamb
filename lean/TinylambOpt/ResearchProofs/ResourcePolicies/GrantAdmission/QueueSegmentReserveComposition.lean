import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_GrantAdmission_QueueSegmentReserveComposition
open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission QueueSegmentReserveComposition
theorem sound : Goal := by
  intro front back
  induction front with
  | nil => simp [headroom]
  | cons j js ih =>
    simp only [headroom, List.cons_append, List.map_cons, List.sum_cons]
    rw [ih]
    omega
end Nov26.Proof_GrantAdmission_QueueSegmentReserveComposition
