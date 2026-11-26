import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_StraddlingRecordFloor
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
theorem refute : ¬ StraddlingRecordFloor.Goal := by
  intro h
  let segs : List Span := [⟨0, 5⟩]
  let floor := 6
  let r : Span := ⟨3, 10⟩
  have h' := h segs floor r (by decide) (by decide)
  simp [retained, intersects, segs, floor, r] at h'
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_StraddlingRecordFloor
