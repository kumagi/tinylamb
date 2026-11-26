import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_StraddlingRecordFloor

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! A record straddling the recycle floor needs the pre-floor segment that
contains its prefix. The stated goal omits the required `floor ≤ r.lo` guard,
so a segment ending strictly inside the record is discarded. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

theorem refute :
    ¬ TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.StraddlingRecordFloor.Goal := by
  intro h
  have hce := h [⟨0, 1⟩] 1 ⟨0, 2⟩ (by decide) (by decide)
  simp [retained, intersects] at hce
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_StraddlingRecordFloor
