import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_MedianBufferFanIn

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Refutation of `MedianBufferFanIn`: the upper median does not bound the
    average byte size, so a median-based buffer estimate can undercount memory. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.MedianBufferFanIn.Goal := by
  intro h
  -- bs = [0,0,1]: upper median is 0, so the byte bound is 0 but the buffers need 1 byte.
  have H := h [0, 0, 1] 0 4 0 (by native_decide) (by native_decide) (by native_decide)
  have hnot : ¬ fanIn 0 4 0 [0, 0, 1] := by
    unfold fanIn
    native_decide
  exact hnot H
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.MergeScheduling.Goal_MedianBufferFanIn
