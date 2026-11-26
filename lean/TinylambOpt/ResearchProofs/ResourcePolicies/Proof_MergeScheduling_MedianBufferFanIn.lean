import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.Goal_MedianBufferFanIn

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling


/-! Refutation of `MedianBufferFanIn`: the upper median does not bound the
    average byte size, so a median-based buffer estimate can undercount memory. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling.MedianBufferFanIn.Goal := by
  intro h
  -- bs = [0,0,1]: upper median is 0, so the byte bound is 0 but the buffers need 1 byte.
  have hs : [0, 0, 1].mergeSort (fun a b => decide (a ≤ b)) = [0, 0, 1] := by
    refine List.mergeSort_of_pairwise ?_
    simp
  have hpre : [0, 0, 1].length
      * ([0, 0, 1].mergeSort (fun a b => decide (a ≤ b)))[[0, 0, 1].length / 2]?.getD 0
      + 0 ≤ 0 := by
    rw [hs]
    decide
  have H := h [0, 0, 1] 0 4 0 (by decide) (by decide) hpre
  have hnot : ¬ fanIn 0 4 0 [0, 0, 1] := by
    unfold fanIn
    decide
  exact hnot H
end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.Goal_MedianBufferFanIn
