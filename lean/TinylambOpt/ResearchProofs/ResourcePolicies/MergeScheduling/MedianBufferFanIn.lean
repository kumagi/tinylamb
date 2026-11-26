import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.MedianBufferFanIn

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ MedianBufferFanIn.Goal := by
  intro h
  let bs : List Nat := [1, 1, 100]
  have hne : bs ≠ [] := by decide
  have hfd : bs.length + 1 ≤ 4 := by decide
  have hmem : bs.length * ((bs.mergeSort (· ≤ ·))[bs.length / 2]?.getD 0) + 0 ≤ 3 := by native_decide
  have hfan := h bs 3 4 0 hne hfd hmem
  have hsum : ¬ (bs.sum + 0 ≤ 3) := by decide
  exact hsum hfan.2

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.MedianBufferFanIn
