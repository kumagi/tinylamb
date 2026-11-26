import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.RowShrinkPredictsByteShrink

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ RowShrinkPredictsByteShrink.Goal := by
  intro h
  have hrows : 5 * ([100] : List Nat).length ≤ 4 * ([100] ++ [1,1,1,1,1]).length := by decide
  have := h [100] [1,1,1,1,1] hrows
  simp at this

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.RowShrinkPredictsByteShrink
