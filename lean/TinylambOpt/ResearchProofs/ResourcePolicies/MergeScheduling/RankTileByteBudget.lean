import TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.RankTileByteBudget

open TinylambOpt.ResearchGoals.ResourcePolicies.MergeScheduling

theorem refute : ¬ RankTileByteBudget.Goal := by
  intro h
  have hsum : ([10, 0, 0] : List Nat).sum ≤ [10, 0, 0].length * 4 := by decide
  have := h [10, 0, 0] 1 4 hsum 0
  simp at this

end TinylambOpt.ResearchProofs.ResourcePolicies.MergeScheduling.RankTileByteBudget
