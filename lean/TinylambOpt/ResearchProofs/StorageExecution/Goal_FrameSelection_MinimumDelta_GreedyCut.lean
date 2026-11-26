import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.MinimumDelta

/-- a = [512], b = [0], c = [0,0] refutes the greedy hypothesis:
    chosen a = 5, chosen b = 4, chosen c = 4 and chosen (a++b) = 6 <= 5+4,
    but the best global option costs min(10, 6+4) = 10 while the split
    a | (b++c) costs 5 + 4 = 9. The locally cheap merge [512,0] hides the
    large residual width needed once the trailing zeros join it. -/
theorem disproof_GreedyCut : ¬ MinimumDelta.GreedyCut.Goal := by
  intro h
  have h' := h [512] [0] [0,0] (by decide) (by decide) (by decide) (by native_decide)
  have hc : ¬ (min (chosen write ([512] ++ [0] ++ [0,0])) (chosen write ([512] ++ [0]) + chosen write [0,0]) ≤
      chosen write [512] + chosen write ([0] ++ [0,0])) := by native_decide
  exact hc h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_MinimumDelta_GreedyCut
