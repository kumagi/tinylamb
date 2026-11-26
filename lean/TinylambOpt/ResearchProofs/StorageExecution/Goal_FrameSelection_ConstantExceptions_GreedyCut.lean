import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions

/-- The greedy hypothesis holds for a = [0], b = [1]:
    chosen [0,1] = 10 <= chosen [0] + chosen [1] = 6 + 6 = 12.
    But with c = [1] the greedy merge loses to the split [0] | [1,1]:
      min (chosen [0,1,1]) (chosen [0,1] + chosen [1]) = min 14 16 = 14,
      chosen [0] + chosen [1,1] = 6 + 7 = 13.
    The locally cheaper prefix merge [0,1] hides the modal constant of [1,1]. -/
theorem disproof_GreedyCut : ¬ ConstantExceptions.GreedyCut.Goal := by
  intro h
  have h' := h [0] [1] [1] (by decide) (by decide) (by decide) (by native_decide)
  have hc : ¬ (min (chosen write ([0] ++ [1] ++ [1])) (chosen write ([0] ++ [1]) + chosen write [1]) ≤
      chosen write [0] + chosen write ([1] ++ [1])) := by native_decide
  exact hc h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_GreedyCut
