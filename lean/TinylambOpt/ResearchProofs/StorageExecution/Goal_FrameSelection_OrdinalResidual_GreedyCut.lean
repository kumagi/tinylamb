import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Refuted by counterexample

- `OrdinalResidual.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.OrdinalResidual

/-- The greedy hypothesis fails for ordinal residuals.  Take
    a = [2048], b = [0], c = [0,0].  The prefix merge is locally cheap:
      chosen [2048,0] = 7 <= chosen [2048] + chosen [0] = 5 + 3 = 8.
    Once c arrives, the widths of the trailing zeros exploit the restarted
    ordinals, so the true optimum is the split a | (b++c):
      min (chosen [2048,0,0,0]) (chosen [2048,0] + chosen [0,0])
        = min 10 (7 + 4) = 10,
      chosen [2048] + chosen [0,0,0] = 5 + 4 = 9.
    Prepending the outlier 2048 forces the merged frame into a 13-bit residual
    for every subsequent cell, hiding the cheap frame of the three zeros. -/
theorem disproof_GreedyCut : ¬ OrdinalResidual.GreedyCut.Goal := by
  intro h
  have h' := h [2048] [0] [0, 0] (by decide) (by decide) (by decide) (by native_decide)
  have hc : ¬ (min (chosen write ([2048] ++ [0] ++ [0, 0]))
      (chosen write ([2048] ++ [0]) + chosen write [0, 0]) ≤
      chosen write [2048] + chosen write ([0] ++ [0, 0])) := by
    native_decide
  exact hc h'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_GreedyCut
