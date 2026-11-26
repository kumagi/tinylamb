import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Refuted by counterexample

- `PatchedOctet.GreedyCut` — Aggressive hypothesis: the cheapest two adjacent groups
  never need reconsideration when a third arrives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.PatchedOctet

theorem disproof_GreedyCut : ¬ GreedyCut.Goal := by
  intro h
  have hprem : chosen write ([0] ++ [2097407]) ≤ chosen write [0] + chosen write [2097407] := by
    native_decide
  have hcon := h [0] [2097407] [268435711] (by decide) (by decide) (by decide) hprem
  have hbad : ¬ (min (chosen write ([0] ++ [2097407] ++ [268435711]))
        (chosen write ([0] ++ [2097407]) + chosen write [268435711]) ≤
      chosen write [0] + chosen write ([2097407] ++ [268435711])) := by
    native_decide
  exact hbad hcon

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_GreedyCut
