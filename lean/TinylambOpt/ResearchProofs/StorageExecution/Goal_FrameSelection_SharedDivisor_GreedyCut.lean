import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_GreedyCut

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SharedDivisor

/-
Counterexample: a = [2^30] compresses to 6 bytes via the raw fallback,
b = c = [1] compress to 5 bytes each.  Merging a with b destroys the common
divisor so ab falls back to raw (10 bytes), still below 6+5=11, hence the
greedy precondition holds.  But bc = [1,1] stays at 5 bytes while abc is a
3-element raw frame (14 bytes).  Thus min(abc, ab+c) = min(14,15) = 14 > 11.
-/
theorem disproof_GreedyCut :
    ¬ TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SharedDivisor.GreedyCut.Goal := by
  intro h
  have hres := h [2^30] [1] [1] (by decide) (by decide) (by decide) (by native_decide)
  exact (by native_decide :
    ¬ min (chosen write ([2^30] ++ [1] ++ [1]))
          (chosen write ([2^30] ++ [1]) + chosen write [1])
      ≤ chosen write [2^30] + chosen write ([1] ++ [1])) hres

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_GreedyCut
