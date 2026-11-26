import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-!
Two chunks can both miss a needle while their concatenation matches across the
boundary. Needle `[0,1]` is absent from `[0]` and from `[1]` but present at
offset zero of `[0] ++ [1]`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_CombinedChunkFirstMiss

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ CombinedChunkFirstMiss.Goal := by
  intro h
  have t := h ([0, 1] : B) ([0] : B) ([1] : B) (by decide) (by decide)
  have hbad : ¬ (starts ([0, 1] : B) (([0] : B) ++ ([1] : B)) = []) := by
    decide
  exact hbad t

end TinylambOpt.ResearchProofs.JitKernels.LiteralSkip.Goal_CombinedChunkFirstMiss
