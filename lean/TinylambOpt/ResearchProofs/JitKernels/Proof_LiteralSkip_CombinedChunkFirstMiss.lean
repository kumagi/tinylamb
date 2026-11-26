import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-! Independent proof record for the LiteralSkip.CombinedChunkFirstMiss research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

/-!
Two chunks can both miss a needle while their concatenation matches across the
boundary. Needle `[0,1]` is absent from `[0]` and from `[1]` but present at
offset zero of `[0] ++ [1]`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_LiteralSkip_CombinedChunkFirstMiss
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.CombinedChunkFirstMiss
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ Goal := by
  intro h
  have t := h ([0, 1] : B) ([0] : B) ([1] : B) (by decide) (by decide)
  have hbad : ¬ (starts ([0, 1] : B) (([0] : B) ++ ([1] : B)) = []) := by
    decide
  exact hbad t

end Nov26.Proof_LiteralSkip_CombinedChunkFirstMiss
