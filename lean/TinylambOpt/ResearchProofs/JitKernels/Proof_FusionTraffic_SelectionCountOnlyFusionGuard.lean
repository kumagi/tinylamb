import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-! FALSE: Population count alone does not determine the cache-line footprint of a
sorted selection. Counterexample: stride 2, line width 4, `a = [0, 10]` and `b = [0, 1]`
have equal length, but `a`'s rows land on lines `0` and `5` (two lines) while `b`'s rows
both pack into line `0` (one line). -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_SelectionCountOnlyFusionGuard
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-- Refutation of counting-only eager/sparse selection. -/
theorem refute : ¬ SelectionCountOnlyFusionGuard.Goal := by
  intro h
  have key := h [0, 10] [0, 1] 0 2 4 (by decide)
    (by decide) (by decide) (by decide)
  exact absurd key (by decide)

end TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_SelectionCountOnlyFusionGuard
