import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.SelectionCountOnlyFusionGuard.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_FusionTraffic_SelectionCountOnlyFusionGuard

open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.SelectionCountOnlyFusionGuard

theorem refute : ¬ Goal := by
  intro h
  have h' := h [0, 1] [0, 8] 0 1 8 (by decide) (by decide) (by decide) (by decide)
  have hbad : (footprint 0 1 8 [0, 1]).length ≠ (footprint 0 1 8 [0, 8]).length := by decide
  exact hbad h'

end Nov26.Proof_FusionTraffic_SelectionCountOnlyFusionGuard
