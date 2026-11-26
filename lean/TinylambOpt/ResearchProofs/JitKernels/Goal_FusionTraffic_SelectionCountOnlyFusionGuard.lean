import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_SelectionCountOnlyFusionGuard

open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

theorem refute : ¬ SelectionCountOnlyFusionGuard.Goal := by
  intro h
  have h' := h [0, 1] [0, 8] 0 1 8 (by decide) (by decide) (by decide) (by decide)
  have hbad : (footprint 0 1 8 [0, 1]).length ≠ (footprint 0 1 8 [0, 8]).length := by decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_SelectionCountOnlyFusionGuard
