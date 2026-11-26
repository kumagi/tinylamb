import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_PageGuardAmortizedByLineHit

open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

theorem refute : ¬ PageGuardAmortizedByLineHit.Goal := by
  intro h
  have h' := h 0 1 8 0 7 1 (by decide) (by decide) (by decide)
  have hbad : ¬ (0 + 1 * 7 < 1) := by decide
  exact hbad h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_FusionTraffic_PageGuardAmortizedByLineHit
