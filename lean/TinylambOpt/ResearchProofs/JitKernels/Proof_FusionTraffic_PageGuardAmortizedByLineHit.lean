import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic
/-! Certificate for `TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.PageGuardAmortizedByLineHit.Goal`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_FusionTraffic_PageGuardAmortizedByLineHit

open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic.PageGuardAmortizedByLineHit

theorem refute : ¬ Goal := by
  intro h
  have h' := h 0 1 8 0 7 1 (by decide) (by decide) (by decide)
  have hbad : ¬ (0 + 1 * 7 < 1) := by decide
  exact hbad h'

end Nov26.Proof_FusionTraffic_PageGuardAmortizedByLineHit
