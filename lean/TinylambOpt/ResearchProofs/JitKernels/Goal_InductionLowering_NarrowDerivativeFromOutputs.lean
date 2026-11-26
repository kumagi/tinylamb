import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-!
A signed difference of two in-range values need not be in range: the extreme
outputs `-8` and `7` fit in 3 bits but their difference `15` does not. The
derivative register width therefore needs its own range analysis.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InductionLowering.Goal_NarrowDerivativeFromOutputs

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ NarrowDerivativeFromOutputs.Goal := by
  intro h
  have hv : signedFits 3 (-8) := by simp [signedFits]
  have hn : signedFits 3 7 := by simp [signedFits]
  have t := h 3 (-8) 7 hv hn
  simp [signedFits] at t

end TinylambOpt.ResearchProofs.JitKernels.InductionLowering.Goal_NarrowDerivativeFromOutputs
