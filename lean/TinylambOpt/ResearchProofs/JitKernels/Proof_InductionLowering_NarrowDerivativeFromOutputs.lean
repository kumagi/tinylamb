import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-! Independent proof record for the InductionLowering.NarrowDerivativeFromOutputs research goal: the proposed rewrite is unsound and this file exhibits a counterexample. -/

/-!
A signed difference of two in-range values need not be in range: the extreme
outputs `-8` and `7` fit in 3 bits but their difference `15` does not. The
derivative register width therefore needs its own range analysis.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_InductionLowering_NarrowDerivativeFromOutputs
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering.NarrowDerivativeFromOutputs
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem refute : ¬ Goal := by
  intro h
  have hv : signedFits 3 (-8) := by simp [signedFits]
  have hn : signedFits 3 7 := by simp [signedFits]
  have t := h 3 (-8) 7 hv hn
  simp [signedFits] at t

end Nov26.Proof_InductionLowering_NarrowDerivativeFromOutputs
