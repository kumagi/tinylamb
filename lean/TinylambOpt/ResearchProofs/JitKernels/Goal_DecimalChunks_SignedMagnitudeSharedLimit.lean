import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! The signed range is asymmetric: the negative side admits one extra value.
Parsing `"1"` at width zero fails as positive but succeeds as negative, so a
single unsigned magnitude cutoff is not shared by both signs. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_SignedMagnitudeSharedLimit

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.SignedMagnitudeSharedLimit.Goal := by
  intro h
  have hw := h 0 [49]
  simp [parse, digits, magnitude] at hw

end TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_SignedMagnitudeSharedLimit
