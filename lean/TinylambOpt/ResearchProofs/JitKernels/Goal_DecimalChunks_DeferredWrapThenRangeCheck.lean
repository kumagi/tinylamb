import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! A wrapped accumulator can have a small residue while its true magnitude is
out of range. Decimal `"8"` at width two has residue 8 modulo 8, yet 8 does not
fit the signed width, so the final range check cannot be deferred this way. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_DeferredWrapThenRangeCheck

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ DeferredWrapThenRangeCheck.Goal := by
  intro h
  have hw := h [56] 2 (by simp [digits]) (by native_decide) (by simp)
  simp [parse, digits, magnitude] at hw

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_DeferredWrapThenRangeCheck
