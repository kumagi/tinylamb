import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! A leading zero makes the digit-count lower bound useless: `"01"` has two
digits but magnitude one. It passes the length guard for width three while
still fitting the signed range, so rejection by length alone is unsound. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_LeadingZeroLengthGuard

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ LeadingZeroLengthGuard.Goal := by
  intro h
  have hw := h 3 [48, 49] (by simp [digits]) (by native_decide)
  simp [parse, digits, magnitude] at hw

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_LeadingZeroLengthGuard
