import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! Low-nibble range and a 7-bit bound do not force an ASCII decimal digit:
byte 16 has low nibble zero but is not in the `'0'..'9'` range. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_PackedLowNibblesValidateDigits

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ PackedLowNibblesValidateDigits.Goal := by
  intro h
  have hw := h [16] (by simp) (by simp)
  simp [digits] at hw

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_PackedLowNibblesValidateDigits
