import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_PackedLowNibblesValidateDigits

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! Low nibble zero is not sufficient: byte 0 is not an ASCII digit. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.PackedLowNibblesValidateDigits.Goal := by
  rw [PackedLowNibblesValidateDigits.Goal]
  intro h
  have hp := h [0] (by simp) (by decide)
  simp [digits] at hp

end TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_PackedLowNibblesValidateDigits
