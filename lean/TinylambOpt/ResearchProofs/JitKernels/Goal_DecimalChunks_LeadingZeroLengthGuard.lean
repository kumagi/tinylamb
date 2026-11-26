import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

-- Refutation: leading zeros keep the magnitude small even when the digit count exceeds the width.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_LeadingZeroLengthGuard

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.LeadingZeroLengthGuard.Goal := by
  intro h
  have c := h 5 (List.replicate 4 48) (by native_decide) (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.parse 5 false (List.replicate 4 48) = none)) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_LeadingZeroLengthGuard
