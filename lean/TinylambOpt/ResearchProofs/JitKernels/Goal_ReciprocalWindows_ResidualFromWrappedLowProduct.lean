import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

-- Refutation: a small wrapped low residual does not certify a rounded-up reciprocal guess.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_ResidualFromWrappedLowProduct

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows.ResidualFromWrappedLowProduct.Goal := by
  intro h
  have c := h 63 5 0 8 (by native_decide) (by native_decide) (by native_decide)
  have bad : ¬ (TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows.guess 63 5 0 = 63 / 5) := by
    native_decide
  exact bad (by simpa [TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows.exactAt] using c)

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_ResidualFromWrappedLowProduct
