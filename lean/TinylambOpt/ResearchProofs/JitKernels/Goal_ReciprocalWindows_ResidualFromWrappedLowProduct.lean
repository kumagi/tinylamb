import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
A narrow wrapped remainder can look plausible even when the reciprocal guess
overshoots the quotient. The SQL kernel still needs the explicit overestimate
comparison before accepting its candidate.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_ResidualFromWrappedLowProduct

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ ResidualFromWrappedLowProduct.Goal := by
  intro h
  have got := h 1 2 0 1 (by decide)
  have cond : (1 + 2 ^ 1 - (guess 1 2 0 * 2) % 2 ^ 1) % 2 ^ 1 < 2 := by
    native_decide
  have bad := got cond
  simp [exactAt, guess, magic] at bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_ResidualFromWrappedLowProduct
