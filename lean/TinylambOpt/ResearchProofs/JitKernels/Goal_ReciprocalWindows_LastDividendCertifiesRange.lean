import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
An exact quotient for the largest dividend does not certify the whole range:
a rounded-up reciprocal can be exact at a multiple of the divisor and
overestimate a smaller dividend in the same window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_LastDividendCertifiesRange

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ LastDividendCertifiesRange.Goal := by
  intro h
  have got := h 3 3 1 (by decide) (by simp [exactAt, guess, magic])
  have bad := got 2 (by decide)
  simp [exactAt, guess, magic] at bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_LastDividendCertifiesRange
