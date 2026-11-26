import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
A downward-rounded reciprocal plus an unconditional increment of the dividend
is not a substitute for exact division: it loses a quotient boundary when the
dividend itself reaches a multiple of the divisor.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_RoundedDownAddOne

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows.RoundedDownAddOne.Goal := by
  intro h
  have got := h 1 2 1 (by decide) (by decide)
  simp at got

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_RoundedDownAddOne
