import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-!
Combining dividends before division is unsound when the common remainder
accumulates to a whole divisor. Two rows with remainder one modulo two each
have quotient zero, but their combined dividend has quotient one.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_FusedQuotientSum

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ FusedQuotientSum.Goal := by
  intro h
  have got := h [1, 1] 2 1 (by decide) (by decide) (by decide)
  simp at got

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_FusedQuotientSum
