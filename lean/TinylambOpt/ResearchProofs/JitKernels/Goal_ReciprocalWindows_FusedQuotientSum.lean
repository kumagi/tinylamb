import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_FusedQuotientSum

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-! Shared remainders accumulate: two quotients of 1/2 are zero while the
quotient of their sum is one. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows.FusedQuotientSum.Goal := by
  rw [FusedQuotientSum.Goal]
  intro h
  have hh := h [1,1] 2 1 (by decide) (by decide) (by simp)
  have hbad : 0 ≠ 1 := by decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_FusedQuotientSum
