import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-! The initial and final counters can both be in range while an intermediate
lane update leaves the range. A checked scatter must inspect every partial sum,
so the endpoint-only certificate is unsound. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_CheckedScatterFromFinalExtrema

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem refute : ¬ CheckedScatterFromFinalExtrema.Goal := by
  intro h
  have bad := h [(0, 10), (0, -10)] 0 (-1) 1 0 (by native_decide) (by native_decide)
    (by native_decide) (by native_decide)
  have hc : check (-1) 1 0 [(0, 10), (0, -10)] 0 = false := by native_decide
  rw [hc] at bad
  cases bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_CheckedScatterFromFinalExtrema
