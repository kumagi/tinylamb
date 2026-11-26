import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-
Refutes `SuccessfulNullLaneProgress`: NULL-producing rows are dropped by
`filterMap`, so the materialized output length is not the failing ordinal.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_SuccessfulNullLaneProgress

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

theorem refute : ¬ SuccessfulNullLaneProgress.Goal := by
  intro h
  let xs : List Cell := [.ok none, .error 3]
  have hf : first xs = some (1, 3) := by native_decide
  have hbad := h xs 1 3 hf
  have hp : (prefixOut xs).length = 0 := by native_decide
  rw [hp] at hbad
  omega

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_SuccessfulNullLaneProgress
