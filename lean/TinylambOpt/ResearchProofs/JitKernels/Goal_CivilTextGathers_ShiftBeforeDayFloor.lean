import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! Truncating the local timestamp before subtracting the timezone displacement
is not the same as truncating afterwards: the local minute part can carry the
displacement across a UTC day boundary. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_ShiftBeforeDayFloor

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem refute : ¬ ShiftBeforeDayFloor.Goal := by
  intro h
  have bad := h 0 23 59 (-100) (by omega) (by omega) (by omega)
  have h1 : utcMinute 0 23 59 (-100) / 1440 = 1 := by native_decide
  have h2 : (0 * 1440 - (-100)) / 1440 = 0 := by native_decide
  rw [h1, h2] at bad
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_ShiftBeforeDayFloor
