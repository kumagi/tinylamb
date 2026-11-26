import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! Rows with different civil hours can share a UTC hour group when the minute
offset crosses a hour boundary. Grouping by the original hour is therefore not
interchangeable with grouping by the UTC hour in SQL GROUP BY. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_OffsetHashGrouping

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem refute : ¬ OffsetHashGrouping.Goal := by
  intro h
  have bad := h 0 0 59 0 1 0 (-1) (by omega) (by omega) (by omega) (by omega) (by omega) (by omega)
  have h1 : utcHour 0 0 59 (-1) = 1 := by native_decide
  have h2 : utcHour 0 1 0 (-1) = 1 := by native_decide
  rw [h1, h2] at bad
  simp at bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_OffsetHashGrouping
