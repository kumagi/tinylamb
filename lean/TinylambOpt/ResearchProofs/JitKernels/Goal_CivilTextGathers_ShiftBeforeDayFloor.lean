import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-!
Subtracting a timezone displacement after truncating to the day is not the same
as truncating the shifted minute: a late local hour can push the minute sum over
the next 1440-minute boundary. The counterexample uses 23:59 with a positive
offset.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_ShiftBeforeDayFloor

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.ShiftBeforeDayFloor.Goal := by
  intro h
  have h' := h 0 23 59 100 (by omega) (by omega) (by omega) (by omega)
  have hne : ¬ ((utcMinute 0 23 59 100 / 1440 : Int) = ((0 * 1440 - 100) / 1440 : Int)) := by
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_ShiftBeforeDayFloor
