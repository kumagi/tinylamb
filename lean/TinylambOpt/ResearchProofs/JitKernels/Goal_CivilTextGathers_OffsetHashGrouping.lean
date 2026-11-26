import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! Equal local hours are not necessary for equal UTC-hour groups: a minute
    offset crossing a minute boundary shifts adjacent hours into one group. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_OffsetHashGrouping

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.OffsetHashGrouping.Goal := by
  intro h
  have hh := h 0 1 0 0 0 1 1 (by omega) (by omega) (by omega) (by omega)
    (by omega) (by omega) (by omega) (by omega)
  have heq : utcHour 0 1 0 1 = utcHour 0 0 1 1 := by native_decide
  have hhours : (1 : Int) = 0 := hh.mpr heq
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_OffsetHashGrouping
