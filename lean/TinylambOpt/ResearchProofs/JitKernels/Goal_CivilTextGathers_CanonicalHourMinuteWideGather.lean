import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_CanonicalHourMinuteWideGather

theorem sound : TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.CanonicalHourMinuteWideGather.Goal := by
  intro start limit off hrow hoff
  simp at hoff
  rcases hoff with rfl | rfl <;> omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_CanonicalHourMinuteWideGather
