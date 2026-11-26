import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! A whole-hour timezone displacement rotates the hour group id: the residual
minute part is strictly less than one hour and cannot change the truncated
hour. The proof preserves the modeled UTC hour key. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_WholeHourOffsetKeyRotation

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem sound : TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.WholeHourOffsetKeyRotation.Goal := by
  intro d h m z hm hm'
  unfold utcHour utcMinute
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_WholeHourOffsetKeyRotation