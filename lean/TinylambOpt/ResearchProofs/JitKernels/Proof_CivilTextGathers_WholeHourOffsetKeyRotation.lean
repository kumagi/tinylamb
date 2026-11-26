import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! A whole-hour displacement is a constant rotation of the UTC hour key, so the rotation can be precomputed. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_CivilTextGathers_WholeHourOffsetKeyRotation

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers
open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers.WholeHourOffsetKeyRotation

theorem sound : Goal := by
  intro d h m z hm hm'
  unfold utcHour utcMinute
  omega

end Nov26.Proof_CivilTextGathers_WholeHourOffsetKeyRotation
