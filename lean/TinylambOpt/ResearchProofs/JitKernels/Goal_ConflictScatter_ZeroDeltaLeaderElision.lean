import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

/-! Dropping reduced groups whose delta is zero leaves every per-slot sum
unchanged, so the arithmetic store can be skipped for those leaders. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_ZeroDeltaLeaderElision

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

theorem sound : ZeroDeltaLeaderElision.Goal := by
  intro us k
  rw [delta_filter_zero, delta_combine]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_ZeroDeltaLeaderElision
