import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

/-! Reducing all lanes targeting one slot before the store preserves the
accumulator value: a single reduced delta equals the sum of the original
deltas for that slot. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LeaderReducedScatter

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

theorem sound : LeaderReducedScatter.Goal := by
  intro us init k
  simp [sequential, delta_combine]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LeaderReducedScatter
