import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

/-! Splitting lanes by ordinal parity partitions all updates, so reconciling
the two private counters once at the end yields the original per-slot sum. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_PrivatizedStripeMerge

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.PrivatizedStripeMerge.Goal := by
  intro us k
  exact zipIdx_delta_partition us 0 k

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_PrivatizedStripeMerge
