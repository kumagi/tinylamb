import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_ParallelFold

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Increment.ParallelFold.Goal := by
  exact Common.increment_parallelFold

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_ParallelFold
