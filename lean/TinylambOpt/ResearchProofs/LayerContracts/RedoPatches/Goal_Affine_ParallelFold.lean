import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Affine_ParallelFold

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Affine.ParallelFold.Goal := by
  exact Common.affine_parallelFold

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Affine_ParallelFold
