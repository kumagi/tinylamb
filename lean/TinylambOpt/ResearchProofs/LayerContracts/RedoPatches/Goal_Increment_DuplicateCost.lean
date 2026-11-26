import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_DuplicateCost

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Increment.DuplicateCost.Goal := by
  exact Common.increment_duplicateCost

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_DuplicateCost
