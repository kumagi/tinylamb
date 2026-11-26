import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_HighWater_Fusion

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.HighWater.Fusion.Goal := by
  exact Common.highWater_fusion

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_HighWater_Fusion
