import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Consume_Residual

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Consume.Residual.Goal := by
  exact Common.consume_residual

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Consume_Residual
