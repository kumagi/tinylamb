import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Ceiling_ExactExchange

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Ceiling.ExactExchange.Goal := by
  exact Common.ceiling_exactExchange

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Ceiling_ExactExchange
