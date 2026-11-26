import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Ceiling_NoStamp

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Ceiling.NoStamp.Goal := by
  exact Common.ceiling_noStamp

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Ceiling_NoStamp
