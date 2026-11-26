import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_Period

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Modulo.Period.Goal := by
  exact Common.modulo_period

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_Period
