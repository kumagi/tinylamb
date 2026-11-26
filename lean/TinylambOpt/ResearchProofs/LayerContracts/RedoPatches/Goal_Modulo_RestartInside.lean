import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_RestartInside

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Modulo.RestartInside.Goal := by
  exact Common.modulo_restartInside

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_RestartInside
