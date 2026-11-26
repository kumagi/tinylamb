import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_RestartInside

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Increment.RestartInside.Goal := by
  exact Common.not_increment_restartInside

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_RestartInside
