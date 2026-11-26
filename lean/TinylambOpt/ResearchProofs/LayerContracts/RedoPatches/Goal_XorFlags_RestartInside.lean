import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_RestartInside

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.XorFlags.RestartInside.Goal := by
  exact Common.not_xorFlags_restartInside

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_RestartInside
