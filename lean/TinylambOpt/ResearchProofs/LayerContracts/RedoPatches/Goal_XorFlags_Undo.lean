import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_Undo

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.XorFlags.Undo.Goal := by
  exact Common.xorFlags_undo

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_Undo
