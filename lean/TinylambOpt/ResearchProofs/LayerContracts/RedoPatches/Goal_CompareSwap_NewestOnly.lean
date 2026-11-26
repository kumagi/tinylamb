import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_NewestOnly

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.CompareSwap.NewestOnly.Goal := by
  exact Common.not_compareSwap_newestOnly

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_NewestOnly
