import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_ExactExchange

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.CompareSwap.ExactExchange.Goal := by
  exact Common.not_compareSwap_exactExchange

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_ExactExchange
