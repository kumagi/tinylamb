import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_Source

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.CompareSwap.Source.Goal := by
  exact Common.compareSwap_source

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_Source
