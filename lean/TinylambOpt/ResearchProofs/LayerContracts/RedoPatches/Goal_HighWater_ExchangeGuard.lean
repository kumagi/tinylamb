import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_HighWater_ExchangeGuard

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.HighWater.ExchangeGuard.Goal := by
  exact Common.highWater_exchangeGuard

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_HighWater_ExchangeGuard
