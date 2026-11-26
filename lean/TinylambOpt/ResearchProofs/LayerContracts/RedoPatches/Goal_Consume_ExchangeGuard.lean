import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Consume.ExchangeGuard` — When marked independent, patches may be scheduled in either
  order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Consume_ExchangeGuard

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Consume.ExchangeGuard.Goal := by
  exact Common.consume_exchangeGuard

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Consume_ExchangeGuard
