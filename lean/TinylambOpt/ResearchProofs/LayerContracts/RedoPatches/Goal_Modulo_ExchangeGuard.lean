import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Modulo.ExchangeGuard` — When marked independent, patches may be scheduled in either
  order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_ExchangeGuard

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Modulo.ExchangeGuard.Goal := by
  exact Common.modulo_exchangeGuard

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_ExchangeGuard
