import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Increment.ExactExchange` — Is the proposed conflict test complete as well as sound?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_ExactExchange

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Increment.ExactExchange.Goal := by
  exact Common.increment_exactExchange

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_ExactExchange
