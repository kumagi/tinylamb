import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Refuted by counterexample

- `FieldStore.ExactExchange` — Is the proposed conflict test complete as well as sound?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_ExactExchange

theorem refute : ¬ TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.FieldStore.ExactExchange.Goal := by
  exact Common.not_fieldStore_exactExchange

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_ExactExchange
