import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `FieldStore.RestartInside` — Can a fused record also replay on a snapshot already
  containing its first half?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_RestartInside

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.FieldStore.RestartInside.Goal := by
  exact Common.fieldStore_restartInside

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_RestartInside
