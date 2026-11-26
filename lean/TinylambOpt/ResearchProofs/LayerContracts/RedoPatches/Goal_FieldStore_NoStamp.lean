import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `FieldStore.NoStamp` — Blindly repeating a patch after recovery is observationally
  idempotent.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_NoStamp

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.FieldStore.NoStamp.Goal := by
  exact Common.fieldStore_noStamp

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_NoStamp
