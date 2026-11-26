import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `FieldStore.PreserveUnmasked` — `∀ a x i, a.1.testBit i = false → (model.patch a
  x).testBit i = x.testBit i`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_PreserveUnmasked

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.FieldStore.PreserveUnmasked.Goal := by
  exact Common.fieldStore_preserveUnmasked

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_FieldStore_PreserveUnmasked
