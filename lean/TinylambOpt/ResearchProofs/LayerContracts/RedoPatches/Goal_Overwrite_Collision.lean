import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Overwrite.Collision` — `∀ a b x, model.patch a x = model.patch b x ↔ a.1=b.1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Overwrite_Collision

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Overwrite.Collision.Goal := by
  exact Common.overwrite_collision

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Overwrite_Collision
