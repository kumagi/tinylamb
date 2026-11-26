import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Overwrite.Constant` — `∀ a x y, model.patch a x = model.patch a y`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Overwrite_Constant

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Overwrite.Constant.Goal := by
  exact Common.overwrite_constant

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Overwrite_Constant
