import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Affine.Difference` — `∀ a x y, x ≤ y → model.patch a y-model.patch a x = a.1*(y-x)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Affine_Difference

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Affine.Difference.Goal := by
  exact Common.affine_difference

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Affine_Difference
