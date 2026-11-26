import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Ceiling.NeverGrow` — `∀ a x, model.patch a x ≤ x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Ceiling_NeverGrow

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Ceiling.NeverGrow.Goal := by
  exact Common.ceiling_neverGrow

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Ceiling_NeverGrow
