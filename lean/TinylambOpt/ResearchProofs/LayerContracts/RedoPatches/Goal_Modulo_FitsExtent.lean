import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Modulo.FitsExtent` — `∀ a x, model.patch a x ≤ a.1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_FitsExtent

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Modulo.FitsExtent.Goal := by
  exact Common.modulo_fitsExtent

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_FitsExtent
