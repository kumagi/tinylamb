import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Increment.Rebase` — `∀ a x d, model.patch a (x+d) = model.patch a x+d`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_Rebase

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Increment.Rebase.Goal := by
  exact Common.increment_rebase

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Increment_Rebase
