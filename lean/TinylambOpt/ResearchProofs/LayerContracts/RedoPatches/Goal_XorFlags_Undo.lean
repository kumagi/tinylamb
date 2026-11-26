import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `XorFlags.Undo` — `∀ a x, model.patch a (model.patch a x) = x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_Undo

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.XorFlags.Undo.Goal := by
  exact Common.xorFlags_undo

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_Undo
