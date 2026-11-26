import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `XorFlags.CancelDelta` — `∀ a b x, model.patch a x = model.patch b x → a.1=b.1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_CancelDelta

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.XorFlags.CancelDelta.Goal := by
  exact Common.xorFlags_cancelDelta

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_XorFlags_CancelDelta
