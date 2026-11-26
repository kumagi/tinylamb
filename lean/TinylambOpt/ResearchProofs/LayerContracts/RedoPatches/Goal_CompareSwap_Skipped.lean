import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `CompareSwap.Skipped` — `∀ a x, x ≠ a.1 → model.patch a x=x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_Skipped

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.CompareSwap.Skipped.Goal := by
  exact Common.compareSwap_skipped

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_Skipped
