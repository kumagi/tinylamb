import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `CompareSwap.NoStamp` — Blindly repeating a patch after recovery is observationally
  idempotent.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_NoStamp

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.CompareSwap.NoStamp.Goal := by
  exact Common.compareSwap_noStamp

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_CompareSwap_NoStamp
