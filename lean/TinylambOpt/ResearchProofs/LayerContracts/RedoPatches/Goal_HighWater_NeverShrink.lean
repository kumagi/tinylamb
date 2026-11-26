import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `HighWater.NeverShrink` — `∀ a x, x ≤ model.patch a x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_HighWater_NeverShrink

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.HighWater.NeverShrink.Goal := by
  exact Common.highWater_neverShrink

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_HighWater_NeverShrink
