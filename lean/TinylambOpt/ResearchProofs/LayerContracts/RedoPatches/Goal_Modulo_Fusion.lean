import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Modulo.Fusion` — A successful pair-fusion produces the chronological after-image on
  any base value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_Fusion

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Modulo.Fusion.Goal := by
  exact Common.modulo_fusion

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_Fusion
