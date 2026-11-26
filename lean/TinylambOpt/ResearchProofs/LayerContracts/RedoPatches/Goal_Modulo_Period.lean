import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Modulo.Period` — `∀ a x, model.patch a (x+a.1+1)=model.patch a x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_Period

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Modulo.Period.Goal := by
  exact Common.modulo_period

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Modulo_Period
