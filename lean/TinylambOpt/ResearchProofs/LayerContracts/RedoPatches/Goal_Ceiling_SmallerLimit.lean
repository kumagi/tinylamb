import TinylambOpt.ResearchGoals.LayerContracts.RedoPatches
import TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Common_RedoPatches

/-!
Database recovery can combine or replay edits only where the resulting state agrees with
LSN-ordered replay.

Proved

- `Ceiling.SmallerLimit` — `∀ a b x, b.1 ≤ a.1 → model.patch b (model.patch a x) =
  model.patch b x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Ceiling_SmallerLimit

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.RedoPatches.Ceiling.SmallerLimit.Goal := by
  exact Common.ceiling_smallerLimit

end TinylambOpt.ResearchProofs.LayerContracts.RedoPatches.Goal_Ceiling_SmallerLimit
