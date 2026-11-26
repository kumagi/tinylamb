import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `LeftElements.NoRight` — `∀ s p, p ∈ model.rows s → p.2=none`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeftElements_NoRight

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : LeftElements.NoRight.Goal := by
  exact Common.noRight_LeftElements

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeftElements_NoRight
