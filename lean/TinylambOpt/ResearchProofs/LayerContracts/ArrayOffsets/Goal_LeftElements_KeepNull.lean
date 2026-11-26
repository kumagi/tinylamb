import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `LeftElements.KeepNull` — `∀ s i, s.left[i]? = some none → lpos i ∈ model.rows s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeftElements_KeepNull

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : LeftElements.KeepNull.Goal := by
  exact Common.keepNull_LeftElements

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeftElements_KeepNull
