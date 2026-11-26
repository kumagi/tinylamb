import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `LeftElements.Ordinal` — Random access with a zero-based offset reproduces expansion
  order and exhaustion.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeftElements_Ordinal

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : LeftElements.Ordinal.Goal := by
  exact Common.ordinal_LeftElements

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeftElements_Ordinal
