import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `LeftElements.Cardinality` — Avoid materializing UNNEST when only its row count is
  consumed.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeftElements_Cardinality

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : LeftElements.Cardinality.Goal := by
  exact Common.cardinality_LeftElements

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeftElements_Cardinality
