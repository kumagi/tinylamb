import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `MatchValue.Cardinality` — Avoid materializing UNNEST when only its row count is
  consumed.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_Cardinality

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : MatchValue.Cardinality.Goal := by
  exact Common.cardinality_MatchValue

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_Cardinality
