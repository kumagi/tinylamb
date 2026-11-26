import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `NonNullOnly.Cardinality` — Avoid materializing UNNEST when only its row count is
  consumed.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_Cardinality

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : NonNullOnly.Cardinality.Goal := by
  exact Common.cardinality_NonNullOnly

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_Cardinality
