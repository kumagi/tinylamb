import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `Cartesian.LeftOuterCount` — Proposed row-count rule for a preserved parent under
  correlated OUTER expansion.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_Cartesian_LeftOuterCount

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : Cartesian.LeftOuterCount.Goal := by
  exact Common.leftoutercount_Cartesian

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_Cartesian_LeftOuterCount
