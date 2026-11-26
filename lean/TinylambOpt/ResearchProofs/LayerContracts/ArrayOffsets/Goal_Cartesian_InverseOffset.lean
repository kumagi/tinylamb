import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `Cartesian.InverseOffset` — `∀ s i j, i < s.left.length → j < s.right.length →
  selectPos model s (i*s.right.length+j) = some (both i j)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_Cartesian_InverseOffset

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : Cartesian.InverseOffset.Goal := by
  exact Common.inverseOffset_Cartesian

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_Cartesian_InverseOffset
