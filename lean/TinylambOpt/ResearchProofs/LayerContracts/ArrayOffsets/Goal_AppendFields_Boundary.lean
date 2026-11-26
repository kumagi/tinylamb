import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `AppendFields.Boundary` — `∀ s, 0 < s.right.length → selectPos model s s.left.length
  = some (none,some 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_AppendFields_Boundary

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : AppendFields.Boundary.Goal := by
  exact Common.boundary_AppendFields

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_AppendFields_Boundary
