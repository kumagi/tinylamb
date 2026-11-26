import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `ReverseOffsets.LastZero` — `∀ s, 0 < s.left.length → (model.rows s).getLast? = some
  (lpos 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ReverseOffsets_LastZero

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : ReverseOffsets.LastZero.Goal := by
  exact Common.lastZero_ReverseOffsets

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ReverseOffsets_LastZero
