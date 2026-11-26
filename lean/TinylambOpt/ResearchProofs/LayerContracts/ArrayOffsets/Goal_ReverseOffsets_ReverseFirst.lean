import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `ReverseOffsets.ReverseFirst` — `∀ s, 0 < s.left.length → (model.rows s).head? = some
  (lpos (s.left.length-1))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ReverseOffsets_ReverseFirst

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : ReverseOffsets.ReverseFirst.Goal := by
  exact Common.reverseFirst_ReverseOffsets

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ReverseOffsets_ReverseFirst
