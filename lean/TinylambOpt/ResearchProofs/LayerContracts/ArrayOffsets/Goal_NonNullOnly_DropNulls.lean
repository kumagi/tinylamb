import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `NonNullOnly.DropNulls` — `∀ s, model.count s + s.left.count none = s.left.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_DropNulls

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : NonNullOnly.DropNulls.Goal := by
  exact Common.dropNulls_NonNullOnly

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_DropNulls
