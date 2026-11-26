import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `NonNullOnly.PhysicalIndex` — `∀ s i, lpos i ∈ model.rows s ↔
  (s.left[i]?.join).isSome`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_PhysicalIndex

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : NonNullOnly.PhysicalIndex.Goal := by
  exact Common.physicalIndex_NonNullOnly

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_PhysicalIndex
