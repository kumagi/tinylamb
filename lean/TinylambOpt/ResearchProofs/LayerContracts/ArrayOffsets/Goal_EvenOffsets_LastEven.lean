import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `EvenOffsets.LastEven` — `∀ s, 0 < s.left.length → (model.rows s).getLast? = some
  (lpos (2*((s.left.length-1)/2)))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_EvenOffsets_LastEven

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : EvenOffsets.LastEven.Goal := by
  exact Common.lastEven_EvenOffsets

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_EvenOffsets_LastEven
