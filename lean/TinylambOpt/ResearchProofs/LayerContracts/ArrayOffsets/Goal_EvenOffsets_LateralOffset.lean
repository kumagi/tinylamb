import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `EvenOffsets.LateralOffset` — Skip fully expanded parents using their sizes before
  applying global OFFSET.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_EvenOffsets_LateralOffset

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : EvenOffsets.LateralOffset.Goal := by
  exact Common.lateraloffset_EvenOffsets

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_EvenOffsets_LateralOffset
