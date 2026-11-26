import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `EvenOffsets.LateSlice` — Generate only the requested offset interval for a
  correlated LIMIT.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_EvenOffsets_LateSlice

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : EvenOffsets.LateSlice.Goal := by
  exact Common.lateslice_EvenOffsets

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_EvenOffsets_LateSlice
