import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `EvenOffsets.NoOdd` — `∀ s i, lpos (2*i+1) ∉ model.rows s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_EvenOffsets_NoOdd

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : EvenOffsets.NoOdd.Goal := by
  exact Common.noOdd_EvenOffsets

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_EvenOffsets_NoOdd
