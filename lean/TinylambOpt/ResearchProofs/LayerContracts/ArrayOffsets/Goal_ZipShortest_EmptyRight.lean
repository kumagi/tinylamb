import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `ZipShortest.EmptyRight` — `∀ (a : Arr), model.rows ⟨a,[]⟩ = []`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipShortest_EmptyRight

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : ZipShortest.EmptyRight.Goal := by
  exact Common.emptyRight_ZipShortest

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipShortest_EmptyRight
