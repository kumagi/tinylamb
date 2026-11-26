import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `ZipShortest.LateralOffset` — Skip fully expanded parents using their sizes before
  applying global OFFSET.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipShortest_LateralOffset

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : ZipShortest.LateralOffset.Goal := by
  exact Common.lateraloffset_ZipShortest

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipShortest_LateralOffset
