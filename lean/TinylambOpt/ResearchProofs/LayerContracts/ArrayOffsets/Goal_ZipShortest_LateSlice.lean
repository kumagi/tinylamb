import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `ZipShortest.LateSlice` — Generate only the requested offset interval for a
  correlated LIMIT.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipShortest_LateSlice

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : ZipShortest.LateSlice.Goal := by
  exact Common.lateslice_ZipShortest

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipShortest_LateSlice
