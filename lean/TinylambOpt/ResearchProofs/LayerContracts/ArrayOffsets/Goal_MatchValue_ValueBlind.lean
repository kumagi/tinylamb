import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Refuted by counterexample

- `MatchValue.ValueBlind` — Cache both group sizes and the offset map across inputs of
  the same shape.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_ValueBlind

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem refute : ¬ MatchValue.ValueBlind.Goal := by
  exact Common.valueblind_MatchValue

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_ValueBlind
