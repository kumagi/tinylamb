import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `MatchValue.NoNullMatch` — `∀ s i j, both i j ∈ model.rows s →
  (s.left[i]?.join).isSome`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_NoNullMatch

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : MatchValue.NoNullMatch.Goal := by
  exact Common.noNullMatch_MatchValue

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_NoNullMatch
