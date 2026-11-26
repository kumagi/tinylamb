import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `MatchValue.AllMatches` — `∀ n k (v : Int), model.count ⟨List.replicate n (some
  v),List.replicate k (some v)⟩ = n*k`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_AllMatches

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : MatchValue.AllMatches.Goal := by
  exact Common.allMatches_MatchValue

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_MatchValue_AllMatches
