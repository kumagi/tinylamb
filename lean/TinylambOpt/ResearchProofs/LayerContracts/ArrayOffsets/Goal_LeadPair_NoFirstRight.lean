import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `LeadPair.NoFirstRight` — `∀ s i, both i 0 ∉ model.rows s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeadPair_NoFirstRight

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : LeadPair.NoFirstRight.Goal := by
  exact Common.noFirstRight_LeadPair

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeadPair_NoFirstRight
