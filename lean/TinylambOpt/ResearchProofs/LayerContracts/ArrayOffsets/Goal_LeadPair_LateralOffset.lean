import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `LeadPair.LateralOffset` — Skip fully expanded parents using their sizes before
  applying global OFFSET.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeadPair_LateralOffset

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : LeadPair.LateralOffset.Goal := by
  exact Common.lateraloffset_LeadPair

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeadPair_LateralOffset
