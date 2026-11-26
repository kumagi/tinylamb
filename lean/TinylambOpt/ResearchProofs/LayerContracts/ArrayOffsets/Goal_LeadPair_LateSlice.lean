import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `LeadPair.LateSlice` — Generate only the requested offset interval for a correlated
  LIMIT.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeadPair_LateSlice

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : LeadPair.LateSlice.Goal := by
  exact Common.lateslice_LeadPair

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_LeadPair_LateSlice
