import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `NonNullOnly.AddressValidity` — All present coordinates refer to an actual array
  cell, possibly a NULL one.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_AddressValidity

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : NonNullOnly.AddressValidity.Goal := by
  exact Common.addressvalidity_NonNullOnly

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_NonNullOnly_AddressValidity
