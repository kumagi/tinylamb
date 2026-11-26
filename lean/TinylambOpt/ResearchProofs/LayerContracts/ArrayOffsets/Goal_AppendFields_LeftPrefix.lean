import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `AppendFields.LeftPrefix` — `∀ s, (model.rows s).take s.left.length = (ids
  s.left).map lpos`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_AppendFields_LeftPrefix

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : AppendFields.LeftPrefix.Goal := by
  exact Common.leftPrefix_AppendFields

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_AppendFields_LeftPrefix
