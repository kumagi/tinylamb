import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `ZipPad.NoDoublePad` — `∀ s, emptyPos ∉ model.rows s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipPad_NoDoublePad

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : ZipPad.NoDoublePad.Goal := by
  exact Common.noDoublePad_ZipPad

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipPad_NoDoublePad
