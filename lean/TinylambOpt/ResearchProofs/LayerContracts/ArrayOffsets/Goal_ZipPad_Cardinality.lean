import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `ZipPad.Cardinality` — Avoid materializing UNNEST when only its row count is
  consumed.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipPad_Cardinality

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : ZipPad.Cardinality.Goal := by
  exact Common.cardinality_ZipPad

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipPad_Cardinality
