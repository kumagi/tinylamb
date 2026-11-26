import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `ZipPad.RightTail` — `∀ s i, s.left.length ≤ i → i < s.right.length → (none,some i) ∈
  model.rows s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipPad_RightTail

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : ZipPad.RightTail.Goal := by
  exact Common.rightTail_ZipPad

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_ZipPad_RightTail
