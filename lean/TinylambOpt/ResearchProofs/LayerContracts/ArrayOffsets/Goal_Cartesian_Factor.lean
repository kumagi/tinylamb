import TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Common_ArrayOffsets

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.

Proved

- `Cartesian.Factor` — `∀ s i, i < s.left.length → ((model.rows s).filter (fun p => p.1
  == some i)).length = s.right.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_Cartesian_Factor

open TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

theorem sound : Cartesian.Factor.Goal := by
  exact Common.factor_Cartesian

end TinylambOpt.ResearchProofs.LayerContracts.ArrayOffsets.Goal_Cartesian_Factor
