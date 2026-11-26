import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `Sum.PairSum` — `∀ x y, value model [x,y] = some (x+y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_PairSum

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem sound : Sum.PairSum.Goal := by
  intro x y
  simp [value, Sum.model]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_PairSum
