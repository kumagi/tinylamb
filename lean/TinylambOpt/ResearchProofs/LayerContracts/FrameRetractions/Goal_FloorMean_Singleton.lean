import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Proved

- `FloorMean.Singleton` — `∀ x, value model [x] = some x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_Singleton

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions

theorem sound : FloorMean.Singleton.Goal := by
  intro x
  simp [value, FloorMean.model]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_Singleton
