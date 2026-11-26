import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Refuted by counterexample

- `Sum.PhysicalCount` — Can a nullable frame pass physical length instead of non-NULL
  population to the inverse?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_PhysicalCount

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

-- rows = [some 1, none, none] has physical length 3, one non-NULL value 1,
-- but after retracting that row no non-NULL values remain.
theorem refute : ¬ Sum.PhysicalCount.Goal := by
  exact physicalCount_refute 1 0 (by simp [Sum.model]) (by simp [Sum.model])

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_PhysicalCount
