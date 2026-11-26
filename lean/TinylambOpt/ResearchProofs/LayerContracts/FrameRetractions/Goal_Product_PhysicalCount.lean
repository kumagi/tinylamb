import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

/-!
A sliding window can update aggregate state as a row leaves when the retained state
suffices to reconstruct the new result.

Refuted by counterexample

- `Product.PhysicalCount` — Can a nullable frame pass physical length instead of
  non-NULL population to the inverse?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_PhysicalCount

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem refute : ¬ Product.PhysicalCount.Goal := by
  exact physicalCount_refute 1 1 (by simp [Product.model]) (by decide)

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_PhysicalCount
