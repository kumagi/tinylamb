import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_AlwaysInvertible

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

-- [0,0] and [0,1] both have product 0, but dropping 0 leaves products 0 and 1.
theorem refute : ¬ Product.AlwaysInvertible.Goal := by
  apply alwaysInvertible_refute [0, 0] [0, 1] 0
  · simp
  · decide
  · simp
  · simp
  · decide

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Product_AlwaysInvertible
