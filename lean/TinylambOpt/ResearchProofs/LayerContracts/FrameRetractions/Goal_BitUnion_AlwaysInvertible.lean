import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_AlwaysInvertible

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

-- 1 | 2 = 1 | 3 = 3, but removing 1 leaves 2 and 3.
theorem refute : ¬ BitUnion.AlwaysInvertible.Goal := by
  apply alwaysInvertible_refute [1, 2] [1, 3] 1
  · simp
  · decide
  · simp
  · simp
  · decide

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_BitUnion_AlwaysInvertible
