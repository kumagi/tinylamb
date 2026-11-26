import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_AlwaysInvertible

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem refute : ¬ FloorMean.AlwaysInvertible.Goal := by
  apply alwaysInvertible_refute [0, 0] [0, 1] 0
  · simp
  · decide
  · simp
  · simp
  · decide

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_AlwaysInvertible
