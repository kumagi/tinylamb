import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_DuplicateFrame

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

-- A duplicated frame with sum 1 produces 2, not 1.
theorem refute : ¬ Sum.DuplicateFrame.Goal := by
  exact duplicateFrame_refute 1 (by decide)

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_Sum_DuplicateFrame
