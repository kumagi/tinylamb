import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_PhysicalCount

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem refute : ¬ FloorMean.PhysicalCount.Goal := by
  exact physicalCount_refute 1 1 (by simp [FloorMean.model]) (by decide)

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_PhysicalCount
