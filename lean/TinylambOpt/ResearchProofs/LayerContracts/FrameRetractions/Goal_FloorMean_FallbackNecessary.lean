import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_FallbackNecessary

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : FloorMean.FallbackNecessary.Goal := by
  apply fallbackNecessary_vacuous
  simp [FloorMean.model]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_FallbackNecessary
