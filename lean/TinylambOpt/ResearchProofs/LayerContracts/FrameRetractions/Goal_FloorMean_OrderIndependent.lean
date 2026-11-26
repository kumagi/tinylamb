import TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
import TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common_FrameRetractions

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_OrderIndependent

open TinylambOpt.ResearchGoals.LayerContracts.FrameRetractions
open TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Common

theorem sound : FloorMean.OrderIndependent.Goal := by
  apply orderIndependent_of
  intro a b hp
  have hs := perm_sum hp
  have hl := hp.length_eq
  simp [FloorMean.model, hs, hl]

end TinylambOpt.ResearchProofs.LayerContracts.FrameRetractions.Goal_FloorMean_OrderIndependent
