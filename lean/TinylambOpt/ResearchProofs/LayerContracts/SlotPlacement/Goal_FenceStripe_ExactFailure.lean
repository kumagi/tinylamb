import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_FenceStripe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_ExactFailure

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts.FenceStripeFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.FenceStripe.ExactFailure.Goal := by
  apply exactFailure_of_firstFit FenceStripe.model
  apply firstFit_of FenceStripe.model prev
  · exact fix
  · exact prevSpec

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_FenceStripe_ExactFailure
