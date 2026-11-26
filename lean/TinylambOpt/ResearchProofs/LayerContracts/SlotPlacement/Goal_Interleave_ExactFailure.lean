import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Interleave_ExactFailure

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Interleave.ExactFailure.Goal := by
  apply exactFailure_of_firstFit Interleave.model
  apply firstFit_of Interleave.model SlotGroupAndInterleaveFacts.prevI
  · exact SlotGroupAndInterleaveFacts.fixI
  · exact SlotGroupAndInterleaveFacts.prevSpecI

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Interleave_ExactFailure
