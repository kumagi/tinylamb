import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_AlignedWord_AfterFree

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.AlignedWord.AfterFree.Goal := by
  apply afterFree_of_firstFit AlignedWord.model
  apply firstFit_of AlignedWord.model AlignedWordFacts.prevA
  · exact AlignedWordFacts.fixA
  · exact AlignedWordFacts.prevSpecA

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_AlignedWord_AfterFree
