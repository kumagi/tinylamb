import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `PhaseAligned.AfterFree` — Removing an occupied byte may reveal an earlier first-fit
  address.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_PhaseAligned_AfterFree

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.PhaseAligned.AfterFree.Goal := by
  apply afterFree_of_firstFit PhaseAligned.model
  apply firstFit_of PhaseAligned.model PhaseAlignedFacts.prevP
  · exact PhaseAlignedFacts.fixP
  · exact PhaseAlignedFacts.prevSpecP

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_PhaseAligned_AfterFree
