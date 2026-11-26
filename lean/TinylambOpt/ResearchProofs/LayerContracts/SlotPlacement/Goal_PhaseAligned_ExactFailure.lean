import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `PhaseAligned.ExactFailure` — Can a missed probe establish exhaustion without
  scanning the page?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_PhaseAligned_ExactFailure

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.PhaseAligned.ExactFailure.Goal := by
  apply exactFailure_of_firstFit PhaseAligned.model
  apply firstFit_of PhaseAligned.model PhaseAlignedFacts.prevP
  · exact PhaseAlignedFacts.fixP
  · exact PhaseAlignedFacts.prevSpecP

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_PhaseAligned_ExactFailure
