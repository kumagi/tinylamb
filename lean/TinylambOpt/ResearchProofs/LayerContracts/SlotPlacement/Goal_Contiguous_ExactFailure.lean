import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `Contiguous.ExactFailure` — Can a missed probe establish exhaustion without scanning
  the page?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_ExactFailure

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Contiguous.ExactFailure.Goal := by
  apply exactFailure_of_firstFit Contiguous.model
  apply firstFit_of Contiguous.model ContiguousFacts.prev
  · exact ContiguousFacts.fix
  · exact ContiguousFacts.prevSpec

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Contiguous_ExactFailure
