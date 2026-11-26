import TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts_SlotPlacement

/-!
Page allocation for stored SQL rows must preserve the byte ranges, alignment, and live
data required by the modeled layout.

Proved

- `Interleave.FirstFit` — Return the same lowest address as the dense allocator,
  including failure.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Interleave_FirstFit

open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Common
open TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.ModelFacts

theorem sound : TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement.Interleave.FirstFit.Goal := by
  apply firstFit_of Interleave.model SlotGroupAndInterleaveFacts.prevI
  · exact SlotGroupAndInterleaveFacts.fixI
  · exact SlotGroupAndInterleaveFacts.prevSpecI

end TinylambOpt.ResearchProofs.LayerContracts.SlotPlacement.Goal_Interleave_FirstFit
